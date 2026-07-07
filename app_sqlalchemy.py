import streamlit as st
import pandas as pd
import time
import io
import re
import os
import hashlib
from PIL import Image
from sqlalchemy import create_engine, text, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, IntegrityError

# ============================================================
# CONFIGURAR OCR (EasyOCR)
# ============================================================
try:
    import easyocr
    _EASYOCR_READER = easyocr.Reader(['pt', 'en'], gpu=False, verbose=False)
    EASYOCR_DISPONIVEL = True
except Exception:
    EASYOCR_DISPONIVEL = False
    _EASYOCR_READER = None

# ============================================================
# CONFIGURACAO SQLALCHEMY + NEON/COCKROACHDB + SYNC AUTOMATICO
# ============================================================

# Caminho do banco local (usado quando nao ha internet/DATABASE_URL)
LOCAL_DB_PATH = os.path.join(os.path.expanduser("~"), ".auditoria_ativos_local.db")

# Intervalo minimo (segundos) entre tentativas automaticas de reconexao com a nuvem.
# Evita ficar testando a rede a cada interacao do usuario (o Streamlit reexecuta o
# script inteiro a cada clique).
INTERVALO_RECONEXAO_SEGUNDOS = 15


def _criar_engine_local():
    """Cria engine SQLite local (funciona sem internet)."""
    try:
        engine = create_engine(
            f"sqlite:///{LOCAL_DB_PATH}",
            connect_args={"check_same_thread": False}
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine, None
    except Exception as e:
        return None, f"Erro ao criar banco local: {str(e)}"


def _obter_config_remota():
    """Le a connection string da nuvem (Neon/CockroachDB) do st.secrets ou da
    variavel de ambiente DATABASE_URL e monta os parametros de conexao.
    NAO tenta conectar de fato aqui - so monta a configuracao (operacao local,
    sem rede). Retorna None se nao houver nenhuma URL configurada.
    """
    db_url = ""
    try:
        db_config = st.secrets.get("database", {})
        db_url = db_config.get("url", "")
    except Exception:
        pass

    if not db_url:
        db_url = os.environ.get("DATABASE_URL", "")

    if not db_url:
        return None

    url_normalizada = db_url
    eh_cockroach = "cockroachlabs.cloud" in url_normalizada or url_normalizada.startswith("cockroachdb")
    eh_neon = "neon.tech" in url_normalizada
    usa_pooler = "-pooler" in url_normalizada or "pgbouncer=true" in url_normalizada

    if eh_cockroach:
        # CockroachDB Cloud: usa o dialeto especifico (sqlalchemy-cockroachdb)
        if url_normalizada.startswith("postgres://"):
            url_normalizada = url_normalizada.replace("postgres://", "cockroachdb://", 1)
        elif url_normalizada.startswith("postgresql+psycopg2://"):
            url_normalizada = url_normalizada.replace("postgresql+psycopg2://", "cockroachdb+psycopg2://", 1)
        elif url_normalizada.startswith("postgresql://"):
            url_normalizada = url_normalizada.replace("postgresql://", "cockroachdb://", 1)
    else:
        # Postgres padrao (Neon, Supabase, Railway, etc.): mantem postgresql:// normal
        if url_normalizada.startswith("postgres://"):
            url_normalizada = url_normalizada.replace("postgres://", "postgresql://", 1)

    # Tamanho do pool: app com varios usuarios simultaneos precisa de mais
    # conexoes disponiveis. Com o pooler do Neon (PgBouncer) isso e seguro;
    # sem pooler, o free tier do Neon tem poucas conexoes diretas disponiveis.
    if eh_cockroach:
        tam_pool, overflow = 1, 2
    elif eh_neon and usa_pooler:
        tam_pool, overflow = 10, 15
    elif eh_neon:
        tam_pool, overflow = 3, 5
    else:
        tam_pool, overflow = 5, 10

    aviso_pool = None
    if eh_neon and not usa_pooler:
        aviso_pool = (
            "⚠️ Voce esta usando a conexao DIRETA do Neon com varios usuarios simultaneos. "
            "Para evitar erro de 'too many connections', troque pela connection string com "
            "'-pooler' no host (disponivel no painel do Neon, opcao 'Pooled connection')."
        )

    connect_timeout = 10 if eh_neon else 30

    return {
        "url": url_normalizada,
        "pool_size": tam_pool,
        "max_overflow": overflow,
        "connect_timeout": connect_timeout,
        "aviso_pool": aviso_pool,
    }


@st.cache_resource(show_spinner=False)
def _criar_engine_remoto_cacheado(url, pool_size, max_overflow, connect_timeout):
    """Cria (uma unica vez por sessao de servidor) o engine/pool de conexoes
    com a nuvem. create_engine() NAO conecta de verdade - so monta o objeto -
    entao isso e barato de chamar a cada rerun do Streamlit.

    Importante: NAO usamos 'options': '-c statement_timeout=...' no connect_args
    porque o pooler do Neon (PgBouncer) rejeita esse parametro de startup
    ('unsupported startup parameter in options'). Em vez disso, aplicamos o
    statement_timeout via um comando SET logo apos cada conexao fisica ser
    aberta (evento 'connect' do SQLAlchemy), que e compativel com o pooler.
    """
    connect_args = {
        'connect_timeout': connect_timeout,
        'sslmode': 'require',
    }
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args=connect_args,
    )

    @event.listens_for(engine, "connect")
    def _definir_statement_timeout(dbapi_connection, connection_record):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SET statement_timeout = 60000")
            cursor.close()
        except Exception:
            pass

    return engine


def obter_engine_remoto():
    """Retorna (engine_remoto, aviso_pool) sem testar a conexao. engine_remoto
    e None se nenhuma DATABASE_URL/neon estiver configurada."""
    cfg = _obter_config_remota()
    if cfg is None:
        return None, None
    engine = _criar_engine_remoto_cacheado(
        cfg["url"], cfg["pool_size"], cfg["max_overflow"], cfg["connect_timeout"]
    )
    return engine, cfg["aviso_pool"]


def testar_remoto(engine_remoto):
    """Faz um SELECT 1 rapido para saber se a internet/nuvem esta disponivel agora.
    Retorna (ok, mensagem_erro) - a mensagem ajuda a diagnosticar por que a
    conexao com a nuvem esta falhando (credenciais erradas, driver faltando,
    rede bloqueada, etc.)."""
    if engine_remoto is None:
        return False, "Nenhuma DATABASE_URL configurada (verifique st.secrets / variavel de ambiente)."
    try:
        with engine_remoto.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _migrar_colunas_sync_local(engine_local):
    """Garante que a tabela local (SQLite) tenha a coluna 'pendente_sync', usada
    para saber quais registros ainda precisam subir para a nuvem. E seguro
    chamar varias vezes (so faz ALTER TABLE se a coluna ainda nao existir) -
    isso cobre bancos locais criados por uma versao anterior do app."""
    if engine_local is None:
        return
    try:
        with engine_local.begin() as conn:
            colunas = [row[1] for row in conn.execute(text("PRAGMA table_info(inventario)")).fetchall()]
            if colunas and "pendente_sync" not in colunas:
                conn.execute(text("ALTER TABLE inventario ADD COLUMN pendente_sync INTEGER DEFAULT 1"))
    except Exception:
        pass


def _criar_tabela_tombstones_local(engine_local):
    """Cria (se nao existir) a tabela local que registra exclusoes feitas
    offline (apagar uma obra inteira ou apagar tudo), para que essas exclusoes
    sejam replicadas na nuvem quando a internet voltar."""
    if engine_local is None:
        return
    try:
        with engine_local.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_tombstones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo TEXT NOT NULL,
                    obra_id TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """))
    except Exception:
        pass


def _registrar_tombstone(engine_local, tipo, obra_id=None):
    """Registra localmente que algo foi excluido enquanto offline, para
    replicar a exclusao na nuvem na proxima sincronizacao."""
    if engine_local is None:
        return
    try:
        with engine_local.begin() as conn:
            if tipo == "tudo":
                # Exclusao total torna irrelevantes tombstones de obra anteriores
                # (a exclusao total ja cobre tudo).
                conn.execute(text("DELETE FROM sync_tombstones"))
            elif tipo == "obra":
                conn.execute(text("DELETE FROM sync_tombstones WHERE tipo = 'obra' AND obra_id = :obra_id"), {"obra_id": obra_id})
            conn.execute(text("INSERT INTO sync_tombstones (tipo, obra_id) VALUES (:tipo, :obra_id)"),
                         {"tipo": tipo, "obra_id": obra_id})
    except Exception:
        pass


def contar_pendentes_sync(engine_local):
    """Conta quantos itens salvos localmente ainda nao subiram para a nuvem."""
    if engine_local is None:
        return 0
    try:
        with engine_local.connect() as conn:
            row = conn.execute(text("SELECT COUNT(*) FROM inventario WHERE pendente_sync = 1")).fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


def sincronizar_local_para_nuvem(engine_local, engine_remoto):
    """Envia para a nuvem (Neon) tudo que foi feito localmente enquanto o app
    estava offline: primeiro replica as exclusoes (apagar obra / apagar tudo),
    depois envia os itens criados/editados. Estrategia para itens: 'ultima
    escrita vence' - como os itens pendentes so existem porque foram editados
    offline, eles sobrescrevem o que estiver na nuvem para o mesmo
    (obra_id, codigo). Chave unica obra_id+codigo evita duplicacao."""
    if engine_local is None or engine_remoto is None:
        return {"status": "erro", "mensagem": "Banco local ou remoto indisponivel."}
    try:
        exclusoes_aplicadas = 0
        exclusao_total = False

        with engine_local.connect() as conn_local:
            tombstones = conn_local.execute(text(
                "SELECT id, tipo, obra_id FROM sync_tombstones ORDER BY id"
            )).mappings().all()

        for tb in tombstones:
            try:
                with engine_remoto.begin() as conn_remoto:
                    if tb["tipo"] == "tudo":
                        conn_remoto.execute(text("DELETE FROM inventario WHERE TRUE"))
                        exclusao_total = True
                    elif tb["tipo"] == "obra" and tb["obra_id"]:
                        conn_remoto.execute(text("DELETE FROM inventario WHERE obra_id = :obra_id"), {"obra_id": tb["obra_id"]})
                exclusoes_aplicadas += 1
            except Exception:
                continue

        if tombstones:
            with engine_local.begin() as conn_local:
                conn_local.execute(text("DELETE FROM sync_tombstones"))

        with engine_local.connect() as conn_local:
            pendentes = conn_local.execute(text("""
                SELECT obra_id, codigo, descricao, status, data_auditoria, observacoes, quantidade, local
                FROM inventario
                WHERE pendente_sync = 1
            """)).mappings().all()

        if not pendentes:
            if exclusoes_aplicadas:
                return {"status": "ok", "enviados": 0, "erros": 0, "exclusoes": exclusoes_aplicadas}
            return {"status": "nada_a_sincronizar", "enviados": 0, "erros": 0}

        enviados = 0
        erros = 0
        sincronizados = []

        for row in pendentes:
            try:
                with engine_remoto.begin() as conn_remoto:
                    conn_remoto.execute(text("""
                        INSERT INTO inventario
                        (obra_id, codigo, descricao, status, data_auditoria, observacoes, quantidade, local)
                        VALUES (:obra_id, :codigo, :descricao, :status, :data_aud, :obs, :qtd, :local)
                        ON CONFLICT (obra_id, codigo) DO UPDATE SET
                            descricao = EXCLUDED.descricao,
                            status = EXCLUDED.status,
                            data_auditoria = EXCLUDED.data_auditoria,
                            observacoes = EXCLUDED.observacoes,
                            quantidade = EXCLUDED.quantidade,
                            local = EXCLUDED.local,
                            updated_at = now()
                    """), {
                        "obra_id": row["obra_id"], "codigo": row["codigo"], "descricao": row["descricao"],
                        "status": row["status"], "data_aud": row["data_auditoria"], "obs": row["observacoes"],
                        "qtd": row["quantidade"], "local": row["local"]
                    })
                enviados += 1
                sincronizados.append((row["obra_id"], row["codigo"]))
            except Exception:
                erros += 1
                continue

        if sincronizados:
            with engine_local.begin() as conn_local:
                for obra_id, codigo in sincronizados:
                    conn_local.execute(text("""
                        UPDATE inventario SET pendente_sync = 0 WHERE obra_id = :obra_id AND codigo = :codigo
                    """), {"obra_id": obra_id, "codigo": codigo})

        return {"status": "ok", "enviados": enviados, "erros": erros, "exclusoes": exclusoes_aplicadas}
    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}


def _inicializar_conexao():
    """Decide, a cada execucao do script, se o app deve usar a nuvem (Neon) ou
    o banco local (SQLite). Quando detecta que a internet/nuvem voltou depois de
    um periodo offline, sincroniza automaticamente os dados locais pendentes
    para a nuvem antes de passar a usa-la. Retorna (engine, erro, modo_offline)."""
    if "modo_offline" not in st.session_state:
        st.session_state.modo_offline = None  # ainda nao determinado nesta sessao
    if "ultima_verificacao_conexao" not in st.session_state:
        st.session_state.ultima_verificacao_conexao = 0.0
    if "ultimo_sync_info" not in st.session_state:
        st.session_state.ultimo_sync_info = None
    if "forcar_verificacao_conexao" not in st.session_state:
        st.session_state.forcar_verificacao_conexao = False
    if "ultimo_erro_conexao" not in st.session_state:
        st.session_state.ultimo_erro_conexao = None

    engine_local, erro_local = _criar_engine_local()
    if engine_local is not None:
        criar_tabela_inventario(engine_local)
        _migrar_colunas_sync_local(engine_local)
        _criar_tabela_tombstones_local(engine_local)

    engine_remoto, aviso_pool = obter_engine_remoto()
    st.session_state.aviso_pool = aviso_pool

    if engine_remoto is None:
        # Nenhuma DATABASE_URL configurada -> so existe modo local
        st.session_state.modo_offline = True
        st.session_state.ultimo_erro_conexao = (
            "Nenhuma DATABASE_URL encontrada em st.secrets['database']['url'] nem na "
            "variavel de ambiente DATABASE_URL. Configure os Secrets do deploy."
        )
        return engine_local, erro_local, True

    agora = time.time()
    deve_verificar = (
        st.session_state.modo_offline is None
        or (agora - st.session_state.ultima_verificacao_conexao) >= INTERVALO_RECONEXAO_SEGUNDOS
        or st.session_state.forcar_verificacao_conexao
    )

    if deve_verificar:
        st.session_state.ultima_verificacao_conexao = agora
        st.session_state.forcar_verificacao_conexao = False
        estava_offline = st.session_state.modo_offline in (None, True)
        remoto_ok, erro_remoto = testar_remoto(engine_remoto)
        st.session_state.ultimo_erro_conexao = erro_remoto

        if remoto_ok:
            if estava_offline and engine_local is not None:
                # A internet/nuvem acabou de voltar -> sobe os dados locais pendentes
                criar_tabela_inventario(engine_remoto)
                resultado = sincronizar_local_para_nuvem(engine_local, engine_remoto)
                st.session_state.ultimo_sync_info = resultado
            st.session_state.modo_offline = False
        else:
            st.session_state.modo_offline = True

    if st.session_state.modo_offline:
        if engine_local is None:
            return None, erro_local or "Nenhuma DATABASE_URL encontrada e falha ao criar banco local.", True
        return engine_local, None, True

    return engine_remoto, None, False


def test_connection(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            return result.fetchone()[0] == 1
    except Exception:
        return False

def criar_tabela_inventario(engine):
    is_sqlite = engine.dialect.name == "sqlite"
    is_cockroach = engine.dialect.name == "cockroachdb"
    is_postgres_generico = (not is_sqlite) and (not is_cockroach)
    try:
        with engine.begin() as conn:
            if is_sqlite:
                result = conn.execute(text("""
                    SELECT name FROM sqlite_master WHERE type='table' AND name='inventario'
                """))
            else:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'inventario'
                """))
            if result.fetchone():
                return True

            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE inventario (
                        id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                        obra_id TEXT NOT NULL,
                        codigo TEXT NOT NULL,
                        descricao TEXT NOT NULL,
                        status TEXT DEFAULT 'Pendente',
                        data_auditoria TEXT DEFAULT '',
                        observacoes TEXT DEFAULT '',
                        quantidade TEXT DEFAULT '1',
                        local TEXT DEFAULT '',
                        created_at TEXT DEFAULT (datetime('now')),
                        updated_at TEXT DEFAULT (datetime('now')),
                        pendente_sync INTEGER DEFAULT 1,
                        UNIQUE (obra_id, codigo)
                    )
                """))
            elif is_postgres_generico:
                # Postgres padrao (Neon, Supabase, Railway, RDS, etc.)
                # gen_random_uuid() precisa da extensao pgcrypto nesses provedores
                try:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
                except Exception:
                    pass  # alguns provedores ja vem com a extensao habilitada por padrao
                conn.execute(text("""
                    CREATE TABLE inventario (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        obra_id TEXT NOT NULL,
                        codigo TEXT NOT NULL,
                        descricao TEXT NOT NULL,
                        status TEXT DEFAULT 'Pendente',
                        data_auditoria TEXT DEFAULT '',
                        observacoes TEXT DEFAULT '',
                        quantidade TEXT DEFAULT '1',
                        local TEXT DEFAULT '',
                        created_at TIMESTAMP DEFAULT now(),
                        updated_at TIMESTAMP DEFAULT now(),
                        CONSTRAINT unique_codigo_obra UNIQUE (obra_id, codigo)
                    )
                """))
            else:
                conn.execute(text("""
                    CREATE TABLE inventario (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        obra_id STRING NOT NULL,
                        codigo STRING NOT NULL,
                        descricao STRING NOT NULL,
                        status STRING DEFAULT 'Pendente',
                        data_auditoria STRING DEFAULT '',
                        observacoes STRING DEFAULT '',
                        quantidade STRING DEFAULT '1',
                        local STRING DEFAULT '',
                        created_at TIMESTAMP DEFAULT now(),
                        updated_at TIMESTAMP DEFAULT now(),
                        CONSTRAINT unique_codigo_obra UNIQUE (obra_id, codigo)
                    )
                """))
            conn.execute(text("CREATE INDEX idx_inventario_obra_id ON inventario (obra_id)"))
            conn.execute(text("CREATE INDEX idx_inventario_codigo ON inventario (obra_id, codigo)"))
            return True
    except Exception as e:
        st.error(f"Erro ao criar tabela: {e}")
        return False

engine, erro_engine, MODO_OFFLINE = _inicializar_conexao()
AVISO_POOL = st.session_state.get("aviso_pool")

# ============================================================
# FUNCOES DE RETRY
# ============================================================
def execute_with_retry(query_func, max_retries=5, base_delay=0.5):
    for attempt in range(max_retries):
        try:
            with engine.begin() as conn:
                result = query_func(conn)
                return result
        except (OperationalError, IntegrityError) as e:
            error_msg = str(e).lower()
            retry_codes = ['40001', '40003', '08006', '08001', 'retry', 'serialization', 'restart transaction']
            should_retry = any(code in error_msg for code in retry_codes)
            if should_retry and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
                continue
            else:
                raise
        except Exception:
            raise
    return None

# ============================================================
# FUNCOES DE BANCO DE DADOS
# ============================================================
def carregar_do_banco(obra_id="default"):
    try:
        def _carregar(conn):
            result = conn.execute(text("""
                SELECT codigo, descricao, status, data_auditoria, 
                       observacoes, quantidade, local
                FROM inventario
                WHERE obra_id = :obra_id
                ORDER BY codigo
            """), {"obra_id": obra_id})
            return result.mappings().all()

        dados = execute_with_retry(_carregar)
        if not dados:
            return pd.DataFrame(columns=["Codigo do Bem", "Descricao do Bem", "Status", 
                                         "Data Auditoria", "Observacoes", "Quantidade", "Local"])

        df = pd.DataFrame([dict(row) for row in dados])
        mapeamento = {
            "codigo": "Codigo do Bem",
            "descricao": "Descricao do Bem",
            "status": "Status",
            "data_auditoria": "Data Auditoria",
            "observacoes": "Observacoes",
            "quantidade": "Quantidade",
            "local": "Local"
        }
        df = df.rename(columns={k: v for k, v in mapeamento.items() if k in df.columns})
        for col in ["Codigo do Bem", "Descricao do Bem", "Status", 
                    "Data Auditoria", "Observacoes", "Quantidade", "Local"]:
            if col not in df.columns:
                df[col] = ""
        return df[["Codigo do Bem", "Descricao do Bem", "Status", 
                   "Data Auditoria", "Observacoes", "Quantidade", "Local"]]
    except Exception as e:
        st.error(f"Erro ao carregar: {e}")
        return pd.DataFrame(columns=["Codigo do Bem", "Descricao do Bem", "Status", 
                                     "Data Auditoria", "Observacoes", "Quantidade", "Local"])

def verificar_item_existe(codigo, obra_id="default"):
    """Verifica se um código já existe no banco para a obra."""
    try:
        def _verificar(conn):
            result = conn.execute(text("""
                SELECT COUNT(*) as total FROM inventario 
                WHERE obra_id = :obra_id AND codigo = :codigo
            """), {"obra_id": obra_id, "codigo": str(codigo)})
            return result.fetchone()[0] > 0
        return execute_with_retry(_verificar)
    except Exception:
        return False

def verificar_descricao_existe(descricao, obra_id="default"):
    """Verifica se uma descrição SEM PATRIMONIO já existe na obra."""
    try:
        def _verificar(conn):
            result = conn.execute(text("""
                SELECT COUNT(*) as total FROM inventario 
                WHERE obra_id = :obra_id AND descricao = :descricao
            """), {"obra_id": obra_id, "descricao": str(descricao)})
            return result.fetchone()[0] > 0
        return execute_with_retry(_verificar)
    except Exception:
        return False

def salvar_lote_banco(df, obra_id="default"):
    try:
        total_excel = len(df)
        def _contar(conn):
            result = conn.execute(text("""
                SELECT COUNT(*) as total FROM inventario WHERE obra_id = :obra_id
            """), {"obra_id": obra_id})
            return result.fetchone()[0]

        total_banco = execute_with_retry(_contar)
        if total_banco > 0 and total_banco == total_excel:
            return {"status": "carregar", "mensagem": f"{total_excel} itens ja carregados."}

        if total_banco == 0:
            def _inserir_tudo(conn):
                for _, row in df.iterrows():
                    conn.execute(text(f"""
                        INSERT INTO inventario 
                        (obra_id, codigo, descricao, status, data_auditoria, observacoes, quantidade, local)
                        VALUES (:obra_id, :codigo, :descricao, :status, :data_aud, :obs, :qtd, :local)
                        ON CONFLICT (obra_id, codigo) DO NOTHING
                    """), {
                        "obra_id": obra_id,
                        "codigo": str(row["Codigo do Bem"]),
                        "descricao": str(row["Descricao do Bem"]),
                        "status": str(row.get("Status", "Pendente")),
                        "data_aud": str(row.get("Data Auditoria", "")),
                        "obs": str(row.get("Observacoes", "")),
                        "qtd": str(row.get("Quantidade", "1")),
                        "local": str(row.get("Local", ""))
                    })
                return True
            execute_with_retry(_inserir_tudo)
            return {"status": "sucesso", "novos": total_excel, "alterados": 0, "iguais": 0}

        def _buscar_existentes(conn):
            result = conn.execute(text("""
                SELECT codigo, descricao, status 
                FROM inventario 
                WHERE obra_id = :obra_id
            """), {"obra_id": obra_id})
            return result.mappings().all()

        existentes = execute_with_retry(_buscar_existentes)
        existentes_dict = {str(item["codigo"]): dict(item) for item in existentes}
        novos = 0
        alterados = 0
        iguais = 0

        def _processar(conn):
            nonlocal novos, alterados, iguais
            is_sqlite = engine.dialect.name == "sqlite"
            campo_sync = ", pendente_sync = 1" if is_sqlite else ""
            for _, row in df.iterrows():
                codigo = str(row["Codigo do Bem"])
                descricao = str(row["Descricao do Bem"])
                if codigo in existentes_dict:
                    if existentes_dict[codigo]["descricao"] != descricao:
                        status_manter = existentes_dict[codigo]["status"] if existentes_dict[codigo]["status"] == "Auditado" else str(row.get("Status", "Pendente"))
                        conn.execute(text(f"""
                            UPDATE inventario 
                            SET descricao = :descricao, status = :status, updated_at = :updated_at{campo_sync}
                            WHERE obra_id = :obra_id AND codigo = :codigo
                        """), {"descricao": descricao, "status": status_manter, "obra_id": obra_id, "codigo": codigo,
                               "updated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")})
                        alterados += 1
                    else:
                        iguais += 1
                else:
                    conn.execute(text("""
                        INSERT INTO inventario 
                        (obra_id, codigo, descricao, status, data_auditoria, observacoes, quantidade, local)
                        VALUES (:obra_id, :codigo, :descricao, :status, :data_aud, :obs, :qtd, :local)
                    """), {
                        "obra_id": obra_id, "codigo": codigo, "descricao": descricao,
                        "status": str(row.get("Status", "Pendente")),
                        "data_aud": str(row.get("Data Auditoria", "")),
                        "obs": str(row.get("Observacoes", "")),
                        "qtd": str(row.get("Quantidade", "1")),
                        "local": str(row.get("Local", ""))
                    })
                    novos += 1
            return True

        execute_with_retry(_processar)
        if novos == 0 and alterados == 0:
            return {"status": "carregar", "mensagem": f"{total_excel} itens ja atualizados."}
        return {"status": "sucesso", "novos": novos, "alterados": alterados, "iguais": iguais}
    except Exception as e:
        st.error(f"Erro ao salvar lote: {e}")
        return {"status": "erro", "mensagem": str(e)}

def salvar_item_banco(codigo, descricao, status="Pendente", data_aud="", obs="", qtd="1", local="", obra_id="default"):
    try:
        def _salvar(conn):
            is_sqlite = engine.dialect.name == "sqlite"
            campo_sync = ", pendente_sync = 1" if is_sqlite else ""
            result = conn.execute(text("""
                SELECT id FROM inventario 
                WHERE obra_id = :obra_id AND codigo = :codigo
            """), {"obra_id": obra_id, "codigo": str(codigo)})
            existe = result.fetchone()
            if existe:
                conn.execute(text(f"""
                    UPDATE inventario 
                    SET status = :status, data_auditoria = :data_aud, observacoes = :obs, 
                        quantidade = :qtd, local = :local, updated_at = :updated_at{campo_sync}
                    WHERE obra_id = :obra_id AND codigo = :codigo
                """), {"status": status, "data_aud": data_aud, "obs": obs, "qtd": qtd, 
                       "local": local, "obra_id": obra_id, "codigo": str(codigo),
                       "updated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")})
            else:
                conn.execute(text("""
                    INSERT INTO inventario 
                    (obra_id, codigo, descricao, status, data_auditoria, observacoes, quantidade, local)
                    VALUES (:obra_id, :codigo, :descricao, :status, :data_aud, :obs, :qtd, :local)
                """), {"obra_id": obra_id, "codigo": str(codigo), "descricao": str(descricao),
                       "status": status, "data_aud": data_aud, "obs": obs, "qtd": qtd, "local": local})
            return True
        execute_with_retry(_salvar)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar item: {e}")
        return False

def deletar_obra_banco(obra_id):
    try:
        is_sqlite = engine.dialect.name == "sqlite"
        def _deletar(conn):
            conn.execute(text("DELETE FROM inventario WHERE obra_id = :obra_id"), {"obra_id": obra_id})
            return True
        execute_with_retry(_deletar)
        if is_sqlite:
            _registrar_tombstone(engine, "obra", obra_id)
        return True
    except Exception as e:
        st.error(f"Erro ao deletar obra: {e}")
        return False

def deletar_tudo_banco():
    """Deleta TODOS os registros do banco (compativel com CockroachDB)."""
    try:
        is_sqlite = engine.dialect.name == "sqlite"
        def _deletar(conn):
            conn.execute(text("DELETE FROM inventario WHERE TRUE"))
            return True
        execute_with_retry(_deletar)
        if is_sqlite:
            _registrar_tombstone(engine, "tudo")
        return True
    except Exception as e:
        st.error(f"Erro ao deletar tudo: {e}")
        return False

def limpar_tudo_app_e_banco():
    """Limpa TUDO: banco de dados, session_state, memoria do app."""
    sucesso_banco = deletar_tudo_banco()
    chaves_para_remover = []
    for chave in list(st.session_state.keys()):
        if chave not in ['_is_running_with_streamlit', 'previous_query_params']:
            chaves_para_remover.append(chave)
    for chave in chaves_para_remover:
        if chave in st.session_state:
            del st.session_state[chave]
    st.session_state.obra_id = "obra_001"
    st.session_state.db = pd.DataFrame(columns=["Codigo do Bem", "Descricao do Bem", "Status", 
                                                "Data Auditoria", "Observacoes", "Quantidade", "Local"])
    st.session_state.codigo_ocr = ""
    st.session_state.arquivo_processado = False
    return sucesso_banco

def listar_obras_banco():
    try:
        def _listar(conn):
            result = conn.execute(text("SELECT DISTINCT obra_id FROM inventario ORDER BY obra_id"))
            return [row[0] for row in result.fetchall()]
        return execute_with_retry(_listar)
    except Exception as e:
        st.error(f"Erro ao listar obras: {e}")
        return []

# ============================================================
# FUNCOES DE OCR
# ============================================================
def extrair_codigo_ocr(texto):
    texto_limpo = texto.upper().replace(" ", "").replace("-", "")
    padroes = [r"CPBE[0-9]{3}([0-9]{3,6})", r"([0-9]{6,8})", r"([0-9]{3,6})"]
    for padrao in padroes:
        match = re.search(padrao, texto_limpo)
        if match:
            return match.group(1).lstrip("0")
    numeros = re.findall(r"[0-9]+", texto_limpo)
    if numeros:
        return max(numeros, key=len).lstrip("0")
    return None

def executar_ocr(imagem):
    if not EASYOCR_DISPONIVEL or _EASYOCR_READER is None:
        return ""
    try:
        import numpy as np
        img = imagem.convert('RGB')
        img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)
        img_array = np.array(img)
        resultados = _EASYOCR_READER.readtext(img_array, detail=1, allowlist='0123456789', paragraph=False)
        numeros = []
        for (bbox, texto, conf) in resultados:
            if conf < 0.3:
                continue
            nums = "".join([c for c in texto if c.isdigit()])
            if nums:
                numeros.append((nums, conf))
        numeros.sort(key=lambda x: x[1], reverse=True)
        melhor = ""
        for nums, conf in numeros:
            if len(nums) > len(melhor):
                melhor = nums
        return melhor
    except Exception as e:
        st.error(f"Erro no OCR: {e}")
        return ""

def normalizar(codigo):
    if pd.isna(codigo):
        return ""
    return str(codigo).split("-")[0].lstrip("0")

def limpar_excel(df_raw):
    if "Codigo do Bem" not in df_raw.columns:
        df_raw.columns = df_raw.iloc[0]
        df_raw = df_raw.iloc[1:].reset_index(drop=True)
    mask = (
        df_raw["Codigo do Bem"].astype(str).str.match(r"^[0-9]{3,}", na=False) &
        df_raw["Descricao do Bem"].notna() &
        (df_raw["Descricao do Bem"].astype(str).str.strip() != "") &
        (~df_raw["Descricao do Bem"].astype(str).str.contains("Estel Servicos", na=False))
    )
    df_clean = df_raw.loc[mask, ["Codigo do Bem", "Descricao do Bem"]].copy()
    df_clean["Codigo do Bem"] = df_clean["Codigo do Bem"].astype(str).str.strip()
    return df_clean

def exportar_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Inventario")
    output.seek(0)
    return output

def calcular_hash_arquivo(arquivo_bytes):
    """Calcula hash MD5 dos bytes do arquivo para detectar duplicados."""
    return hashlib.md5(arquivo_bytes).hexdigest()

# ============================================================
# CONFIGURACAO VISUAL
# ============================================================

# 1. DESENHO DO ICONE (prancheta com checklist + selo de "check")
icone_svg = """
<svg viewBox="0 0 512 512" xmlns="http://www.w3.org/2000/svg">
  <rect width="512" height="512" rx="100" fill="#1E293B"/>
  <circle cx="256" cy="256" r="170" fill="#3B82F6" opacity="0.24"/>
  <rect x="140" y="110" width="232" height="310" rx="24" fill="#F1F5F9"/>
  <rect x="206" y="87" width="100" height="46" rx="12" fill="#3B82F6"/>
  <rect x="155" y="180" width="20" height="20" rx="4" fill="none" stroke="#94A3B8" stroke-width="4"/>
  <line x1="180" y1="190" x2="332" y2="190" stroke="#64748B" stroke-width="14" stroke-linecap="round"/>
  <rect x="155" y="240" width="20" height="20" rx="4" fill="none" stroke="#94A3B8" stroke-width="4"/>
  <line x1="180" y1="250" x2="300" y2="250" stroke="#64748B" stroke-width="14" stroke-linecap="round"/>
  <rect x="155" y="300" width="20" height="20" rx="4" fill="none" stroke="#94A3B8" stroke-width="4"/>
  <line x1="180" y1="310" x2="332" y2="310" stroke="#64748B" stroke-width="14" stroke-linecap="round"/>
  <circle cx="372" cy="380" r="70" fill="#10B981"/>
  <polyline points="342,380 364,406 402,350" fill="none" stroke="white" stroke-width="14"
            stroke-linecap="round" stroke-linejoin="round"/>
</svg>
"""

# 2. CONVERSAO PARA DATA URL (favicon da aba - navegadores modernos aceitam SVG)
import base64
img_base64 = base64.b64encode(icone_svg.encode()).decode()
data_url = f"data:image/svg+xml;base64,{img_base64}"

# 2.1 ARQUIVOS NECESSARIOS PARA O CHROME/ANDROID OFERECER "INSTALAR APP"
# O Chrome nao aceita bem SVG dentro do manifest.json - precisa de PNG real.
# Tambem exige um service worker ativo para disparar o prompt de instalacao.
import json
import pathlib
from PIL import ImageDraw

_static_dir = pathlib.Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)


def _gerar_icone_png(size: int) -> Image.Image:
    """Redesenha o icone_svg como PNG real (prancheta + checklist + selo)."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    s = size / 512.0
    d = ImageDraw.Draw(img)

    d.rounded_rectangle([0, 0, size - 1, size - 1], radius=int(100 * s), fill=(30, 41, 59, 255))

    cx, cy, r = 256 * s, 256 * s, 170 * s
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    ImageDraw.Draw(glow).ellipse([cx - r, cy - r, cx + r, cy + r], fill=(59, 130, 246, 60))
    img = Image.alpha_composite(img, glow)
    d = ImageDraw.Draw(img)

    d.rounded_rectangle([140 * s, 110 * s, 372 * s, 420 * s], radius=int(24 * s), fill=(241, 245, 249, 255))

    clip_w, clip_h = 100 * s, 46 * s
    d.rounded_rectangle(
        [256 * s - clip_w / 2, 110 * s - clip_h / 2, 256 * s + clip_w / 2, 110 * s + clip_h / 2],
        radius=int(12 * s), fill=(59, 130, 246, 255),
    )

    for y1, x1, x2 in ((190, 180, 332), (250, 180, 300), (310, 180, 332)):
        d.line([(x1 * s, y1 * s), (x2 * s, y1 * s)], fill=(100, 116, 139, 255), width=max(2, int(14 * s)))
        box = [155 * s, y1 * s - 10 * s, 155 * s + 20 * s, y1 * s + 10 * s]
        d.rounded_rectangle(box, radius=int(4 * s), outline=(148, 163, 184, 255), width=max(1, int(4 * s)))

    badge_r = 70 * s
    bx, by = 372 * s, 380 * s
    d.ellipse([bx - badge_r, by - badge_r, bx + badge_r, by + badge_r], fill=(16, 185, 129, 255))
    check_pts = [(bx - 30 * s, by), (bx - 8 * s, by + 26 * s), (bx + 35 * s, by - 30 * s)]
    d.line(check_pts, fill=(255, 255, 255, 255), width=max(2, int(14 * s)), joint="curve")

    return img


for _size in (192, 512):
    _gerar_icone_png(_size).save(_static_dir / f"icon-{_size}.png")

_manifest = {
    "name": "Auditoria de Ativos",
    "short_name": "Auditoria",
    "start_url": ".",
    "scope": ".",
    "display": "standalone",
    "background_color": "#1E293B",
    "theme_color": "#1E293B",
    "icons": [
        {"src": "icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
        {"src": "icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "maskable"},
        {"src": "icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
        {"src": "icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
    ],
}
(_static_dir / "manifest.json").write_text(json.dumps(_manifest), encoding="utf-8")
(_static_dir / "sw.js").write_text(
    "self.addEventListener('install', () => self.skipWaiting());\n"
    "self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));\n"
    "self.addEventListener('fetch', () => {});\n",
    encoding="utf-8",
)

# 3. CONFIGURACAO DA PAGINA COM O ICONE NOVO
st.set_page_config(page_title="Auditoria de Ativos", page_icon=data_url, layout="wide")

# 4. INJETA FAVICON + APPLE-TOUCH-ICON + MANIFEST NO <head> REAL DA PAGINA
# (um <head> dentro de st.markdown nao funciona - o navegador descarta
# tags de <head> quando aparecem dentro do <body>)
import streamlit.components.v1 as components

components.html(f"""
<script>
    const head = window.parent.document.head;

    function setLink(rel, href, sizes) {{
        let link = head.querySelector(`link[rel="${{rel}}"]${{sizes ? `[sizes="${{sizes}}"]` : ''}}`);
        if (!link) {{
            link = window.parent.document.createElement('link');
            link.rel = rel;
            if (sizes) link.sizes = sizes;
            head.appendChild(link);
        }}
        link.href = href;
    }}

    function setMeta(name, content) {{
        let meta = head.querySelector(`meta[name="${{name}}"]`);
        if (!meta) {{
            meta = window.parent.document.createElement('meta');
            meta.name = name;
            head.appendChild(meta);
        }}
        meta.content = content;
    }}

    setLink('icon', '{data_url}');
    setLink('shortcut icon', '{data_url}');
    setLink('apple-touch-icon', '{data_url}');
    setLink('manifest', 'app/static/manifest.json');
    setMeta('apple-mobile-web-app-capable', 'yes');
    setMeta('mobile-web-app-capable', 'yes');
    setMeta('apple-mobile-web-app-status-bar-style', 'black-translucent');

    if ('serviceWorker' in window.parent.navigator) {{
        window.parent.navigator.serviceWorker.register('app/static/sw.js').catch(() => {{}});
    }}
</script>
""", height=0, width=0)

# 5. BOTAO DE INSTALACAO DO APP (PWA)
# Android/Chrome: 1 clique ja mostra a confirmacao nativa de instalacao.
# iOS/Safari: a Apple nao expoe API para instalar programaticamente -
# mostramos instrucoes claras nesse caso (nao ha como automatizar).
components.html(f"""
<div id="pwa-btn-wrap" style="display:none; margin: 4px 0 12px 0;">
    <button id="pwa-install-btn" style="
        background:#3B82F6; color:white; border:none; border-radius:8px;
        padding:10px 18px; font-family:'Inter', sans-serif; font-weight:600;
        font-size:14px; cursor:pointer; display:inline-flex; align-items:center; gap:8px;">
        📲 Instalar App
    </button>
    <div id="pwa-ios-instrucoes" style="
        display:none; margin-top:10px; background:#EFF6FF; border:1px solid #BFDBFE;
        border-radius:8px; padding:12px; font-family:'Inter', sans-serif; font-size:13px; color:#1E3A8A;">
        Para instalar no iPhone: toque no ícone de <b>Compartilhar</b>
        <span style="font-size:16px;">⬆️</span> na barra do Safari e depois em
        <b>"Adicionar à Tela de Início"</b>.
    </div>
</div>
<script>
(function() {{
    const topWindow = window.parent;
    const btnWrap = document.getElementById('pwa-btn-wrap');
    const btn = document.getElementById('pwa-install-btn');
    const iosBox = document.getElementById('pwa-ios-instrucoes');
    let deferredPrompt = null;

    const isIOS = /iphone|ipad|ipod/i.test(topWindow.navigator.userAgent);

    topWindow.addEventListener('beforeinstallprompt', (e) => {{
        e.preventDefault();
        deferredPrompt = e;
        btnWrap.style.display = 'block';
    }});

    if (isIOS) {{
        btnWrap.style.display = 'block';
        btn.textContent = '📲 Instalar App (iPhone)';
    }}

    btn.addEventListener('click', async () => {{
        if (deferredPrompt) {{
            deferredPrompt.prompt();
            await deferredPrompt.userChoice;
            deferredPrompt = null;
            btnWrap.style.display = 'none';
        }} else if (isIOS) {{
            iosBox.style.display = iosBox.style.display === 'none' ? 'block' : 'none';
        }}
    }});

    topWindow.addEventListener('appinstalled', () => {{
        btnWrap.style.display = 'none';
    }});
}})();
</script>
""", height=110)

st.markdown("""
    <style>
    @import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap");
    html, body, [class*="css"] { font-family: "Inter", sans-serif; background-color: #F8FAFC; }
    .stButton>button { width: 100%; border-radius: 8px; height: 48px; background-color: #1E293B; color: white; font-weight: 600; border: none; }
    .stButton>button:hover { background-color: #334155; }
    .success-box { background: #ECFDF5; border-left: 4px solid #10B981; padding: 16px; border-radius: 8px; margin: 12px 0; }
    .error-box { background: #FEF2F2; border-left: 4px solid #EF4444; padding: 16px; border-radius: 8px; margin: 12px 0; }
    .warning-box { background: #FFFBEB; border-left: 4px solid #F59E0B; padding: 16px; border-radius: 8px; margin: 12px 0; }
    .ocr-box { background: #EFF6FF; border: 2px solid #3B82F6; border-radius: 12px; padding: 16px; margin: 12px 0; }
    .sem-pat-box { background: #F3E8FF; border: 2px solid #9333EA; border-radius: 12px; padding: 16px; margin: 12px 0; }
    .info-box { background: #DBEAFE; border-left: 4px solid #3B82F6; padding: 16px; border-radius: 8px; margin: 12px 0; }

    /* Oculta a barra de ferramentas do Streamlit Cloud:
       Share, estrela (favoritar), lápis (editar), GitHub, Deploy e o menu (⋮) */
    [data-testid="stToolbar"] {visibility: hidden !important; height: 0 !important; position: fixed;}
    [data-testid="stDecoration"] {display: none !important;}
    [data-testid="stStatusWidget"] {display: none !important;}
    #MainMenu {visibility: hidden !important;}
    .stDeployButton {display: none !important;}
    .stAppDeployButton {display: none !important;}
    footer {visibility: hidden !important;}
    </style>
    """, unsafe_allow_html=True)

# ============================================================
# VERIFICACAO INICIAL DO BANCO
# ============================================================
if engine is None:
    st.error(f"❌ Nao foi possivel iniciar nenhum banco de dados (nem CockroachDB, nem local): {erro_engine}")
    st.stop()

with st.spinner("🔌 Conectando..."):
    if not test_connection(engine):
        st.error("❌ Nao foi possivel conectar ao banco de dados!")
        st.stop()
    if not criar_tabela_inventario(engine):
        st.error("❌ Erro ao verificar tabela 'inventario'")
        st.stop()

if MODO_OFFLINE:
    pendentes_local = contar_pendentes_sync(engine) if engine is not None else 0
    st.markdown(
        '<div class="warning-box">⚠️ <strong>Modo Local (sem internet)</strong> — os dados estao sendo salvos '
        f'no seu computador em <code>{LOCAL_DB_PATH}</code> ({pendentes_local} item(ns) aguardando envio). '
        'Assim que a internet/banco em nuvem voltar, o app detecta automaticamente e envia esses dados '
        'para a nuvem.</div>',
        unsafe_allow_html=True
    )
else:
    sync_info = st.session_state.get("ultimo_sync_info")
    if sync_info and sync_info.get("status") == "ok" and (sync_info.get("enviados", 0) > 0 or sync_info.get("exclusoes", 0) > 0):
        partes = []
        if sync_info.get("enviados", 0) > 0:
            partes.append(f"{sync_info['enviados']} item(ns) salvos offline")
        if sync_info.get("exclusoes", 0) > 0:
            partes.append(f"{sync_info['exclusoes']} exclusao(oes)")
        msg = f"☁️ Conexao com a nuvem restabelecida! {' e '.join(partes)} sincronizado(s)."
        if sync_info.get("erros", 0) > 0:
            msg += f" ({sync_info['erros']} falharam e serao tentados novamente)."
        st.success(msg)
        st.session_state.ultimo_sync_info = None
    elif sync_info and sync_info.get("status") == "erro":
        st.warning(f"⚠️ Conexao com a nuvem restabelecida, mas houve erro ao sincronizar dados locais: {sync_info.get('mensagem', '')}")
        st.session_state.ultimo_sync_info = None

if AVISO_POOL:
    st.markdown(f'<div class="warning-box">{AVISO_POOL}</div>', unsafe_allow_html=True)

# ============================================================
# INICIALIZACAO MULTISESSAO / MULTIOBRA
# ============================================================
if "obra_id" not in st.session_state:
    st.session_state.obra_id = "obra_001"

obra_id_atual = st.session_state.obra_id
db_key = f"db_{obra_id_atual}"

if db_key not in st.session_state:
    st.session_state[db_key] = carregar_do_banco(obra_id_atual)

if "codigo_ocr" not in st.session_state:
    st.session_state.codigo_ocr = ""

if "busca_ativo" not in st.session_state:
    st.session_state.busca_ativo = st.session_state.codigo_ocr

if "arquivo_processado" not in st.session_state:
    st.session_state.arquivo_processado = False

if "arquivos_processados" not in st.session_state:
    st.session_state.arquivos_processados = set()

if "confirmar_excluir_obra" not in st.session_state:
    st.session_state.confirmar_excluir_obra = False

if "confirmar_excluir_tudo" not in st.session_state:
    st.session_state.confirmar_excluir_tudo = False

# ============================================================
# INTERFACE PRINCIPAL
# ============================================================
st.markdown('<h1 style="color: #0F172A; margin-bottom: 8px;">Auditoria de Ativos</h1>', unsafe_allow_html=True)
st.markdown('<p style="color: #64748B; margin-bottom: 24px;">Sistema de auditoria com OCR</p>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Identificacao da Obra")
    obras_existentes = listar_obras_banco()

    obra_input = st.text_input("ID da Obra:", value=obra_id_atual, key="obra_input_field", help="Use um ID unico para cada obra")

    if obras_existentes:
        obra_selecionada = st.selectbox("Ou selecione obra existente:", [""] + obras_existentes, key="obra_select")
        if obra_selecionada:
            obra_input = obra_selecionada

    if obra_input != obra_id_atual:
        st.session_state.obra_id = obra_input
        novo_db_key = f"db_{obra_input}"
        if novo_db_key not in st.session_state:
            st.session_state[novo_db_key] = carregar_do_banco(obra_input)
        st.session_state.codigo_ocr = ""
        st.session_state.busca_ativo = ""
        st.rerun()

    st.markdown("---")
    st.markdown("### Importar Excel")
    arquivo = st.file_uploader("Selecionar ficheiro", type=["xlsx", "xls"], key=f"excel_uploader_{obra_id_atual}")

    if arquivo is not None:
        try:
            # Verificar se arquivo ja foi processado (anti-duplicado)
            arquivo_bytes = arquivo.getvalue()
            arquivo_hash = calcular_hash_arquivo(arquivo_bytes)

            if arquivo_hash in st.session_state.arquivos_processados:
                st.warning("⚠️ Este arquivo ja foi importado anteriormente! Selecione outro arquivo.")
            else:
                with st.spinner("Lendo Excel..."):
                    df_raw = pd.read_excel(arquivo_bytes, header=None, engine='openpyxl')
                    df_clean = limpar_excel(df_raw)

                if df_clean.empty:
                    st.error("Nenhum item valido encontrado no Excel.")
                else:
                    df_clean["Status"] = "Pendente"
                    df_clean["Data Auditoria"] = ""
                    df_clean["Observacoes"] = ""
                    df_clean["Quantidade"] = "1"
                    df_clean["Local"] = ""
                    total = len(df_clean)
                    status_text = st.empty()
                    status_text.text(f"Verificando {total} itens...")
                    resultado = salvar_lote_banco(df_clean, obra_id_atual)
                    status_text.empty()

                    if resultado == False:
                        st.error("Erro ao importar. Tente novamente.")
                    elif isinstance(resultado, dict) and resultado.get("status") == "erro":
                        st.error(f"❌ {resultado['mensagem']}")
                    elif isinstance(resultado, dict) and resultado.get("status") == "carregar":
                        st.session_state[db_key] = carregar_do_banco(obra_id_atual)
                        st.success(f"📋 {resultado['mensagem']}")
                        st.session_state.arquivos_processados.add(arquivo_hash)
                    elif isinstance(resultado, dict) and resultado.get("status") == "sucesso":
                        st.session_state[db_key] = carregar_do_banco(obra_id_atual)
                        msg = f"✅ Importacao concluida em '{obra_id_atual}':"
                        if resultado['novos'] > 0:
                            msg += f" **{resultado['novos']} novos**"
                        if resultado['alterados'] > 0:
                            msg += f" **{resultado['alterados']} atualizados**"
                        if resultado['iguais'] > 0:
                            msg += f" | {resultado['iguais']} sem alteracao"
                        st.success(msg)
                        st.session_state.arquivos_processados.add(arquivo_hash)
                    else:
                        st.session_state[db_key] = carregar_do_banco(obra_id_atual)
                        st.success(f"✅ {total} itens processados em '{obra_id_atual}'")
                        st.session_state.arquivos_processados.add(arquivo_hash)
        except Exception as e:
            st.error(f"Erro ao importar: {e}")

    st.markdown("---")
    st.markdown("### Exportar Excel")
    df_export = st.session_state.get(db_key, pd.DataFrame())
    if df_export.empty:
        df_export = carregar_do_banco(obra_id_atual)
        st.session_state[db_key] = df_export

    if not df_export.empty:
        try:
            excel_bytes = exportar_excel(df_export)
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Baixar Inventario", 
                data=excel_bytes, 
                file_name=f"inventario_{obra_id_atual}_{timestamp}.xlsx", 
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"export_btn_{obra_id_atual}"
            )
        except Exception as e:
            st.error(f"Erro ao exportar: {e}")
    else:
        st.info("Nenhum dado para exportar")

    st.markdown("---")
    st.markdown("### Zona de Perigo")

    if st.button("Limpar Tela da Obra", type="secondary", key="limpar_tela"):
        st.session_state[db_key] = pd.DataFrame(columns=["Codigo do Bem", "Descricao do Bem", "Status", 
                                                           "Data Auditoria", "Observacoes", "Quantidade", "Local"])
        st.success(f"Tela da obra '{obra_id_atual}' limpa!")
        time.sleep(1)
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("⚠️ Excluir Obra do Banco", expanded=False):
        st.warning(f"Isso vai APAGAR permanentemente todos os dados da obra '{obra_id_atual}'!")
        if not st.session_state.confirmar_excluir_obra:
            if st.button("EXCLUIR OBRA", type="primary", key="btn_excluir_obra_1"):
                st.session_state.confirmar_excluir_obra = True
                st.rerun()
        else:
            st.error("⚠️ CONFIRME NOVAMENTE PARA EXCLUIR")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ SIM, EXCLUIR", type="primary", key="btn_excluir_obra_2"):
                    sucesso = deletar_obra_banco(obra_id_atual)
                    if sucesso:
                        if db_key in st.session_state:
                            st.session_state[db_key] = pd.DataFrame(columns=["Codigo do Bem", "Descricao do Bem", "Status", 
                                                                               "Data Auditoria", "Observacoes", "Quantidade", "Local"])
                        st.session_state.confirmar_excluir_obra = False
                        st.success(f"✅ Obra '{obra_id_atual}' excluida!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Erro ao excluir obra.")
                        st.session_state.confirmar_excluir_obra = False
            with col2:
                if st.button("❌ CANCELAR", type="secondary", key="btn_cancelar_obra"):
                    st.session_state.confirmar_excluir_obra = False
                    st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("🚨 EXCLUIR TUDO (App + Banco)", expanded=False):
        st.error("⚠️ ATENCAO: Isso vai APAGAR TODOS os dados de TODAS as obras do BANCO e da MEMORIA do app!")
        if not st.session_state.confirmar_excluir_tudo:
            if st.button("🚨 APAGAR TUDO", type="primary", key="btn_excluir_tudo_1"):
                st.session_state.confirmar_excluir_tudo = True
                st.rerun()
        else:
            st.error("⚠️⚠️ CONFIRME NOVAMENTE - ESTA ACAO NAO PODE SER DESFEITA")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ SIM, APAGAR TUDO", type="primary", key="btn_excluir_tudo_2"):
                    try:
                        sucesso = limpar_tudo_app_e_banco()
                        if sucesso:
                            st.session_state.confirmar_excluir_tudo = False
                            st.success("🗑️ TODOS os dados foram excluidos do banco e da memoria!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Erro ao excluir do banco. Memoria limpa.")
                            st.session_state.confirmar_excluir_tudo = False
                    except Exception as e:
                        st.error(f"Erro: {e}")
                        st.session_state.confirmar_excluir_tudo = False
            with col2:
                if st.button("❌ CANCELAR", type="secondary", key="btn_cancelar_tudo"):
                    st.session_state.confirmar_excluir_tudo = False
                    st.rerun()

    st.markdown("---")
    st.markdown("**Resumo**")
    df_stats = st.session_state.get(db_key, pd.DataFrame())
    if df_stats.empty:
        df_stats = carregar_do_banco(obra_id_atual)
        st.session_state[db_key] = df_stats
    total = len(df_stats)
    auditados = len(df_stats[df_stats["Status"] == "Auditado"]) if not df_stats.empty else 0
    st.metric("Total", total)
    st.metric("Auditados", auditados)
    if total > 0:
        st.progress(auditados / total, text=f"{auditados}/{total}")

# ============================================================
# AREA PRINCIPAL
# ============================================================
df_atual = st.session_state.get(db_key, pd.DataFrame())
if df_atual.empty:
    df_atual = carregar_do_banco(obra_id_atual)
    st.session_state[db_key] = df_atual

if df_atual.empty:
    st.warning("Nenhum inventario carregado. Importe um Excel ou selecione uma obra existente.")
    st.info("""
    Como usar:
    1. Digite um ID de obra (ex: obra_001)
    2. Importe o Excel pela barra lateral
    3. Use a aba "Scanner e OCR" para auditar com foto
    4. Os dados sao salvos automaticamente no CockroachDB
    """)
else:
    tab1, tab2, tab3, tab_sobre = st.tabs(["Scanner e OCR", "Lista Completa", "Dashboard", "Sobre"])

    with tab1:
        st.markdown("### Captura da Etiqueta com OCR")
        if not EASYOCR_DISPONIVEL:
            st.warning("OCR nao disponivel. Verifique se 'easyocr' esta em requirements.txt.")
        else:
            st.info("Dica: Aponte a camera para a etiqueta e tire a foto.")

        foto = st.camera_input("Aponte a camera para a etiqueta e clique em 'Take Photo'")
        codigo_detectado = None

        if foto is not None:
            col1, col2 = st.columns([1, 1])
            with col1:
                st.image(foto, caption="Foto capturada", use_container_width=True)
            with col2:
                if EASYOCR_DISPONIVEL:
                    with st.spinner("Analisando imagem com OCR..."):
                        img = Image.open(foto)
                        texto_ocr = executar_ocr(img)
                        if texto_ocr:
                            st.markdown("**Texto detectado:**")
                            st.code(texto_ocr)
                            codigo_detectado = extrair_codigo_ocr(texto_ocr)
                            if codigo_detectado:
                                st.markdown(f'<div class="ocr-box"><h4>Codigo Detectado: <code>{codigo_detectado}</code></h4></div>', unsafe_allow_html=True)
                                st.session_state.codigo_ocr = codigo_detectado
                                st.session_state.busca_ativo = codigo_detectado
                            else:
                                st.markdown('<div class="warning-box"><h4>Codigo nao reconhecido</h4></div>', unsafe_allow_html=True)
                        else:
                            st.warning("Nenhum texto detectado.")
                else:
                    st.info("OCR desabilitado.")

        st.markdown("---")
        st.markdown("### Confirmar ou Digitar Codigo Manualmente")
        if st.session_state.get("limpar_busca_ativo", False):
            # Tem que limpar o valor do widget ANTES dele ser instanciado nesta
            # execucao - o Streamlit nao permite alterar st.session_state de um
            # widget depois que ele ja foi criado na mesma execucao do script.
            st.session_state.busca_ativo = ""
            st.session_state.limpar_busca_ativo = False
        busca = st.text_input("Numero do Ativo:", placeholder="Ex: 00000333 ou 333", key="busca_ativo")
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("Limpar OCR e Tentar Novamente", key="limpar_ocr"):
                st.session_state.codigo_ocr = ""
                st.session_state.limpar_busca_ativo = True
                st.rerun()

        if busca:
            alvo = normalizar(busca)
            item = df_atual[df_atual["Codigo do Bem"].astype(str).apply(normalizar) == alvo]
            if not item.empty:
                idx = item.index[0]
                status_atual = item.at[idx, "Status"]
                descricao = item.at[idx, "Descricao do Bem"]
                codigo_completo = item.at[idx, "Codigo do Bem"]
                cor_status = "#10B981" if status_atual == "Auditado" else "#F59E0B"
                st.markdown(f'<div class="success-box"><h4>Item Encontrado</h4><p><strong>Codigo:</strong> {codigo_completo}</p><p><strong>Descricao:</strong> {descricao}</p><p><strong>Status:</strong> <span style="color: {cor_status}; font-weight: bold;">{status_atual}</span></p></div>', unsafe_allow_html=True)
                if status_atual == "Auditado":
                    st.info("Este item ja foi auditado!")
                    st.write(f"Data: {item.at[idx, 'Data Auditoria']}")
                    if item.at[idx, "Observacoes"]:
                        st.write(f"Obs: {item.at[idx, 'Observacoes']}")
                else:
                    obs = st.text_area("Observacoes (opcional):", placeholder="Ex: Bom estado...", height=80, key="obs_auditoria")
                    if st.button("CONFIRMAR AUDITORIA", type="primary", key="btn_auditoria"):
                        data_agora = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
                        sucesso = salvar_item_banco(codigo_completo, descricao, "Auditado", data_agora, obs, "1", "", obra_id_atual)
                        if sucesso:
                            st.session_state[db_key].at[idx, "Status"] = "Auditado"
                            st.session_state[db_key].at[idx, "Data Auditoria"] = data_agora
                            st.session_state[db_key].at[idx, "Observacoes"] = obs
                            st.balloons()
                            st.success(f'"{descricao}" auditado com sucesso!')
                            st.session_state.codigo_ocr = ""
                            st.session_state.limpar_busca_ativo = True
                            time.sleep(1.5)
                            st.rerun()
            else:
                st.markdown('<div class="error-box"><h4>Item Nao Encontrado</h4></div>', unsafe_allow_html=True)
                with st.expander("Cadastrar como Sobra"):
                    desc_sobra = st.text_input("Descricao da Sobra:", placeholder="Ex: Computador Dell", key="desc_sobra")
                    obs_sobra = st.text_area("Observacoes:", placeholder="Ex: Encontrado no escritorio...", height=60, key="obs_sobra")
                    if st.button("Salvar como Sobra", key="btn_sobra"):
                        if desc_sobra:
                            # BLOQUEIO ANTI-DUPLICADO: verificar se codigo ja existe
                            if verificar_item_existe(busca, obra_id_atual):
                                st.error(f"⚠️ O codigo '{busca}' ja existe nesta obra! Nao e possivel cadastrar duplicado.")
                            else:
                                data_agora = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
                                salvar_item_banco(busca, f"[SOBRA] {desc_sobra}", "Auditado", data_agora, obs_sobra, "1", "", obra_id_atual)
                                st.session_state[db_key] = carregar_do_banco(obra_id_atual)
                                st.success("Sobra cadastrada!")
                                st.session_state.codigo_ocr = ""
                                st.session_state.limpar_busca_ativo = True
                                time.sleep(1)
                                st.rerun()
                        else:
                            st.warning("Digite uma descricao.")

        st.markdown("---")
        st.markdown("### Cadastrar Item Sem Patrimonio")
        st.markdown('<div class="sem-pat-box"><p>Cadastre itens <strong>sem etiqueta de patrimonio</strong>.</p></div>', unsafe_allow_html=True)

        with st.form("form_sem_patrimonio"):
            col_sp1, col_sp2 = st.columns(2)
            with col_sp1:
                desc_sem_pat = st.text_input("Descricao do Item:", placeholder="Ex: Mesa de escritorio...")
            with col_sp2:
                qtd_sem_pat = st.number_input("Quantidade:", min_value=1, value=1, step=1)
            local_sem_pat = st.text_input("Local / Setor:", placeholder="Ex: Escritorio 3...")
            obs_sem_pat = st.text_area("Observacoes:", placeholder="Ex: Item novo...", height=60)

            submitted = st.form_submit_button("CADASTRAR ITEM SEM PATRIMONIO", type="primary")
            if submitted:
                if desc_sem_pat and local_sem_pat:
                    descricao_final = f"[SEM PATRIMONIO] {desc_sem_pat}"
                    # BLOQUEIO ANTI-DUPLICADO: verificar se descricao ja existe
                    if verificar_descricao_existe(descricao_final, obra_id_atual):
                        st.error(f"⚠️ Ja existe um item com a descricao '{desc_sem_pat}' nesta obra! Nao e possivel cadastrar duplicado.")
                    else:
                        data_agora = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
                        codigo_gerado = f"SEM_PAT_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}_{os.urandom(2).hex().upper()}"
                        sucesso = salvar_item_banco(
                            codigo_gerado, descricao_final,
                            "Auditado", data_agora, obs_sem_pat,
                            str(int(qtd_sem_pat)), local_sem_pat, obra_id_atual
                        )
                        if sucesso:
                            st.session_state[db_key] = carregar_do_banco(obra_id_atual)
                            st.balloons()
                            st.success(f'Item cadastrado!')
                            time.sleep(1.5)
                            st.rerun()
                else:
                    st.warning("Preencha Descricao e Local.")

    with tab2:
        st.markdown("### Inventario Completo")
        df_lista = st.session_state.get(db_key, pd.DataFrame())
        if df_lista.empty:
            df_lista = carregar_do_banco(obra_id_atual)
            st.session_state[db_key] = df_lista

        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filtro_status = st.selectbox("Filtrar por Status:", ["Todos", "Pendente", "Auditado", "Sobra", "Sem Patrimonio"], key="filtro_status")
        with col_f2:
            busca_texto = st.text_input("Buscar:", placeholder="Digite para filtrar...", key="busca_lista")
        df_filtrado = df_lista.copy()
        is_sobra = df_filtrado["Descricao do Bem"].astype(str).str.contains("SOBRA", na=False)
        is_sem_pat = df_filtrado["Descricao do Bem"].astype(str).str.contains("SEM PATRIMONIO", na=False)
        if filtro_status != "Todos":
            if filtro_status == "Sobra":
                df_filtrado = df_filtrado[is_sobra]
            elif filtro_status == "Sem Patrimonio":
                df_filtrado = df_filtrado[is_sem_pat]
            elif filtro_status == "Pendente":
                # Pendente real: nunca inclui Sobra/Sem Patrimonio (esses sempre entram como "Auditado")
                df_filtrado = df_filtrado[(df_filtrado["Status"] == "Pendente") & (~is_sobra) & (~is_sem_pat)]
            elif filtro_status == "Auditado":
                # Auditado real: exclui Sobra/Sem Patrimonio, que tem categoria propria no filtro
                df_filtrado = df_filtrado[(df_filtrado["Status"] == "Auditado") & (~is_sobra) & (~is_sem_pat)]
            else:
                df_filtrado = df_filtrado[df_filtrado["Status"] == filtro_status]
        if busca_texto:
            df_filtrado = df_filtrado[df_filtrado["Descricao do Bem"].str.contains(busca_texto, case=False, na=False)]
        st.dataframe(df_filtrado, use_container_width=True, height=500)
        st.caption(f"Mostrando {len(df_filtrado)} de {len(df_lista)} itens")

    with tab3:
        st.markdown("### Dashboard da Auditoria")
        df_dash = st.session_state.get(db_key, pd.DataFrame())
        if df_dash.empty:
            df_dash = carregar_do_banco(obra_id_atual)
            st.session_state[db_key] = df_dash

        total = len(df_dash)
        if total == 0:
            st.info("Importe dados para ver o dashboard.")
        else:
            col1, col2, col3, col4, col5 = st.columns(5)
            auditados = len(df_dash[df_dash["Status"] == "Auditado"])
            pendentes = len(df_dash[df_dash["Status"] == "Pendente"])
            sobras = len(df_dash[df_dash["Descricao do Bem"].astype(str).str.contains("SOBRA", na=False)])
            sem_pat = len(df_dash[df_dash["Descricao do Bem"].astype(str).str.contains("SEM PATRIMONIO", na=False)])
            with col1:
                st.metric("Total", total)
            with col2:
                st.metric("Auditados", auditados, f"{auditados/total*100:.1f}%")
            with col3:
                st.metric("Pendentes", pendentes, f"-{pendentes/total*100:.1f}%")
            with col4:
                st.metric("Sobras", sobras)
            with col5:
                st.metric("Sem Patrimonio", sem_pat)
            st.markdown("---")
            st.markdown("#### Progresso")
            progresso = auditados / total
            st.progress(progresso, text=f"{auditados}/{total} ({progresso*100:.1f}%)")
            st.markdown("---")
            st.markdown("#### Distribuicao por Status")
            st.bar_chart(df_dash["Status"].value_counts())
            st.markdown("---")
            st.markdown("#### Ultimos Auditados")
            ultimos = df_dash[df_dash["Status"] == "Auditado"].tail(5)
            if not ultimos.empty:
                st.dataframe(ultimos[["Codigo do Bem", "Descricao do Bem", "Data Auditoria", "Observacoes", "Quantidade", "Local"]], use_container_width=True)
            else:
                st.info("Nenhum item auditado.")

    with tab_sobre:
        st.markdown("### Sobre o Sistema")
        st.markdown("""
        <div style="background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%); border-radius: 16px; padding: 32px; color: white; margin-bottom: 24px;">
            <h2 style="color: #38BDF8; margin-top: 0;">Sistema de Auditoria de Ativos</h2>
            <p style="font-size: 16px; line-height: 1.6; color: #CBD5E1;">
                Sistema completo com OCR.
            </p>
        </div>
        """, unsafe_allow_html=True)

        col_about1, col_about2 = st.columns(2)
        with col_about1:
            st.markdown("""
            <div style="background: #F1F5F9; border-radius: 12px; padding: 20px; border-left: 4px solid #3B82F6;">
                <h4 style="color: #1E293B; margin-top: 0;">🚀 Funcionalidades</h4>
                <ul style="color: #475569; line-height: 1.8;">
                    <li>Scanner OCR para leitura automatica</li>
                    <li>Importacao de Excel</li>
                    <li>Cadastro sem patrimonio</li>
                    <li>Cadastro de sobras</li>
                    <li>Dashboard em tempo real</li>
                    <li>Exportacao Excel</li>
                    <li>ACID Transactions (CockroachDB)</li>
                    <li>Sem conflitos multi-usuario</li>
                    <li>Auto-criacao de tabela</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("""
        <div style="background: #ECFDF5; border-radius: 12px; padding: 24px; border: 2px solid #10B981; text-align: center;">
            <h3 style="color: #065F46; margin-top: 0;">👨‍💻 Desenvolvedor</h3>
            <p style="font-size: 20px; color: #047857; font-weight: 700; margin: 8px 0;">
                Robespierre Santana Silva
            </p>
            <p style="font-size: 16px; color: #059669; font-weight: 600; margin: 4px 0;">
                Desenvolvedor de Solucoes de Dados Logisticos
            </p>
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")
st.caption("Sistema de Auditoria de Ativos")
