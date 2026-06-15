import streamlit as st
import pandas as pd
import time
import io
import re
import os
import hashlib
from PIL import Image
from sqlalchemy import create_engine, text
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
# CONFIGURACAO SQLALCHEMY + COCKROACHDB
# ============================================================

def get_engine():
    """Cria engine SQLAlchemy com connection pooling para CockroachDB."""
    db_url = ""
    try:
        db_config = st.secrets.get("database", {})
        db_url = db_config.get("url", "")
    except Exception:
        pass

    if not db_url:
        db_url = os.environ.get("DATABASE_URL", "")

    if not db_url:
        return None, "Nenhuma DATABASE_URL encontrada."

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "cockroachdb://", 1)
    elif db_url.startswith("postgresql://") and not db_url.startswith("cockroachdb"):
        db_url = db_url.replace("postgresql://", "cockroachdb://", 1)
    elif db_url.startswith("postgresql+psycopg2://"):
        db_url = db_url.replace("postgresql+psycopg2://", "cockroachdb+psycopg2://", 1)

    try:
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=1,
            max_overflow=2,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={
                'connect_timeout': 30,
                'options': '-c statement_timeout=60000'
            } if 'sslmode' in db_url else {
                'connect_timeout': 30,
                'options': '-c statement_timeout=60000',
                'sslmode': 'require'
            }
        )
        return engine, None
    except Exception as e:
        return None, f"Erro ao criar engine: {str(e)}"

def test_connection(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            return result.fetchone()[0] == 1
    except Exception:
        return False

def criar_tabela_inventario(engine):
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'inventario'
            """))
            if result.fetchone():
                return True

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

engine, erro_engine = get_engine()

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
                    conn.execute(text("""
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
            for _, row in df.iterrows():
                codigo = str(row["Codigo do Bem"])
                descricao = str(row["Descricao do Bem"])
                if codigo in existentes_dict:
                    if existentes_dict[codigo]["descricao"] != descricao:
                        status_manter = existentes_dict[codigo]["status"] if existentes_dict[codigo]["status"] == "Auditado" else str(row.get("Status", "Pendente"))
                        conn.execute(text("""
                            UPDATE inventario 
                            SET descricao = :descricao, status = :status, updated_at = now()
                            WHERE obra_id = :obra_id AND codigo = :codigo
                        """), {"descricao": descricao, "status": status_manter, "obra_id": obra_id, "codigo": codigo})
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
            result = conn.execute(text("""
                SELECT id FROM inventario 
                WHERE obra_id = :obra_id AND codigo = :codigo
            """), {"obra_id": obra_id, "codigo": str(codigo)})
            existe = result.fetchone()
            if existe:
                conn.execute(text("""
                    UPDATE inventario 
                    SET status = :status, data_auditoria = :data_aud, observacoes = :obs, 
                        quantidade = :qtd, local = :local, updated_at = now()
                    WHERE obra_id = :obra_id AND codigo = :codigo
                """), {"status": status, "data_aud": data_aud, "obs": obs, "qtd": qtd, 
                       "local": local, "obra_id": obra_id, "codigo": str(codigo)})
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
        def _deletar(conn):
            conn.execute(text("DELETE FROM inventario WHERE obra_id = :obra_id"), {"obra_id": obra_id})
            return True
        execute_with_retry(_deletar)
        return True
    except Exception as e:
        st.error(f"Erro ao deletar obra: {e}")
        return False

def deletar_tudo_banco():
    """Deleta TODOS os registros do banco (compativel com CockroachDB)."""
    try:
        def _deletar(conn):
            conn.execute(text("DELETE FROM inventario WHERE TRUE"))
            return True
        execute_with_retry(_deletar)
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
st.set_page_config(page_title="Auditoria de Ativos", page_icon="📋", layout="wide")

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
    </style>
    """, unsafe_allow_html=True)

# ============================================================
# VERIFICACAO INICIAL DO BANCO
# ============================================================
if engine is None:
    st.error(f"❌ DATABASE_URL nao configurada: {erro_engine}")
    st.stop()

with st.spinner("🔌 Conectando ao CockroachDB..."):
    if not test_connection(engine):
        st.error("❌ Nao foi possivel conectar ao CockroachDB!")
        st.stop()
    if not criar_tabela_inventario(engine):
        st.error("❌ Erro ao verificar tabela 'inventario'")
        st.stop()

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
                            else:
                                st.markdown('<div class="warning-box"><h4>Codigo nao reconhecido</h4></div>', unsafe_allow_html=True)
                        else:
                            st.warning("Nenhum texto detectado.")
                else:
                    st.info("OCR desabilitado.")

        st.markdown("---")
        st.markdown("### Confirmar ou Digitar Codigo Manualmente")
        busca = st.text_input("Numero do Ativo:", value=st.session_state.codigo_ocr, placeholder="Ex: 00000333 ou 333", key="busca_ativo")
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("Limpar OCR e Tentar Novamente", key="limpar_ocr"):
                st.session_state.codigo_ocr = ""
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
        if filtro_status != "Todos":
            if filtro_status == "Sobra":
                df_filtrado = df_filtrado[df_filtrado["Descricao do Bem"].astype(str).str.contains("SOBRA", na=False)]
            elif filtro_status == "Sem Patrimonio":
                df_filtrado = df_filtrado[df_filtrado["Descricao do Bem"].astype(str).str.contains("SEM PATRIMONIO", na=False)]
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
