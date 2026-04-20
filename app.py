"""
Dashboard Financeiro e Resultados - Sistema de Cobrança
Versão 3.2 - Melhorias visuais (contraste, fontes maiores) + Exportação Excel
"""

import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import base64

# ---------- CONFIGURAÇÃO INICIAL ----------
st.set_page_config(page_title="Dashboard Financeiro", page_icon="💰", layout="wide")

# --- ESTILO CSS GLOBAL PARA AUMENTAR FONTES E MELHORAR APARÊNCIA ---
st.markdown("""
<style>
    /* Aumenta fonte geral da página */
    html, body, [class*="css"]  {
        font-size: 18px;
    }
    /* Títulos maiores */
    h1 { font-size: 2.5rem !important; }
    h2 { font-size: 2rem !important; }
    h3 { font-size: 1.6rem !important; }
    /* Métricas (st.metric) maiores */
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1.1rem !important;
    }
    /* Botões maiores */
    .stButton button {
        font-size: 1.1rem !important;
        padding: 0.5rem 1rem !important;
    }
    /* Cards personalizados para assistentes (usando metric) também ficam maiores */
</style>
""", unsafe_allow_html=True)

DB_PATH = "data/cobranca.db"
UPLOAD_FOLDER = "data/uploads"
os.makedirs("data", exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Mapeamento de status
STATUS_MAP = {
    'pendente': '⏳ Pendente',
    'em_tratativa': '📞 Em Tratativa',
    'contatado_sem_exito': '❌ Sem Êxito',
    'acordo_finalizado': '✅ Acordo Finalizado',
    'acordo_pendente': '⏰ Acordo Pendente'
}
STATUS_REVERSE = {v: k for k, v in STATUS_MAP.items()}

# ---------- BANCO DE DADOS (ATUALIZADO) ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            senha_hash TEXT NOT NULL,
            perfil TEXT NOT NULL
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_cliente TEXT UNIQUE NOT NULL,
            razao_social TEXT,
            valor_original REAL,
            juros REAL,
            valor_atualizado REAL,
            tempo_atraso INTEGER,
            emissao TEXT,
            vencimento TEXT,
            tipo_faturamento TEXT,
            vendedor TEXT,
            situacao TEXT,
            historico_contato TEXT,
            assistente_responsavel TEXT,
            status_tratativa TEXT DEFAULT 'pendente',
            observacao TEXT DEFAULT '',
            data_ultima_atualizacao TEXT,
            data_criacao TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    c.execute("PRAGMA table_info(clientes)")
    colunas = [info[1] for info in c.fetchall()]
    if 'data_pagamento_programado' not in colunas:
        c.execute("ALTER TABLE clientes ADD COLUMN data_pagamento_programado TEXT")
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS historico_tratativas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER,
            assistente TEXT,
            acao TEXT,
            status_anterior TEXT,
            status_novo TEXT,
            observacao TEXT,
            data_hora TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (cliente_id) REFERENCES clientes (id)
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS solicitacoes_reabertura (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER NOT NULL,
            assistente TEXT NOT NULL,
            motivo TEXT,
            status TEXT DEFAULT 'pendente',
            data_solicitacao TEXT DEFAULT CURRENT_TIMESTAMP,
            data_resposta TEXT,
            admin_responsavel TEXT,
            FOREIGN KEY (cliente_id) REFERENCES clientes (id)
        )
    ''')
    
    conn.commit()
    conn.close()

def criar_usuarios_iniciais():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    usuarios = [
        ("Edvanison Muniz", "edvanison@empresa.com", "admin123", "admin"),
        ("Jane Xavier", "jane@empresa.com", "jane123", "assistente"),
        ("Renata Kelly", "renata@empresa.com", "renata123", "assistente")
    ]
    for nome, email, senha, perfil in usuarios:
        senha_hash = hashlib.sha256(senha.encode()).hexdigest()
        try:
            c.execute("INSERT INTO usuarios (nome, email, senha_hash, perfil) VALUES (?, ?, ?, ?)",
                      (nome, email, senha_hash, perfil))
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()

def verificar_login(email, senha):
    senha_hash = hashlib.sha256(senha.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT nome, perfil FROM usuarios WHERE email = ? AND senha_hash = ?", (email, senha_hash))
    user = c.fetchone()
    conn.close()
    return user

def processar_upload_excel(arquivo):
    try:
        df = pd.read_excel(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler Excel: {e}")
        return None
    df.columns = df.columns.str.strip()
    colunas_esperadas = [
        'Código do cliente', 'Razão social', 'Valor original', 'Juros',
        'Valor atualizado', 'Tempo de atraso', 'Emissão', 'Vencimento',
        'Tipo de faturamento', 'Vendedor', 'Situação', 'Histórico de contato'
    ]
    if not all(col in df.columns for col in colunas_esperadas):
        st.error("Colunas obrigatórias ausentes.")
        return None
    df['assistente_responsavel'] = df['Tempo de atraso'].apply(
        lambda x: 'Jane Xavier' if x <= 30 else 'Renata Kelly'
    )
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for _, row in df.iterrows():
        codigo = str(row['Código do cliente']).strip()
        if not codigo:
            continue
        c.execute("SELECT id FROM clientes WHERE codigo_cliente = ?", (codigo,))
        existente = c.fetchone()
        emissao_str = row['Emissão'].strftime('%Y-%m-%d') if hasattr(row['Emissão'], 'strftime') else str(row['Emissão'])
        vencimento_str = row['Vencimento'].strftime('%Y-%m-%d') if hasattr(row['Vencimento'], 'strftime') else str(row['Vencimento'])
        try:
            valor_original = float(row['Valor original'])
            juros = float(row['Juros'])
            valor_atualizado = float(row['Valor atualizado'])
            tempo_atraso = int(row['Tempo de atraso'])
        except:
            continue
        if existente:
            c.execute('''
                UPDATE clientes SET razao_social=?, valor_original=?, juros=?, valor_atualizado=?,
                tempo_atraso=?, emissao=?, vencimento=?, tipo_faturamento=?, vendedor=?,
                situacao=?, historico_contato=?, assistente_responsavel=?, data_ultima_atualizacao=CURRENT_TIMESTAMP
                WHERE id=?
            ''', (row['Razão social'], valor_original, juros, valor_atualizado, tempo_atraso,
                  emissao_str, vencimento_str, row['Tipo de faturamento'], row['Vendedor'],
                  row['Situação'], row['Histórico de contato'], row['assistente_responsavel'], existente[0]))
        else:
            c.execute('''
                INSERT INTO clientes (codigo_cliente, razao_social, valor_original, juros, valor_atualizado,
                tempo_atraso, emissao, vencimento, tipo_faturamento, vendedor, situacao, historico_contato,
                assistente_responsavel, status_tratativa, observacao, data_pagamento_programado)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendente', '', NULL)
            ''', (codigo, row['Razão social'], valor_original, juros, valor_atualizado, tempo_atraso,
                  emissao_str, vencimento_str, row['Tipo de faturamento'], row['Vendedor'],
                  row['Situação'], row['Histórico de contato'], row['assistente_responsavel']))
    conn.commit()
    conn.close()
    return df

def atualizar_status_cliente(cliente_id, novo_status, observacao, assistente, data_pagamento=None):
    try:
        cliente_id = int(cliente_id)
    except:
        return False
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT status_tratativa FROM clientes WHERE id = ?", (cliente_id,))
    resultado = c.fetchone()
    if not resultado:
        conn.close()
        return False
    status_anterior = resultado[0]
    
    if data_pagamento:
        c.execute('''
            UPDATE clientes SET status_tratativa=?, observacao=?, data_pagamento_programado=?,
            data_ultima_atualizacao=CURRENT_TIMESTAMP WHERE id=?
        ''', (novo_status, observacao, data_pagamento, cliente_id))
    else:
        c.execute('''
            UPDATE clientes SET status_tratativa=?, observacao=?,
            data_ultima_atualizacao=CURRENT_TIMESTAMP WHERE id=?
        ''', (novo_status, observacao, cliente_id))
    
    c.execute('''
        INSERT INTO historico_tratativas (cliente_id, assistente, acao, status_anterior, status_novo, observacao)
        VALUES (?, ?, 'atualizacao_status', ?, ?, ?)
    ''', (cliente_id, assistente, status_anterior, novo_status, observacao))
    conn.commit()
    conn.close()
    return True

def carregar_clientes_assistente(nome):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM clientes WHERE assistente_responsavel = ?", conn, params=(nome,))
    conn.close()
    return df

def criar_solicitacao_reabertura(cliente_id, assistente, motivo):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO solicitacoes_reabertura (cliente_id, assistente, motivo, status)
        VALUES (?, ?, ?, 'pendente')
    ''', (cliente_id, assistente, motivo))
    conn.commit()
    conn.close()

def listar_solicitacoes_pendentes():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('''
        SELECT s.id, s.cliente_id, c.codigo_cliente, c.razao_social, s.assistente, s.motivo, s.data_solicitacao
        FROM solicitacoes_reabertura s
        JOIN clientes c ON s.cliente_id = c.id
        WHERE s.status = 'pendente'
        ORDER BY s.data_solicitacao
    ''', conn)
    conn.close()
    return df

def processar_solicitacao(solicitacao_id, aprovado, admin_nome):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    novo_status_solic = 'aprovada' if aprovado else 'rejeitada'
    c.execute('''
        UPDATE solicitacoes_reabertura
        SET status=?, data_resposta=CURRENT_TIMESTAMP, admin_responsavel=?
        WHERE id=?
    ''', (novo_status_solic, admin_nome, solicitacao_id))
    
    if aprovado:
        c.execute("SELECT cliente_id FROM solicitacoes_reabertura WHERE id=?", (solicitacao_id,))
        cliente_id = c.fetchone()[0]
        c.execute("UPDATE clientes SET status_tratativa='em_tratativa', data_ultima_atualizacao=CURRENT_TIMESTAMP WHERE id=?", (cliente_id,))
        c.execute('''
            INSERT INTO historico_tratativas (cliente_id, assistente, acao, status_anterior, status_novo, observacao)
            VALUES (?, 'Sistema', 'reabertura_aprovada', 'acordo_finalizado', 'em_tratativa', ?)
        ''', (cliente_id, f"Reabertura aprovada por {admin_nome}"))
    conn.commit()
    conn.close()

# ---------- INICIALIZAÇÃO ----------
init_db()
criar_usuarios_iniciais()

if "autenticado" not in st.session_state:
    st.session_state.autenticado = False
    st.session_state.usuario = None
    st.session_state.perfil = None

if not st.session_state.autenticado:
    st.title("🔐 Login")
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        email = st.text_input("Email")
        senha = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            user = verificar_login(email, senha)
            if user:
                st.session_state.autenticado = True
                st.session_state.usuario = user[0]
                st.session_state.perfil = user[1]
                st.rerun()
            else:
                st.error("Credenciais inválidas.")
    st.stop()

# ---------- INTERFACE PRINCIPAL ----------
st.sidebar.title(f"👤 {st.session_state.usuario}")
st.sidebar.write(f"Perfil: **{st.session_state.perfil}**")

if st.session_state.perfil == "admin":
    menu = st.sidebar.radio("Menu", ["📤 Upload", "📊 Dashboard Geral", "🔄 Solicitações de Reabertura", "📥 Exportar Dados"])
else:
    menu = st.sidebar.radio("Menu", ["📋 Meus Clientes", "📊 Meu Dashboard"])

# ---------- ADMIN ----------
if st.session_state.perfil == "admin":
    if menu == "📤 Upload":
        st.header("Upload da Planilha")
        arquivo = st.file_uploader("Selecione o arquivo Excel", type=["xlsx", "xls"])
        if arquivo:
            with st.spinner("Processando..."):
                df = processar_upload_excel(arquivo)
                if df is not None:
                    st.success(f"{len(df)} registros processados.")
                    st.dataframe(df.head(10))

    elif menu == "📊 Dashboard Geral":
        st.header("Dashboard Gerencial")
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM clientes", conn)
        conn.close()
        if df.empty:
            st.info("Sem dados.")
            st.stop()

        # ---- Cards estilizados com alto contraste ----
        total_clientes = len(df)
        total_valor = df['valor_atualizado'].sum()
        inad_valor = df[df['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_inad = (inad_valor / total_valor * 100) if total_valor else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div style="background-color:#1E3A8A; padding:20px; border-radius:15px; box-shadow: 2px 2px 8px rgba(0,0,0,0.2);">
                <h3 style="margin:0; color:#FFFFFF;">📋 Total de Clientes</h3>
                <h1 style="margin:0; color:#FDE047;">{total_clientes}</h1>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style="background-color:#065F46; padding:20px; border-radius:15px; box-shadow: 2px 2px 8px rgba(0,0,0,0.2);">
                <h3 style="margin:0; color:#FFFFFF;">💰 Valor Total em Aberto</h3>
                <h1 style="margin:0; color:#A7F3D0;">R$ {total_valor:,.2f}</h1>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            cor_fundo = "#991B1B" if percent_inad > 3 else "#14532D"
            cor_valor = "#FCA5A5" if percent_inad > 3 else "#BBF7D0"
            st.markdown(f"""
            <div style="background-color:{cor_fundo}; padding:20px; border-radius:15px; box-shadow: 2px 2px 8px rgba(0,0,0,0.2);">
                <h3 style="margin:0; color:#FFFFFF;">⚠️ Inadimplência</h3>
                <h1 style="margin:0; color:{cor_valor};">{percent_inad:.2f}%</h1>
                <p style="color:#E5E7EB;">Meta ≤3%</p>
            </div>
            """, unsafe_allow_html=True)

        # ---- Status com valor total (cards) ----
        st.subheader("📈 Status das Tratativas (Global)")
        status_list = ['pendente', 'em_tratativa', 'contatado_sem_exito', 'acordo_finalizado', 'acordo_pendente']
        cols = st.columns(len(status_list))
        for i, status in enumerate(status_list):
            df_status = df[df['status_tratativa'] == status]
            qtd = len(df_status)
            valor = df_status['valor_atualizado'].sum()
            with cols[i]:
                st.metric(STATUS_MAP[status], f"{qtd} clientes", f"R$ {valor:,.2f}")

        # ---- Gráficos e insights adicionais ----
        st.subheader("📊 Análise Comparativa por Assistente")
        df_assistente = df.groupby('assistente_responsavel').agg(
            Valor_Total=('valor_atualizado', 'sum'),
            Clientes_Em_Atraso=('tempo_atraso', lambda x: (x > 0).sum()),
            Clientes_Total=('codigo_cliente', 'count')
        ).reset_index()
        df_assistente['Taxa_Inadimplencia'] = (df_assistente['Clientes_Em_Atraso'] / df_assistente['Clientes_Total'] * 100)
        
        fig = px.bar(df_assistente, x='assistente_responsavel', y='Valor_Total', text='Clientes_Em_Atraso',
                     color='Taxa_Inadimplencia', color_continuous_scale='RdYlGn_r',
                     labels={'Valor_Total': 'Valor Total (R$)', 'assistente_responsavel': 'Assistente'})
        st.plotly_chart(fig, use_container_width=True)

        # Top 5 motivos de atraso
        st.subheader("🔍 Principais Motivos de Atraso")
        motivos = df['observacao'].dropna().str.extract(r'^([^:]+):')
        if not motivos.empty:
            motivos_count = motivos[0].value_counts().head(5)
            fig_motivos = px.pie(values=motivos_count.values, names=motivos_count.index, title='Distribuição de Motivos')
            st.plotly_chart(fig_motivos, use_container_width=True)
        else:
            st.info("Ainda não há dados de motivos registrados.")

        # Pagamentos programados (próximos 30 dias)
        st.subheader("📅 Pagamentos Programados (Próximos 30 dias)")
        if 'data_pagamento_programado' in df.columns:
            df['data_pagamento_programado'] = pd.to_datetime(df['data_pagamento_programado'], errors='coerce')
            hoje = datetime.now()
            limite = hoje + timedelta(days=30)
            df_prox = df[(df['data_pagamento_programado'] >= hoje) & (df['data_pagamento_programado'] <= limite)]
            if not df_prox.empty:
                df_prox_display = df_prox[['razao_social', 'data_pagamento_programado', 'valor_atualizado', 'assistente_responsavel']]
                df_prox_display['data_pagamento_programado'] = df_prox_display['data_pagamento_programado'].dt.strftime('%d/%m/%Y')
                st.dataframe(df_prox_display, use_container_width=True)
            else:
                st.info("Nenhum pagamento programado para os próximos 30 dias.")
        else:
            st.info("Nenhum pagamento programado registrado.")

        # Top 10 inadimplentes
        st.subheader("🔴 Top 10 Inadimplentes")
        top_inad = df.nlargest(10, 'valor_atualizado')[['razao_social', 'valor_atualizado', 'tempo_atraso', 'assistente_responsavel']]
        st.dataframe(top_inad, use_container_width=True)

    elif menu == "🔄 Solicitações de Reabertura":
        st.header("Solicitações de Reabertura de Acordos")
        df_solicitacoes = listar_solicitacoes_pendentes()
        if df_solicitacoes.empty:
            st.info("Nenhuma solicitação pendente.")
        else:
            for _, row in df_solicitacoes.iterrows():
                with st.expander(f"Cliente {row['codigo_cliente']} - {row['razao_social']} (Solicitado por {row['assistente']})"):
                    st.write(f"**Motivo:** {row['motivo']}")
                    st.write(f"**Data:** {row['data_solicitacao']}")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(f"✅ Aprovar", key=f"apr_{row['id']}"):
                            processar_solicitacao(row['id'], True, st.session_state.usuario)
                            st.success("Solicitação aprovada!")
                            st.rerun()
                    with col2:
                        if st.button(f"❌ Rejeitar", key=f"rej_{row['id']}"):
                            processar_solicitacao(row['id'], False, st.session_state.usuario)
                            st.success("Solicitação rejeitada.")
                            st.rerun()

    elif menu == "📥 Exportar Dados":
        st.header("Exportar Base de Dados Completa")
        st.markdown("Clique no botão abaixo para baixar um arquivo Excel com todos os clientes e suas respectivas tratativas.")
        conn = sqlite3.connect(DB_PATH)
        df_export = pd.read_sql_query("""
            SELECT 
                codigo_cliente, razao_social, valor_original, juros, valor_atualizado, tempo_atraso,
                emissao, vencimento, tipo_faturamento, vendedor, situacao, historico_contato,
                assistente_responsavel, status_tratativa, observacao, data_pagamento_programado,
                data_ultima_atualizacao
            FROM clientes
            ORDER BY assistente_responsavel, status_tratativa
        """, conn)
        conn.close()
        
        if df_export.empty:
            st.warning("Ainda não há dados para exportar.")
        else:
            # Converter para Excel
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_export.to_excel(writer, index=False, sheet_name='Clientes')
            processed_data = output.getvalue()
            
            st.download_button(
                label="📥 Baixar base completa (Excel)",
                data=processed_data,
                file_name=f"base_cobranca_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.dataframe(df_export.head(10))

# ---------- ASSISTENTE ----------
else:
    df_clientes = carregar_clientes_assistente(st.session_state.usuario)

    if menu == "📋 Meus Clientes":
        st.header(f"Clientes de {st.session_state.usuario}")

        if df_clientes.empty:
            st.info("Nenhum cliente atribuído.")
            st.stop()

        # --- Cards de status clicáveis (melhorados) ---
        st.subheader("📊 Status das Tratativas")
        status_list = ['pendente', 'em_tratativa', 'contatado_sem_exito', 'acordo_finalizado', 'acordo_pendente']
        cols = st.columns(len(status_list))
        if 'filtro_status' not in st.session_state:
            st.session_state.filtro_status = None

        cores_card = {
            'pendente': '#6B7280',
            'em_tratativa': '#2563EB',
            'contatado_sem_exito': '#DC2626',
            'acordo_finalizado': '#059669',
            'acordo_pendente': '#D97706'
        }

        for i, status in enumerate(status_list):
            df_status = df_clientes[df_clientes['status_tratativa'] == status]
            qtd = len(df_status)
            valor = df_status['valor_atualizado'].sum()
            cor = cores_card.get(status, '#4B5563')
            with cols[i]:
                card_html = f"""
                <div style="background-color:{cor}; padding:15px; border-radius:15px; text-align:center; margin-bottom:10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1);">
                    <h4 style="color:white; margin:0;">{STATUS_MAP[status]}</h4>
                    <h2 style="color:white; margin:5px 0;">{qtd}</h2>
                    <p style="color:#FDE047; margin:0;">R$ {valor:,.2f}</p>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)
                if st.button("Filtrar", key=f"card_{status}"):
                    st.session_state.filtro_status = status
                    st.rerun()

        if st.session_state.filtro_status:
            st.info(f"Filtrando por: {STATUS_MAP[st.session_state.filtro_status]}")
            if st.button("❌ Limpar filtro"):
                st.session_state.filtro_status = None
                st.rerun()
            df_filtrado = df_clientes[df_clientes['status_tratativa'] == st.session_state.filtro_status]
        else:
            df_filtrado = df_clientes

        # --- Lista de clientes (filtrada) ---
        st.subheader("📋 Lista de Clientes")
        if not df_filtrado.empty:
            codigos = df_filtrado['codigo_cliente'].tolist()
            codigo_sel = st.selectbox(
                "Selecione um cliente:",
                codigos,
                format_func=lambda c: f"{c} - {df_filtrado[df_filtrado['codigo_cliente']==c]['razao_social'].iloc[0]}"
            )
            if codigo_sel:
                conn = sqlite3.connect(DB_PATH)
                cliente_df = pd.read_sql_query(
                    "SELECT * FROM clientes WHERE codigo_cliente=? AND assistente_responsavel=?",
                    conn, params=(codigo_sel, st.session_state.usuario)
                )
                conn.close()
                if cliente_df.empty:
                    st.error("Cliente não encontrado.")
                else:
                    cliente = cliente_df.iloc[0]
                    cliente_id = int(cliente['id'])
                    status_atual = cliente['status_tratativa']

                    with st.expander("📄 Detalhes", expanded=True):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Código:** {cliente['codigo_cliente']}")
                            st.write(f"**Razão:** {cliente['razao_social']}")
                            st.write(f"**Valor Atualizado:** R$ {cliente['valor_atualizado']:,.2f}")
                            st.write(f"**Atraso:** {cliente['tempo_atraso']} dias")
                        with col2:
                            st.write(f"**Vencimento:** {cliente['vencimento']}")
                            st.write(f"**Vendedor:** {cliente['vendedor']}")
                            st.write(f"**Status:** {STATUS_MAP.get(status_atual, status_atual)}")
                            if 'data_pagamento_programado' in cliente and cliente['data_pagamento_programado']:
                                st.write(f"**Pagamento Programado:** {cliente['data_pagamento_programado']}")

                    # --- Ações conforme status ---
                    if status_atual == 'pendente':
                        if st.button("🔔 Pegar para Tratativa"):
                            if atualizar_status_cliente(cliente_id, 'em_tratativa', f"Pego por {st.session_state.usuario}", st.session_state.usuario):
                                st.success("Cliente em tratativa!")
                                st.rerun()
                    elif status_atual in ['em_tratativa', 'contatado_sem_exito', 'acordo_pendente']:
                        with st.form("form_tratativa"):
                            novo_status_raw = st.selectbox(
                                "Novo Status",
                                options=['em_tratativa', 'contatado_sem_exito', 'acordo_finalizado', 'acordo_pendente'],
                                format_func=lambda x: STATUS_MAP[x]
                            )
                            motivos_opcoes = ['', 'Vencimento fim de semana', 'Repasse de verba', 
                                              'Problemas financeiros', 'Erro de programação', 
                                              'Mudança de Pessoal', 'Contato não atende!']
                            motivo = st.selectbox("Motivo (opcional)", motivos_opcoes)
                            obs = st.text_area("Observações")
                            
                            data_pagamento = st.date_input("Data de Pagamento Programado (opcional)", value=None, min_value=datetime.today())
                            
                            if st.form_submit_button("Registrar"):
                                obs_completa = f"{motivo}: {obs}" if motivo else obs
                                data_str = data_pagamento.strftime('%Y-%m-%d') if data_pagamento else None
                                if atualizar_status_cliente(cliente_id, novo_status_raw, obs_completa, st.session_state.usuario, data_str):
                                    st.success("Atualizado!")
                                    st.rerun()
                    elif status_atual == 'acordo_finalizado':
                        st.warning("Este cliente possui acordo finalizado. Para reabrir, solicite autorização do administrador.")
                        with st.form("form_reabertura"):
                            motivo_reabertura = st.text_area("Justificativa para reabertura")
                            if st.form_submit_button("📩 Solicitar Reabertura"):
                                if motivo_reabertura.strip():
                                    criar_solicitacao_reabertura(cliente_id, st.session_state.usuario, motivo_reabertura)
                                    st.success("Solicitação enviada ao administrador.")
                                    st.rerun()
                                else:
                                    st.error("Descreva o motivo da reabertura.")

                    # Histórico
                    st.subheader("📜 Histórico")
                    conn = sqlite3.connect(DB_PATH)
                    hist = pd.read_sql_query(
                        "SELECT data_hora, assistente, status_anterior, status_novo, observacao FROM historico_tratativas WHERE cliente_id=? ORDER BY data_hora DESC",
                        conn, params=(cliente_id,)
                    )
                    conn.close()
                    if not hist.empty:
                        st.dataframe(hist, use_container_width=True)
                    else:
                        st.info("Sem histórico.")
        else:
            st.info("Nenhum cliente com este status.")

    elif menu == "📊 Meu Dashboard":
        st.header("Meu Desempenho")
        if df_clientes.empty:
            st.info("Sem dados.")
            st.stop()

        conn = sqlite3.connect(DB_PATH)
        df_global = pd.read_sql_query("SELECT * FROM clientes", conn)
        conn.close()

        # Indicador global
        total_global = df_global['valor_atualizado'].sum()
        inad_global = df_global[df_global['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_global = (inad_global / total_global * 100) if total_global else 0
        st.metric("🌍 Inadimplência Global", f"{percent_global:.2f}%", delta="Meta ≤3%" if percent_global <=3 else "Acima da meta")

        # Métricas individuais
        total_ind = df_clientes['valor_atualizado'].sum()
        inad_ind = df_clientes[df_clientes['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_ind = (inad_ind / total_ind * 100) if total_ind else 0
        col1, col2, col3 = st.columns(3)
        col1.metric("Meu Valor Aberto", f"R$ {total_ind:,.2f}")
        col2.metric("Minha Inadimplência", f"{percent_ind:.2f}%")
        col3.metric("Clientes em Atraso", len(df_clientes[df_clientes['tempo_atraso'] > 0]))

        # Cards de status (melhorados)
        st.subheader("📊 Status das Minhas Tratativas")
        status_list = ['pendente', 'em_tratativa', 'contatado_sem_exito', 'acordo_finalizado', 'acordo_pendente']
        cols = st.columns(len(status_list))
        cores_card = {
            'pendente': '#6B7280',
            'em_tratativa': '#2563EB',
            'contatado_sem_exito': '#DC2626',
            'acordo_finalizado': '#059669',
            'acordo_pendente': '#D97706'
        }
        for i, status in enumerate(status_list):
            df_status = df_clientes[df_clientes['status_tratativa'] == status]
            qtd = len(df_status)
            valor = df_status['valor_atualizado'].sum()
            with cols[i]:
                card_html = f"""
                <div style="background-color:{cores_card[status]}; padding:15px; border-radius:15px; text-align:center;">
                    <h4 style="color:white; margin:0;">{STATUS_MAP[status]}</h4>
                    <h2 style="color:white; margin:5px 0;">{qtd}</h2>
                    <p style="color:#FDE047; margin:0;">R$ {valor:,.2f}</p>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)

        # Pagamentos programados
        st.subheader("📅 Meus Pagamentos Programados")
        if 'data_pagamento_programado' in df_clientes.columns:
            df_clientes['data_pagamento_programado'] = pd.to_datetime(df_clientes['data_pagamento_programado'], errors='coerce')
            hoje = datetime.now()
            limite = hoje + timedelta(days=30)
            df_prox = df_clientes[(df_clientes['data_pagamento_programado'] >= hoje) & (df_clientes['data_pagamento_programado'] <= limite)]
            if not df_prox.empty:
                df_prox_display = df_prox[['razao_social', 'data_pagamento_programado', 'valor_atualizado']]
                df_prox_display['data_pagamento_programado'] = df_prox_display['data_pagamento_programado'].dt.strftime('%d/%m/%Y')
                st.dataframe(df_prox_display, use_container_width=True)
            else:
                st.info("Nenhum pagamento programado para os próximos 30 dias.")
        else:
            st.info("Nenhum pagamento programado registrado.")

        # Gráfico de pizza
        st.subheader("Distribuição")
        status_counts = df_clientes['status_tratativa'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Quantidade']
        status_counts['Status'] = status_counts['Status'].map(STATUS_MAP)
        fig = px.pie(status_counts, names='Status', values='Quantidade', hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

        # Top 5 inadimplentes
        st.subheader("🔴 Meus Top 5 Inadimplentes")
        top5 = df_clientes.nlargest(5, 'valor_atualizado')[['razao_social', 'valor_atualizado', 'tempo_atraso']]
        st.dataframe(top5, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.caption("Dashboard Financeiro v3.2")
