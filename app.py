"""
Dashboard Financeiro e Resultados - Sistema de Cobrança
Desenvolvido com Streamlit e SQLite
Versão final com correções de ID e maior robustez
"""

import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# ---------- CONFIGURAÇÃO INICIAL ----------
st.set_page_config(
    page_title="Dashboard Financeiro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes
DB_PATH = "data/cobranca.db"
UPLOAD_FOLDER = "data/uploads"

# Criar pastas se não existirem
os.makedirs("data", exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ---------- FUNÇÕES DE BANCO DE DADOS ----------
def init_db():
    """Cria as tabelas no banco de dados SQLite."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Tabela de usuários
    c.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            senha_hash TEXT NOT NULL,
            perfil TEXT NOT NULL
        )
    ''')
    
    # Tabela de clientes (dados da planilha + status interno)
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
    
    # Tabela de histórico de tratativas
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
    
    conn.commit()
    conn.close()

def criar_usuarios_iniciais():
    """Insere os três usuários padrão (se não existirem)."""
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
            pass  # Usuário já existe
    
    conn.commit()
    conn.close()

def verificar_login(email, senha):
    """Verifica credenciais e retorna (nome, perfil) ou None."""
    senha_hash = hashlib.sha256(senha.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT nome, perfil FROM usuarios WHERE email = ? AND senha_hash = ?", (email, senha_hash))
    user = c.fetchone()
    conn.close()
    return user

def processar_upload_excel(arquivo):
    """
    Lê o arquivo Excel, distribui clientes por tempo de atraso e
    insere/atualiza no banco de dados preservando status existentes.
    """
    try:
        df = pd.read_excel(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo Excel: {e}")
        return None
    
    df.columns = df.columns.str.strip()
    
    colunas_esperadas = [
        'Código do cliente', 'Razão social', 'Valor original', 'Juros',
        'Valor atualizado', 'Tempo de atraso', 'Emissão', 'Vencimento',
        'Tipo de faturamento', 'Vendedor', 'Situação', 'Histórico de contato'
    ]
    
    colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
    if colunas_faltantes:
        st.error(f"Colunas não encontradas: {', '.join(colunas_faltantes)}")
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
        
        # Converter datas
        emissao_str = row['Emissão'].strftime('%Y-%m-%d') if hasattr(row['Emissão'], 'strftime') else str(row['Emissão'])
        vencimento_str = row['Vencimento'].strftime('%Y-%m-%d') if hasattr(row['Vencimento'], 'strftime') else str(row['Vencimento'])
        
        try:
            valor_original = float(row['Valor original'])
            juros = float(row['Juros'])
            valor_atualizado = float(row['Valor atualizado'])
            tempo_atraso_int = int(row['Tempo de atraso'])
        except (ValueError, TypeError) as e:
            st.warning(f"Erro nos valores numéricos do cliente {codigo}: {e}")
            continue
        
        if existente:
            cliente_id = existente[0]
            c.execute('''
                UPDATE clientes
                SET razao_social = ?, valor_original = ?, juros = ?, valor_atualizado = ?,
                    tempo_atraso = ?, emissao = ?, vencimento = ?, tipo_faturamento = ?,
                    vendedor = ?, situacao = ?, historico_contato = ?,
                    assistente_responsavel = ?, data_ultima_atualizacao = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                row['Razão social'], valor_original, juros, valor_atualizado,
                tempo_atraso_int, emissao_str, vencimento_str, row['Tipo de faturamento'],
                row['Vendedor'], row['Situação'], row['Histórico de contato'],
                row['assistente_responsavel'], cliente_id
            ))
        else:
            c.execute('''
                INSERT INTO clientes (
                    codigo_cliente, razao_social, valor_original, juros,
                    valor_atualizado, tempo_atraso, emissao, vencimento,
                    tipo_faturamento, vendedor, situacao, historico_contato,
                    assistente_responsavel, status_tratativa, observacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendente', '')
            ''', (
                codigo, row['Razão social'], valor_original, juros,
                valor_atualizado, tempo_atraso_int, emissao_str,
                vencimento_str, row['Tipo de faturamento'], row['Vendedor'],
                row['Situação'], row['Histórico de contato'], row['assistente_responsavel']
            ))
    
    conn.commit()
    conn.close()
    return df

def atualizar_status_cliente(cliente_id, novo_status, observacao, assistente):
    """
    Atualiza status do cliente e registra no histórico.
    Retorna True se bem-sucedido, False caso contrário.
    """
    # Garantir que cliente_id seja inteiro
    try:
        cliente_id = int(cliente_id)
    except (ValueError, TypeError):
        st.error(f"ID de cliente inválido: {cliente_id}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Verificar existência do cliente
    c.execute("SELECT status_tratativa FROM clientes WHERE id = ?", (cliente_id,))
    resultado = c.fetchone()
    
    if resultado is None:
        st.error(f"Cliente com ID {cliente_id} não encontrado no banco de dados.")
        conn.close()
        return False
    
    status_anterior = resultado[0]
    
    # Atualizar cliente
    c.execute('''
        UPDATE clientes
        SET status_tratativa = ?, observacao = ?, data_ultima_atualizacao = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (novo_status, observacao, cliente_id))
    
    # Registrar histórico
    c.execute('''
        INSERT INTO historico_tratativas (cliente_id, assistente, acao, status_anterior, status_novo, observacao)
        VALUES (?, ?, 'atualizacao_status', ?, ?, ?)
    ''', (cliente_id, assistente, status_anterior, novo_status, observacao))
    
    conn.commit()
    conn.close()
    return True

def carregar_clientes_assistente(nome_assistente):
    """Retorna DataFrame com clientes atribuídos a uma assistente."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM clientes WHERE assistente_responsavel = ?",
        conn, params=(nome_assistente,)
    )
    conn.close()
    return df

# ---------- INICIALIZAÇÃO DO BANCO ----------
init_db()
criar_usuarios_iniciais()

# ---------- CONTROLE DE SESSÃO ----------
if "autenticado" not in st.session_state:
    st.session_state.autenticado = False
    st.session_state.usuario = None
    st.session_state.perfil = None

if not st.session_state.autenticado:
    st.title("🔐 Login - Dashboard Financeiro")
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
                st.error("Email ou senha inválidos.")
    st.stop()

# ---------- INTERFACE PRINCIPAL (após login) ----------
st.sidebar.title(f"👤 {st.session_state.usuario}")
st.sidebar.write(f"Perfil: **{st.session_state.perfil}**")

if st.session_state.perfil == "admin":
    menu = st.sidebar.radio("Menu", ["📤 Upload de Planilha", "📊 Dashboard Geral"])
else:
    menu = st.sidebar.radio("Menu", ["📋 Meus Clientes", "📊 Meu Dashboard"])

# ---------- ÁREA DO ADMINISTRADOR ----------
if st.session_state.perfil == "admin":
    if menu == "📤 Upload de Planilha":
        st.header("Upload da Planilha de Clientes")
        st.markdown("""
        Faça o upload da planilha Excel contendo as colunas:
        `Código do cliente`, `Razão social`, `Valor original`, `Juros`, `Valor atualizado`,
        `Tempo de atraso`, `Emissão`, `Vencimento`, `Tipo de faturamento`, `Vendedor`,
        `Situação`, `Histórico de contato`.
        """)
        arquivo = st.file_uploader("Selecione o arquivo Excel", type=["xlsx", "xls"])
        if arquivo is not None:
            with st.spinner("Processando arquivo..."):
                df = processar_upload_excel(arquivo)
                if df is not None:
                    st.success(f"Arquivo processado! {len(df)} registros.")
                    st.dataframe(df.head(10))

    elif menu == "📊 Dashboard Geral":
        st.header("Dashboard Financeiro - Visão Geral")
        conn = sqlite3.connect(DB_PATH)
        df_todos = pd.read_sql_query("SELECT * FROM clientes", conn)
        conn.close()
        
        if df_todos.empty:
            st.info("Nenhum dado disponível.")
            st.stop()
        
        total_clientes = len(df_todos)
        total_valor = df_todos['valor_atualizado'].sum()
        valor_inadimplente = df_todos[df_todos['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_inad = (valor_inadimplente / total_valor * 100) if total_valor > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Clientes", total_clientes)
        col2.metric("Valor Total em Aberto", f"R$ {total_valor:,.2f}")
        col3.metric("Inadimplência (%)", f"{percent_inad:.2f}%",
                   delta=f"Meta ≤3% ({percent_inad-3:.2f} p.p.)" if percent_inad > 3 else "✅ Dentro da meta")
        
        st.subheader("Inadimplência por Assistente")
        df_assistente = df_todos.groupby('assistente_responsavel').agg(
            Valor_Total=('valor_atualizado', 'sum'),
            Clientes_Em_Atraso=('tempo_atraso', lambda x: (x > 0).sum())
        ).reset_index()
        if not df_assistente.empty:
            fig_bar = px.bar(df_assistente, x='assistente_responsavel', y='Valor_Total',
                            text='Clientes_Em_Atraso', color='assistente_responsavel')
            st.plotly_chart(fig_bar, use_container_width=True)
        
        st.subheader("🔴 Top 10 Inadimplentes")
        top_inad = df_todos.nlargest(10, 'valor_atualizado')[
            ['razao_social', 'valor_atualizado', 'tempo_atraso', 'assistente_responsavel']
        ]
        st.dataframe(top_inad, use_container_width=True)
        
        st.subheader("🟢 Top 10 Melhores Pagadores")
        melhores = df_todos[df_todos['tempo_atraso'] == 0].nsmallest(10, 'valor_atualizado')[
            ['razao_social', 'valor_atualizado', 'vencimento']
        ]
        if not melhores.empty:
            st.dataframe(melhores, use_container_width=True)
        else:
            st.info("Nenhum cliente com atraso zero.")

# ---------- ÁREA DAS ASSISTENTES ----------
else:
    if menu == "📋 Meus Clientes":
        st.header(f"Clientes de {st.session_state.usuario}")
        df_clientes = carregar_clientes_assistente(st.session_state.usuario)
        
        if df_clientes.empty:
            st.info("Você ainda não possui clientes atribuídos.")
            st.stop()
        
        # Pesquisa rápida
        st.sidebar.subheader("🔍 Pesquisar Cliente")
        termo_busca = st.sidebar.text_input("Código ou Razão Social")
        if termo_busca:
            conn = sqlite3.connect(DB_PATH)
            query = """
                SELECT * FROM clientes 
                WHERE (codigo_cliente LIKE ? OR razao_social LIKE ?)
                AND assistente_responsavel = ?
            """
            params = [f"%{termo_busca}%", f"%{termo_busca}%", st.session_state.usuario]
            df_busca = pd.read_sql_query(query, conn, params=params)
            conn.close()
            if not df_busca.empty:
                st.sidebar.write("Resultados:")
                for _, row in df_busca.iterrows():
                    if st.sidebar.button(f"{row['codigo_cliente']} - {row['razao_social'][:20]}..."):
                        st.session_state.codigo_cliente_foco = row['codigo_cliente']
                        st.rerun()
            else:
                st.sidebar.write("Nenhum cliente encontrado.")
        
        tab1, tab2 = st.tabs(["⏳ Pendentes", "📋 Todos os Clientes"])
        
        with tab1:
            df_pendentes = df_clientes[df_clientes['status_tratativa'] == 'pendente']
            st.subheader(f"Clientes Aguardando Tratativa ({len(df_pendentes)})")
            
            if df_pendentes.empty:
                st.info("Nenhum cliente pendente.")
            else:
                codigos_pendentes = df_pendentes['codigo_cliente'].tolist()
                default_index = 0
                if 'codigo_cliente_foco' in st.session_state and st.session_state.codigo_cliente_foco in codigos_pendentes:
                    default_index = codigos_pendentes.index(st.session_state.codigo_cliente_foco)
                
                codigo_selecionado = st.selectbox(
                    "Selecione um cliente para iniciar a tratativa:",
                    codigos_pendentes,
                    index=default_index,
                    format_func=lambda cod: f"{cod} - {df_pendentes[df_pendentes['codigo_cliente']==cod]['razao_social'].iloc[0]}"
                )
                
                if codigo_selecionado:
                    # Buscar cliente fresco do banco
                    conn = sqlite3.connect(DB_PATH)
                    cliente_df = pd.read_sql_query(
                        "SELECT * FROM clientes WHERE codigo_cliente = ? AND assistente_responsavel = ?",
                        conn, params=(codigo_selecionado, st.session_state.usuario)
                    )
                    conn.close()
                    
                    if cliente_df.empty:
                        st.error(f"Cliente {codigo_selecionado} não encontrado.")
                        st.stop()
                    
                    cliente = cliente_df.iloc[0]
                    # Converter ID para int nativo do Python (evita problemas com numpy.int64)
                    cliente_id = int(cliente['id'])
                    
                    # Exibir detalhes
                    with st.expander("📄 Detalhes do Cliente", expanded=True):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Código:** {cliente['codigo_cliente']}")
                            st.write(f"**Razão Social:** {cliente['razao_social']}")
                            st.write(f"**Valor Atualizado:** R$ {cliente['valor_atualizado']:,.2f}")
                            st.write(f"**Tempo de Atraso:** {cliente['tempo_atraso']} dias")
                        with col2:
                            st.write(f"**Vencimento:** {cliente['vencimento']}")
                            st.write(f"**Vendedor:** {cliente['vendedor']}")
                            st.write(f"**Situação:** {cliente['situacao']}")
                    
                    # Ações conforme status
                    if cliente['status_tratativa'] == 'pendente':
                        if st.button("🔔 Pegar Cliente para Tratativa"):
                            sucesso = atualizar_status_cliente(
                                cliente_id,
                                'em_tratativa',
                                f"Cliente pego por {st.session_state.usuario}",
                                st.session_state.usuario
                            )
                            if sucesso:
                                st.success("Cliente em tratativa!")
                                st.rerun()
                    
                    elif cliente['status_tratativa'] == 'em_tratativa':
                        st.subheader("Registrar Tratativa")
                        with st.form("form_tratativa"):
                            novo_status = st.selectbox(
                                "Status da Tratativa",
                                options=['contatado_sem_exito', 'acordo_finalizado', 'acordo_pendente', 'em_tratativa']
                            )
                            motivo = st.selectbox(
                                "Motivo do Atraso (opcional)",
                                options=['', 'Vencimento fim de semana', 'Repasse de verba',
                                         'Problemas financeiros', 'Erro de programação', 'Mudança de Pessoal']
                            )
                            observacao = st.text_area("Observações da conversa")
                            submitted = st.form_submit_button("Registrar")
                            
                            if submitted:
                                obs_completa = f"{motivo}: {observacao}" if motivo else observacao
                                sucesso = atualizar_status_cliente(
                                    cliente_id,
                                    novo_status,
                                    obs_completa,
                                    st.session_state.usuario
                                )
                                if sucesso:
                                    st.success("Tratativa registrada!")
                                    st.rerun()
                    
                    # Histórico
                    st.subheader("📜 Histórico de Tratativas")
                    conn = sqlite3.connect(DB_PATH)
                    hist = pd.read_sql_query(
                        "SELECT data_hora, assistente, status_anterior, status_novo, observacao "
                        "FROM historico_tratativas WHERE cliente_id = ? ORDER BY data_hora DESC",
                        conn, params=(cliente_id,)
                    )
                    conn.close()
                    if not hist.empty:
                        st.dataframe(hist, use_container_width=True)
                    else:
                        st.info("Nenhum histórico ainda.")
        
        with tab2:
            st.subheader("Todos os Clientes Atribuídos")
            st.dataframe(df_clientes, use_container_width=True)
    
    elif menu == "📊 Meu Dashboard":
        st.header("Meu Desempenho")
        conn = sqlite3.connect(DB_PATH)
        df_global = pd.read_sql_query("SELECT * FROM clientes", conn)
        df_assistente = df_global[df_global['assistente_responsavel'] == st.session_state.usuario]
        conn.close()
        
        if df_assistente.empty:
            st.info("Você ainda não possui dados.")
            st.stop()
        
        total_global = df_global['valor_atualizado'].sum()
        inad_global = df_global[df_global['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_global = (inad_global / total_global * 100) if total_global > 0 else 0
        
        st.metric("🌍 Inadimplência Global (Meta ≤3%)", f"{percent_global:.2f}%",
                 delta="✅ Dentro da meta" if percent_global <= 3 else "⚠️ Acima da meta")
        
        total_ind = df_assistente['valor_atualizado'].sum()
        inad_ind = df_assistente[df_assistente['tempo_atraso'] > 0]['valor_atualizado'].sum()
        percent_ind = (inad_ind / total_ind * 100) if total_ind > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Meu Valor em Aberto", f"R$ {total_ind:,.2f}")
        col2.metric("Minha Inadimplência", f"{percent_ind:.2f}%")
        col3.metric("Clientes em Atraso", len(df_assistente[df_assistente['tempo_atraso'] > 0]))
        
        st.subheader("Status das Minhas Tratativas")
        status_counts = df_assistente['status_tratativa'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Quantidade']
        fig_pie = px.pie(status_counts, names='Status', values='Quantidade', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)
        
        st.subheader("🔴 Meus Top 5 Inadimplentes")
        top5 = df_assistente.nlargest(5, 'valor_atualizado')[
            ['razao_social', 'valor_atualizado', 'tempo_atraso']
        ]
        st.dataframe(top5, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.caption("Dashboard Financeiro v1.0")