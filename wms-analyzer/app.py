import streamlit as st
import pandas as pd
import os
import tempfile
from datetime import datetime
import base64
import time

from data_processor import WMSDataProcessor
from excel_exporter import ExcelExporter

# Configuração da página
st.set_page_config(
    page_title="Analisador de Vencimentos WMS",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .badge {
        padding: 0.3rem 0.8rem;
        border-radius: 15px;
        font-size: 0.8rem;
        font-weight: bold;
        text-transform: uppercase;
        color: white;
        display: inline-block;
    }
    .badge-expired { background-color: #2c3e50; }
    .badge-high { background-color: #e74c3c; }
    .badge-medium { background-color: #f39c12; }
    .badge-low { background-color: #27ae60; }
    .product-card {
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        background-color: #f9f9f9;
    }
</style>
""", unsafe_allow_html=True)

class WMSAnalyzerApp:
    def __init__(self):
        self.processor = WMSDataProcessor()
        self.exporter = ExcelExporter()
        
        # Inicializar session state
        if 'analysis' not in st.session_state:
            st.session_state.analysis = None
        if 'expanded_state' not in st.session_state:
            st.session_state.expanded_state = {}
    
    def run(self):
        # Header
        st.title("📦 Analisador de Vencimentos WMS")
        st.markdown("### Processamento local com datas DD/MM/AAAA")
        
        # Upload de arquivo
        uploaded_file = st.file_uploader(
            "Envie sua planilha WMS (CSV ou Excel)",
            type=['csv', 'xlsx', 'xls'],
            help="Formatos suportados: CSV, XLSX, XLS. Datas no formato DD/MM/AAAA"
        )
        
        if uploaded_file is not None:
            if st.session_state.analysis is None:
                self.process_uploaded_file(uploaded_file)
            elif st.button("🔄 Reprocessar Arquivo"):
                self.process_uploaded_file(uploaded_file)
            
            if st.session_state.analysis is not None:
                self.display_results(st.session_state.analysis, uploaded_file.name)
    
    def process_uploaded_file(self, uploaded_file):
        """Processar arquivo enviado com barra de progresso"""
        # Salvar arquivo temporariamente
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        try:
            # Barra de progresso
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("📥 Carregando arquivo...")
            progress_bar.progress(20)
            
            # Processar arquivo
            file_type = uploaded_file.name.split('.')[-1].lower()
            df = self.processor.load_data(tmp_path, file_type)
            
            status_text.text("🔍 Processando dados...")
            progress_bar.progress(60)
            
            analysis = self.processor.process_data(df)
            
            status_text.text("✅ Finalizando...")
            progress_bar.progress(90)
            
            # Salvar no session state
            st.session_state.analysis = analysis
            st.session_state.expanded_state = {}
            
            progress_bar.progress(100)
            status_text.text("✅ Análise concluída!")
            
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            
        except Exception as e:
            st.error(f"❌ Erro ao processar arquivo: {str(e)}")
        finally:
            # Limpar arquivo temporário
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def display_results(self, analysis, filename):
        """Exibir resultados da análise"""
        
        # Indicador de processamento
        st.success(f"✅ **Processado** - {analysis['estatisticas']['linhas_processadas']} linhas válidas | {analysis['resumo']['total_produtos']} produtos")
        
        # Cards de métricas rápidas
        self.display_metrics(analysis['resumo'])
        
        # Filtros
        filtered_products = self.display_filters(analysis)
        
        # Botão de exportação
        self.display_export_button(analysis)
        
        # Produtos críticos
        st.markdown("---")
        st.subheader("📋 Produtos Críticos")
        
        if filtered_products:
            # Paginação para melhor performance
            page_size = 10
            total_pages = (len(filtered_products) + page_size - 1) // page_size
            
            if total_pages > 1:
                page = st.number_input("Página", min_value=1, max_value=total_pages, value=1)
                start_idx = (page - 1) * page_size
                end_idx = min(start_idx + page_size, len(filtered_products))
                products_to_show = filtered_products[start_idx:end_idx]
                st.caption(f"Mostrando {start_idx + 1}-{end_idx} de {len(filtered_products)} produtos")
            else:
                products_to_show = filtered_products
            
            # Mostrar produtos
            for i, produto in enumerate(products_to_show):
                self.display_product_card(produto, i)
        else:
            st.warning("ℹ️ Nenhum produto encontrado com os filtros aplicados")
        
        # Recomendações
        st.markdown("---")
        st.subheader("💡 Recomendações")
        
        for recomendacao in analysis['recomendacoes']:
            st.info(recomendacao)
    
    def display_metrics(self, resumo):
        """Exibir cards de métricas"""
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Produtos", resumo['total_produtos'])
        
        with col2:
            st.metric("Total Itens", f"{resumo['total_itens']:,.0f}")
        
        with col3:
            st.metric("⏳ 30 Dias", resumo['produtos_vencendo_30_dias'])
        
        with col4:
            st.metric("⚠️ 60 Dias", resumo['produtos_vencendo_60_dias'])
        
        with col5:
            st.metric("🔴 Vencidos", resumo['produtos_vencidos'])
    
    def display_filters(self, analysis):
        """Exibir e aplicar filtros"""
        st.markdown("---")
        st.subheader("🔍 Filtros")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            fornecedor_filter = st.selectbox(
                "Fornecedor",
                [""] + analysis['filtros']['fornecedores'],
                key="fornecedor_filter"
            )
        
        with col2:
            cod_prod_filter = st.selectbox(
                "Código Produto",
                [""] + analysis['filtros']['cods_produto'],
                key="cod_prod_filter"
            )
        
        with col3:
            criticidade_filter = st.selectbox(
                "Criticidade",
                ["", "vencido", "alta", "média", "baixa"],
                key="criticidade_filter"
            )
        
        with col4:
            dias_filter = st.selectbox(
                "Dias Restantes",
                ["", "Vencidos (<0)", "Críticos (0-30)", "Atenção (31-60)", "Normais (>60)"],
                key="dias_filter"
            )
        
        # Aplicar filtros
        filtered_products = []
        for produto in analysis['produtos_criticos']:
            if fornecedor_filter and fornecedor_filter not in str(produto['fornecedor']):
                continue
            if cod_prod_filter and cod_prod_filter not in str(produto['cod_prod']):
                continue
            if criticidade_filter and produto['criticidade'] != criticidade_filter:
                continue
            if dias_filter:
                if dias_filter == "Vencidos (<0)" and produto['menor_dias_restantes'] >= 0:
                    continue
                elif dias_filter == "Críticos (0-30)" and (produto['menor_dias_restantes'] < 0 or produto['menor_dias_restantes'] > 30):
                    continue
                elif dias_filter == "Atenção (31-60)" and (produto['menor_dias_restantes'] <= 30 or produto['menor_dias_restantes'] > 60):
                    continue
                elif dias_filter == "Normais (>60)" and produto['menor_dias_restantes'] <= 60:
                    continue
            
            filtered_products.append(produto)
        
        st.info(f"📊 Mostrando {len(filtered_products)} de {len(analysis['produtos_criticos'])} produtos")
        return filtered_products
    
    def display_export_button(self, analysis):
        """Exibir botão de exportação"""
        if st.button("📥 Exportar para Excel", use_container_width=True, type="primary"):
            try:
                with st.spinner("Exportando para Excel..."):
                    export_filename = self.exporter.export_to_excel(analysis)
                    
                    with open(export_filename, "rb") as f:
                        bytes_data = f.read()
                        b64 = base64.b64encode(bytes_data).decode()
                        href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{export_filename}" style="display: inline-block; padding: 0.5rem 1rem; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 5px;">📥 Clique para baixar o Excel</a>'
                        st.markdown(href, unsafe_allow_html=True)
                
                # Limpar arquivo temporário
                if os.path.exists(export_filename):
                    os.unlink(export_filename)
                
                st.success("✅ Arquivo exportado com sucesso!")
                
            except Exception as e:
                st.error(f"❌ Erro ao exportar: {str(e)}")
    
    def get_badge_class(self, criticidade):
        """Retornar classe do badge baseado na criticidade"""
        if criticidade == 'vencido':
            return 'badge-expired'
        elif criticidade == 'alta':
            return 'badge-high'
        elif criticidade == 'média':
            return 'badge-medium'
        else:
            return 'badge-low'
    
    def display_product_card(self, produto, index):
        """Exibir card do produto realmente clicável"""
        badge_class = self.get_badge_class(produto['criticidade'])
        produto_key = f"prod_{produto['cod_prod']}_{index}"
        
        # Card principal
        with st.container():
            # Header do card
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.markdown(f"### {produto['nome']}")
                st.caption(f"**Código:** {produto['cod_prod']} | **Fornecedor:** {produto['fornecedor']}")
            
            with col2:
                st.markdown(f"<div class='badge {badge_class}' style='text-align: center;'>{produto['criticidade'].upper()}</div>", 
                           unsafe_allow_html=True)
            
            # Métricas rápidas
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📅 Dias Restantes", produto['menor_dias_restantes'])
            
            with col2:
                st.metric("📦 Quantidade Total", f"{produto['quantidade_total']:,.0f}")
            
            with col3:
                vencimentos_count = len(produto['vencimentos_por_mes'])
                st.metric("📊 Vencimentos", vencimentos_count)
            
            # Expander para detalhes (AGORA FUNCIONANDO)
            expander_label = f"📋 Ver detalhes completos - {produto['nome']}"
            with st.expander(expander_label):
                self.display_product_details(produto, produto_key)
            
            st.markdown("---")
    
    def display_product_details(self, produto, produto_key):
        """Exibir detalhes do produto dentro do expander"""
        # Abas para organização
        tab1, tab2 = st.tabs(["📊 Resumo", "📋 Itens Detalhados"])
        
        with tab1:
            # Vencimentos por mês
            st.subheader("Vencimentos por Mês")
            if produto['vencimentos_por_mes']:
                vencimentos_data = []
                for venc in produto['vencimentos_por_mes']:
                    vencimentos_data.append({
                        'Mês/Ano': venc['mes_ano'],
                        'Quantidade': venc['quantidade'],
                        'Dias Restantes': venc['dias_restantes']
                    })
                
                df_vencimentos = pd.DataFrame(vencimentos_data)
                st.dataframe(df_vencimentos, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum vencimento registrado")
            
            # Informações adicionais
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Código Fornecedor:** {produto['cod_fornec']}")
                st.write(f"**Peso Líquido:** {produto['peso_liqun']}")
            
            with col2:
                total_itens = len(produto['itens_detalhados'])
                st.write(f"**Total de Itens:** {total_itens}")
                if produto['itens_detalhados']:
                    dias_min = min(item['dias_restantes'] for item in produto['itens_detalhados'])
                    dias_max = max(item['dias_restantes'] for item in produto['itens_detalhados'])
                    st.write(f"**Variação Validade:** {dias_min} a {dias_max} dias")
        
        with tab2:
            # Itens detalhados
            if produto['itens_detalhados']:
                # Opção para mostrar todos os itens
                mostrar_todos = st.checkbox("Mostrar todos os itens", value=False, key=f"show_all_{produto_key}")
                
                # Limitar itens para performance
                items_to_show = produto['itens_detalhados'] if mostrar_todos else produto['itens_detalhados'][:20]
                
                # Criar DataFrame simplificado para performance
                dados_itens = []
                for item in items_to_show:
                    dados_itens.append({
                        'Endereço': item['cod_endereco'],
                        'Depósito': item['deposito'],
                        'Rua': item['rua'],
                        'Prédio': item['predio'],
                        'Nível': item['nivel'],
                        'Apto': item['apto'],
                        'Quantidade': item['quantidade'],
                        'Data Validade': item['data_validade'].strftime('%d/%m/%Y'),
                        'Dias Restantes': item['dias_restantes'],
                        'Status': item['status']
                    })
                
                df_itens = pd.DataFrame(dados_itens)
                st.dataframe(df_itens, use_container_width=True, hide_index=True,
                           height=min(400, len(df_itens) * 35 + 38))
                
                if not mostrar_todos and len(produto['itens_detalhados']) > 20:
                    st.info(f"Mostrando 20 de {len(produto['itens_detalhados'])} itens. Marque a opção acima para ver todos.")
                
                # Estatísticas rápidas
                st.subheader("📈 Estatísticas dos Itens")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_itens = len(produto['itens_detalhados'])
                    st.metric("Total Itens", total_itens)
                
                with col2:
                    quantidade_positiva = sum(1 for item in produto['itens_detalhados'] if item['quantidade'] > 0)
                    st.metric("Itens Positivos", quantidade_positiva)
                
                with col3:
                    dias_min = min(item['dias_restantes'] for item in produto['itens_detalhados'])
                    st.metric("Menor Validade", f"{dias_min} dias")
                
                with col4:
                    dias_max = max(item['dias_restantes'] for item in produto['itens_detalhados'])
                    st.metric("Maior Validade", f"{dias_max} dias")
                
                # Download dos itens
                csv = df_itens.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Baixar itens em CSV",
                    data=csv,
                    file_name=f"itens_{produto['cod_prod']}.csv",
                    mime="text/csv",
                    key=f"download_{produto_key}"
                )
                
            else:
                st.info("ℹ️ Nenhum item detalhado disponível para este produto")

# Executar aplicação
if __name__ == "__main__":
    app = WMSAnalyzerApp()
    app.run()