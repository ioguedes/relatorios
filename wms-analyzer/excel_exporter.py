import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ExcelExporter:
    def __init__(self):
        self.data_hoje = datetime.now().strftime('%Y-%m-%d')
    
    def export_to_excel(self, analysis, filename=None):
        """Exportar análise para Excel"""
        try:
            if not filename:
                filename = f"Relatorio_Vencimentos_{self.data_hoje}.xlsx"
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Planilha de Resumo
                self._create_summary_sheet(writer, analysis)
                
                # Planilha com todos os produtos
                self._create_all_products_sheet(writer, analysis)
                
                # Planilha de produtos críticos
                self._create_critical_products_sheet(writer, analysis)
            
            logger.info(f"Arquivo exportado com sucesso: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Erro ao exportar para Excel: {e}")
            raise
    
    def _create_summary_sheet(self, writer, analysis):
        """Criar planilha de resumo"""
        resumo_data = [
            ['RESUMO GERAL'],
            [''],
            ['Total de Produtos', analysis['resumo']['total_produtos']],
            ['Total de Itens', analysis['resumo']['total_itens']],
            ['Produtos Vencendo em 30 dias', analysis['resumo']['produtos_vencendo_30_dias']],
            ['Produtos Vencendo em 60 dias', analysis['resumo']['produtos_vencendo_60_dias']],
            ['Produtos Vencidos', analysis['resumo']['produtos_vencidos']],
            [''],
            ['ESTATÍSTICAS DE PROCESSAMENTO'],
            ['Total de Linhas', analysis['estatisticas']['total_linhas']],
            ['Linhas Processadas', analysis['estatisticas']['linhas_processadas']],
            ['Linhas Ignoradas', analysis['estatisticas']['linhas_ignoradas']],
            ['Datas Inválidas', analysis['estatisticas']['datas_invalidas']],
            [''],
            ['Data da Análise', datetime.now().strftime('%d/%m/%Y %H:%M:%S')]
        ]
        
        df_resumo = pd.DataFrame(resumo_data)
        df_resumo.to_excel(writer, sheet_name='Resumo', index=False, header=False)
    
    def _create_all_products_sheet(self, writer, analysis):
        """Criar planilha com todos os produtos"""
        data = []
        
        for produto in analysis['produtos_criticos']:
            for venc in produto['vencimentos_por_mes']:
                data.append({
                    'Código': produto['cod_prod'],
                    'Produto': produto['nome'],
                    'Fornecedor': produto['fornecedor'],
                    'Cód. Fornecedor': produto['cod_fornec'],
                    'Peso Líquido': produto['peso_liqun'],
                    'Mês/Ano Vencimento': venc['mes_ano'],
                    'Quantidade': venc['quantidade'],
                    'Dias Restantes': venc['dias_restantes'],
                    'Criticidade': produto['criticidade'].upper(),
                    'Quantidade Total': produto['quantidade_total']
                })
        
        if data:
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name='Todos os Produtos', index=False)
    
    def _create_critical_products_sheet(self, writer, analysis):
        """Criar planilha de produtos críticos"""
        data = []
        
        for produto in analysis['produtos_criticos']:
            if produto['criticidade'] in ['vencido', 'alta']:
                for item in produto['itens_detalhados']:
                    data.append({
                        'Código': produto['cod_prod'],
                        'Produto': produto['nome'],
                        'Fornecedor': produto['fornecedor'],
                        'Cód. Fornecedor': produto['cod_fornec'],
                        'Endereço': item['cod_endereco'],
                        'Depósito': item['deposito'],
                        'Rua': item['rua'],
                        'Prédio': item['predio'],
                        'Nível': item['nivel'],
                        'Apto': item['apto'],
                        'Status': item['status'],
                        'Quantidade': item['quantidade'],
                        'Data Validade': item['data_validade'].strftime('%d/%m/%Y'),
                        'Dias Restantes': item['dias_restantes'],
                        'Fator': item['fator'],
                        'Unidade': item['unidade'],
                        'Peso Líquido': produto['peso_liqun'],
                        'Criticidade': produto['criticidade'].upper()
                    })
        
        if data:
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name='Produtos Críticos', index=False)