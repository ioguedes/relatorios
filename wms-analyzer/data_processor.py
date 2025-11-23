import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WMSDataProcessor:
    def __init__(self):
        self.hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"Data de referência: {self.hoje.strftime('%d/%m/%Y')}")
    
    def parse_date(self, date_string):
        """Parse de data no formato DD/MM/AAAA"""
        if pd.isna(date_string) or date_string in ['', 'NULL', 'null', 'NaN', 'Invalid Date']:
            return None
        
        str_date = str(date_string).strip()
        logger.debug(f"Tentando parsear data: {str_date}")
        
        # Tentar formato DD/MM/AAAA
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', str_date):
            try:
                day, month, year = map(int, str_date.split('/'))
                date = datetime(year, month, day)
                logger.debug(f"✅ Data parseada (DD/MM/AAAA): {str_date} -> {date.strftime('%d/%m/%Y')}")
                return date
            except ValueError as e:
                logger.warning(f"❌ Erro ao parsear data DD/MM/AAAA: {str_date} - {e}")
        
        # Tentar formato DD-MM-AAAA
        elif re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', str_date):
            try:
                day, month, year = map(int, str_date.split('-'))
                date = datetime(year, month, day)
                logger.debug(f"✅ Data parseada (DD-MM-AAAA): {str_date} -> {date.strftime('%d/%m/%Y')}")
                return date
            except ValueError as e:
                logger.warning(f"❌ Erro ao parsear data DD-MM-AAAA: {str_date} - {e}")
        
        # Tentar formato AAAA-MM-DD (ISO)
        elif re.match(r'^\d{4}-\d{1,2}-\d{1,2}', str_date):
            try:
                date_part = str_date.split(' ')[0]  # Remove hora se existir
                year, month, day = map(int, date_part.split('-'))
                date = datetime(year, month, day)
                logger.debug(f"✅ Data parseada (AAAA-MM-DD): {str_date} -> {date.strftime('%d/%m/%Y')}")
                return date
            except ValueError as e:
                logger.warning(f"❌ Erro ao parsear data AAAA-MM-DD: {str_date} - {e}")
        
        # Tentar como número de série do Excel
        try:
            excel_num = float(str_date)
            if 0 < excel_num < 50000:
                date = datetime(1899, 12, 30) + timedelta(days=excel_num)
                logger.debug(f"✅ Data parseada (Excel): {str_date} -> {date.strftime('%d/%m/%Y')}")
                return date
        except ValueError:
            pass
        
        logger.warning(f"❌ Formato de data não reconhecido: {str_date}")
        return None
    
    def detect_delimiter(self, first_line):
        """Detectar delimitador do arquivo"""
        if '|' in first_line:
            return '|'
        elif ';' in first_line:
            return ';'
        else:
            return ','
    
    def load_data(self, file_path, file_type):
        """Carregar dados do arquivo"""
        try:
            if file_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, dtype=str)
            else:  # CSV
                # Ler primeira linha para detectar delimitador
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline()
                
                delimiter = self.detect_delimiter(first_line)
                logger.info(f"Delimitador detectado: {delimiter}")
                
                df = pd.read_csv(file_path, delimiter=delimiter, dtype=str, encoding='utf-8')
            
            # Limpar nomes das colunas
            df.columns = df.columns.str.upper().str.strip()
            logger.info(f"Colunas encontradas: {list(df.columns)}")
            logger.info(f"Total de linhas carregadas: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo: {e}")
            raise
    
    def process_data(self, df):
        """Processar dados do WMS"""
        logger.info("Iniciando processamento dos dados...")
        
        # Encontrar colunas
        col_mapping = {
            'qt': self._find_column(df.columns, ['QT', 'QUANTIDADE', 'QUANTITY', 'QTD']),
            'cod_prod': self._find_column(df.columns, ['CODPROD', 'COD_PROD', 'PRODUTO', 'CODIGO', 'COD']),
            'descricao': self._find_column(df.columns, ['DESCRICAO', 'PRODUTO', 'NOME', 'DESCR']),
            'dt_val': self._find_column(df.columns, ['DTVAL', 'VALIDADE', 'VENCIMENTO', 'DATA']),
            'peso': self._find_column(df.columns, ['PESOLIQUN', 'PESO', 'PESO_LIQUIDO']),
            'cod_fornec': self._find_column(df.columns, ['CODFORNEC', 'FORNECEDOR_COD', 'COD_FORNECEDOR']),
            'fornecedor': self._find_column(df.columns, ['FORNECEDOR', 'FORNEC', 'FORNECEDOR_NOME']),
            'cod_endereco': self._find_column(df.columns, ['CODENDERECO', 'ENDERECO', 'LOCAL']),
            'deposito': self._find_column(df.columns, ['DEPOSITO', 'DEPOSITO_COD']),
            'rua': self._find_column(df.columns, ['RUA', 'RUA_COD']),
            'predio': self._find_column(df.columns, ['PREDIO', 'PREDIO_COD']),
            'nivel': self._find_column(df.columns, ['NIVEL', 'NIVEL_COD']),
            'apto': self._find_column(df.columns, ['APTO', 'APARTAMENTO']),
            'status': self._find_column(df.columns, ['STATUS', 'STATUS_PROD']),
            'fator': self._find_column(df.columns, ['FATOR', 'FATOR_EMB']),
            'unidade': self._find_column(df.columns, ['UNIDADE', 'UNID_MED']),
            'capacidade': self._find_column(df.columns, ['CAPACIDADE', 'CAPAC']),
            'qttotpal': self._find_column(df.columns, ['QTTOTPAL', 'TOTAL_PALETE']),
            'pesototal': self._find_column(df.columns, ['PESOTOTAL', 'PESO_TOTAL']),
            'est': self._find_column(df.columns, ['EST', 'ESTOQUE', 'LOTE'])
        }
        
        logger.info(f"Colunas mapeadas: {col_mapping}")
        
        # Validar colunas essenciais
        if not col_mapping['dt_val']:
            raise ValueError("Coluna DTVAL não encontrada no arquivo")
        if not col_mapping['cod_prod']:
            raise ValueError("Coluna CODPROD não encontrada no arquivo")
        
        # Processar dados
        produtos = {}
        estatisticas = {
            'total_linhas': len(df),
            'linhas_processadas': 0,
            'linhas_ignoradas': 0,
            'datas_invalidas': 0
        }
        
        for idx, row in df.iterrows():
            try:
                cod_prod = row[col_mapping['cod_prod']]
                qt_str = row[col_mapping['qt']] if col_mapping['qt'] else '0'
                data_validade = row[col_mapping['dt_val']]
                
                # Validar código do produto
                if pd.isna(cod_prod) or str(cod_prod).strip() == '':
                    estatisticas['linhas_ignoradas'] += 1
                    continue
                
                # Converter quantidade
                try:
                    qt = float(str(qt_str).replace(',', '.')) if qt_str else 0
                except ValueError:
                    qt = 0
                
                # Parse data
                dt_val = self.parse_date(data_validade)
                if not dt_val:
                    estatisticas['datas_invalidas'] += 1
                    estatisticas['linhas_ignoradas'] += 1
                    continue
                
                # Calcular dias restantes
                dias_restantes = (dt_val - self.hoje).days
                
                # Coletar dados completos do produto
                if cod_prod not in produtos:
                    produtos[cod_prod] = {
                        'cod_prod': cod_prod,
                        'nome': row[col_mapping['descricao']] if col_mapping['descricao'] else f"Produto {cod_prod}",
                        'fornecedor': row[col_mapping['fornecedor']] if col_mapping['fornecedor'] else 'Sem fornecedor',
                        'cod_fornec': row[col_mapping['cod_fornec']] if col_mapping['cod_fornec'] else '',
                        'peso_liqun': row[col_mapping['peso']] if col_mapping['peso'] else '',
                        'vencimentos_por_mes': {},
                        'quantidade_total': 0,
                        'menor_dias_restantes': float('inf'),
                        'quantidade_original': qt,
                        'itens_detalhados': []  # Lista de itens individuais
                    }
                
                # Adicionar item detalhado
                item_detalhado = {
                    'cod_endereco': row[col_mapping['cod_endereco']] if col_mapping['cod_endereco'] else '',
                    'deposito': row[col_mapping['deposito']] if col_mapping['deposito'] else '',
                    'rua': row[col_mapping['rua']] if col_mapping['rua'] else '',
                    'predio': row[col_mapping['predio']] if col_mapping['predio'] else '',
                    'nivel': row[col_mapping['nivel']] if col_mapping['nivel'] else '',
                    'apto': row[col_mapping['apto']] if col_mapping['apto'] else '',
                    'status': row[col_mapping['status']] if col_mapping['status'] else '',
                    'quantidade': qt,
                    'data_validade': dt_val,
                    'dias_restantes': dias_restantes,
                    'fator': row[col_mapping['fator']] if col_mapping['fator'] else '',
                    'unidade': row[col_mapping['unidade']] if col_mapping['unidade'] else '',
                    'capacidade': row[col_mapping['capacidade']] if col_mapping['capacidade'] else '',
                    'qttotpal': row[col_mapping['qttotpal']] if col_mapping['qttotpal'] else '',
                    'pesototal': row[col_mapping['pesototal']] if col_mapping['pesototal'] else '',
                    'est': row[col_mapping['est']] if col_mapping['est'] else ''
                }
                
                produtos[cod_prod]['itens_detalhados'].append(item_detalhado)
                
                # Agrupar por mês/ano
                mes_ano = dt_val.strftime('%m/%Y')
                if mes_ano not in produtos[cod_prod]['vencimentos_por_mes']:
                    produtos[cod_prod]['vencimentos_por_mes'][mes_ano] = {
                        'mes_ano': mes_ano,
                        'quantidade': 0,
                        'dias_restantes': dias_restantes
                    }
                
                produtos[cod_prod]['vencimentos_por_mes'][mes_ano]['quantidade'] += qt
                produtos[cod_prod]['quantidade_total'] += qt
                
                if dias_restantes < produtos[cod_prod]['menor_dias_restantes']:
                    produtos[cod_prod]['menor_dias_restantes'] = dias_restantes
                
                estatisticas['linhas_processadas'] += 1
                
            except Exception as e:
                logger.warning(f"Erro ao processar linha {idx}: {e}")
                estatisticas['linhas_ignoradas'] += 1
        
        # Converter para lista e calcular criticidade
        produtos_lista = []
        resumo = {
            'total_produtos': 0,
            'total_itens': 0,
            'produtos_vencendo_30_dias': 0,
            'produtos_vencendo_60_dias': 0,
            'produtos_vencidos': 0
        }
        
        for produto in produtos.values():
            # Ordenar itens detalhados por data de validade
            produto['itens_detalhados'].sort(key=lambda x: x['dias_restantes'])
            
            # Converter vencimentos para lista
            vencimentos = list(produto['vencimentos_por_mes'].values())
            vencimentos.sort(key=lambda x: x['dias_restantes'])
            
            # Calcular criticidade
            if produto['menor_dias_restantes'] < 0:
                criticidade = 'vencido'
                resumo['produtos_vencidos'] += 1
            elif produto['menor_dias_restantes'] <= 30:
                criticidade = 'alta'
                resumo['produtos_vencendo_30_dias'] += 1
            elif produto['menor_dias_restantes'] <= 60:
                criticidade = 'média'
                resumo['produtos_vencendo_60_dias'] += 1
            else:
                criticidade = 'baixa'
            
            produto['vencimentos_por_mes'] = vencimentos
            produto['criticidade'] = criticidade
            produto['menor_dias_restantes'] = (produto['menor_dias_restantes'] 
                                            if produto['menor_dias_restantes'] != float('inf') 
                                            else 999)
            
            produtos_lista.append(produto)
            resumo['total_itens'] += abs(produto['quantidade_total'])
        
        resumo['total_produtos'] = len(produtos_lista)
        
        # Ordenar por criticidade
        produtos_lista.sort(key=lambda x: (
            0 if x['menor_dias_restantes'] < 0 else
            1 if x['menor_dias_restantes'] <= 30 else
            2 if x['menor_dias_restantes'] <= 60 else 3,
            x['menor_dias_restantes']
        ))
        
        logger.info(f"Processamento concluído: {resumo}")
        
        return {
            'resumo': resumo,
            'produtos_criticos': produtos_lista,
            'filtros': self._extrair_filtros(produtos_lista),
            'recomendacoes': self._gerar_recomendacoes(produtos_lista),
            'estatisticas': estatisticas,
            'colunas_mapeadas': col_mapping
        }
    
    def _find_column(self, columns, possible_names):
        """Encontrar coluna pelos nomes possíveis"""
        for col in columns:
            if col in possible_names:
                return col
        
        for col in columns:
            for name in possible_names:
                if name in col:
                    return col
        
        return None
    
    def _extrair_filtros(self, produtos):
        """Extrair opções para filtros"""
        fornecedores = set()
        cods_fornecedor = set()
        pesos_liquidos = set()
        cods_produto = set()
        
        for produto in produtos:
            if produto['fornecedor']:
                fornecedores.add(produto['fornecedor'])
            if produto['cod_fornec']:
                cods_fornecedor.add(str(produto['cod_fornec']))
            if produto['peso_liqun']:
                pesos_liquidos.add(str(produto['peso_liqun']))
            if produto['cod_prod']:
                cods_produto.add(str(produto['cod_prod']))
        
        return {
            'fornecedores': sorted(list(fornecedores)),
            'cods_fornecedor': sorted(list(cods_fornecedor)),
            'pesos_liquidos': sorted(list(pesos_liquidos)),
            'cods_produto': sorted(list(cods_produto))
        }
    
    def _gerar_recomendacoes(self, produtos):
        """Gerar recomendações baseadas nos dados"""
        recomendacoes = []
        
        vencidos = [p for p in produtos if p['menor_dias_restantes'] < 0]
        criticos = [p for p in produtos if p['criticidade'] == 'alta']
        medios = [p for p in produtos if p['criticidade'] == 'média']
        grandes_quantidades = [p for p in produtos if p['quantidade_total'] > 100 and p['criticidade'] != 'baixa']
        
        if vencidos:
            recomendacoes.append(f"⚠️ {len(vencidos)} produtos já vencidos - necessário descarte imediato")
        
        if criticos:
            recomendacoes.append(f"🔴 {len(criticos)} produtos vencem em até 30 dias - priorizar venda/uso")
        
        if medios:
            recomendacoes.append(f"🟡 {len(medios)} produtos vencem em 31-60 dias - atenção necessária")
        
        if grandes_quantidades:
            recomendacoes.append(f"📦 {len(grandes_quantidades)} produtos com grandes quantidades próximas do vencimento - considerar promoções")
        
        if not recomendacoes:
            recomendacoes.append("✅ Situação sob controle - estoque com boa validade")
        
        return recomendacoes