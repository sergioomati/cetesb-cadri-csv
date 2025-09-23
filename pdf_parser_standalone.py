#!/usr/bin/env python3
"""
PDF Parser Standalone - Extração de dados de resíduos de PDFs CADRI
"""

import sys
from pathlib import Path
import fitz  # pymupdf
import pandas as pd
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from store_csv import CSVStore
from logging_conf import logger
from config import PDF_DIR, CSV_CADRI_ITEMS, CSV_CADRI_DOCS


class PDFParserStandalone:
    """Parser de PDFs CADRI para extração de dados de resíduos"""

    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.patterns = self._compile_patterns()
        self.parsed_cache = self._load_parsed_cache()
        self.stats = {
            'processed': 0,
            'items_extracted': 0,
            'errors': 0,
            'no_items': 0,
            'skipped': 0
        }

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup"""
        # Cleanup if needed (currently no resources to cleanup)
        pass

    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """Compilar regex patterns para extração"""
        return {
            # Padrão para número do resíduo (D099, F001, K001, etc.)
            'numero_residuo': re.compile(
                r'\b([A-Z]\d{3})\b',
                re.IGNORECASE
            ),

            # Padrão para código de resíduo (formato: XX.XX.XXX) - manter para compatibilidade
            'codigo': re.compile(
                r'(\d{2}[\s\.]?\d{2}[\s\.]?\d{3})',
                re.IGNORECASE
            ),

            # Padrão para resíduo completo com descrição
            'residuo_linha': re.compile(
                r'(\d{1,2})\s+Resíduo\s*:\s*([A-Z]\d{3})\s*-\s*(.+?)(?=\n[A-Z]|\n\n|Classe\s*:|$)',
                re.MULTILINE | re.IGNORECASE | re.DOTALL
            ),

            # Padrão para classe com estado físico e OII
            'classe_detalhada': re.compile(
                r'Classe\s*:\s*([IVX]+[AB]?)\s+Estado\s+Físico\s*:\s*(\w+)\s+O/?I\s*:\s*([I/O]+)\s+Qtde\s*:\s*([0-9.,]+)\s*([a-zA-Z/\s]+?)(?=\n|Composição|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrão para composição aproximada
            'composicao': re.compile(
                r'Composição\s+Aproximada\s*:\s*(.+?)(?=\s*Método\s+Utilizado|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrão para método utilizado
            'metodo': re.compile(
                r'Método\s+Utilizado\s*:\s*(.+?)(?=\s*Cor[,\.]?\s*Cheiro|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrão para cor, cheiro, aspecto
            'cor_cheiro_aspecto': re.compile(
                r'Cor[,\.]?\s*Cheiro[,\.]?\s*Aspecto\s*:\s*(.+?)(?=\s*Acondicionamento|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrão para acondicionamento (múltiplos)
            'acondicionamento': re.compile(
                r'Acondicionamento\s*:.*?(?=Destino|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrão para itens de acondicionamento individuais
            'acondicionamento_item': re.compile(
                r'(?:Acondicionamento\s*:\s*)?(E\d{2})\s*-\s*([^A]+?)(?=\s*Acondicionamento|Destino|$)',
                re.IGNORECASE
            ),

            # Padrão para destino
            'destino': re.compile(
                r'Destino\s*:\s*(T\d{2})\s*-\s*(.+?)(?=\n\s*\d+\s+Resíduo|$)',
                re.IGNORECASE | re.DOTALL
            ),

            # Padrões melhorados para quantidade e unidade separados
            'quantidade_unidade': re.compile(
                r'Qtde\s*:\s*(\d+[.,]?\d*)\s+([a-zA-Z]+/?[a-zA-Z]*)',
                re.IGNORECASE
            ),

            # Padrão para quantidade genérica (fallback)
            'quantidade': re.compile(
                r'(\d+[.,]?\d*)\s*(kg|ton|t|m3|m³|litro|l|unidade|un|peça|pç|ano)',
                re.IGNORECASE
            ),

            # Padrão para classe de resíduo genérica (fallback)
            'classe': re.compile(
                r'classe\s*:\s*(I{1,3}[AB]?|I{1,2}\s*[AB])',
                re.IGNORECASE
            ),

            # Padrão para estado físico
            'estado_fisico': re.compile(
                r'Estado\s+Físico\s*:\s*(\w+)',
                re.IGNORECASE
            ),

            # Padrão para OII
            'oii': re.compile(
                r'OII\s*:\s*([^\s]+)',
                re.IGNORECASE
            ),

            # Identificar tipo de documento
            'doc_type': re.compile(
                r'(CADRI|Certificado.*Movimentação|Licença|CERT.*MOV.*RESIDUOS)',
                re.IGNORECASE
            ),

            # Data de validade
            'validade': re.compile(
                r'(validade|válida?\s+até|vencimento).*?(\d{2}[/-]\d{2}[/-]\d{4})',
                re.IGNORECASE
            ),

            # Número do documento
            'numero_doc': re.compile(
                r'n[úº°]?\s*(\d{5,})',
                re.IGNORECASE
            ),

            # Padrões de limpeza de texto
            'clean_spaces': re.compile(r'\s+'),
            'clean_breaks': re.compile(r'[\n\r]+'),

            # Padrões para entidade geradora (formato flexível para segunda página)
            'entidade_geradora': re.compile(
                r'autenticidade\.cetesb\.sp\.gov\.br\s*\n\s*([^\n]+?)\s+(\d+-\d+-\d+)\s*\n'
                r'([^\n]+?)\s*\n\s*([^\n]+?)\s*\n'
                r'([^\n]+?)\s+([\d-]+)\s+([^\n]+?)\s*\n'
                r'([^\n]+?)\s*\n'
                r'([^\n]+?)\s+(\d+)',
                re.DOTALL | re.IGNORECASE
            ),

            # Padrões para entidade de destinação (formato flexível)
            'entidade_destinacao': re.compile(
                r'(\d+)\s*\n([^\n]+?)\s+(\d+-\d+-\d+)\s*\n'
                r'([^\n]+?)\s+(\d+)(?:\s+([^\n]+?))?\s*\n'
                r'([^\n]+?)\s+([\d-]+)\s+([^\n]+?)\s*\n'
                r'([^\n]+?)\s*\n'
                r'([^\n]+?)\s+(\d+)\s+([\d/]+)',
                re.DOTALL | re.IGNORECASE
            ),

            # Dados do cabeçalho do documento
            'processo_numero': re.compile(r'Processo\s+N[°º]\s*\n\s*(\d+/\d+/\d+)', re.IGNORECASE),
            'certificado_numero': re.compile(r'N[°º]\s+(\d{8})', re.IGNORECASE),
            'versao_data': re.compile(r'Versão:\s*(\d+)\s*\n\s*Data:\s*([\d/]+)', re.IGNORECASE),
        }

    def _load_parsed_cache(self) -> set:
        """Carregar lista de PDFs já processados"""
        try:
            if Path(CSV_CADRI_ITEMS).exists():
                df = pd.read_csv(CSV_CADRI_ITEMS)
                return set(df['numero_documento'].unique())
        except Exception as e:
            logger.debug(f"Erro ao carregar cache: {e}")
        return set()

    def parse_pdf(self, pdf_path: Path) -> List[Dict]:
        """
        Extrair dados de resíduos de um PDF

        Args:
            pdf_path: Caminho do arquivo PDF

        Returns:
            Lista de itens extraídos
        """
        items = []
        numero_documento = pdf_path.stem

        try:
            # Abrir PDF
            with fitz.open(pdf_path) as doc:
                # Extrair texto de todas as páginas
                full_text = ""
                for page in doc:
                    full_text += page.get_text()

                # Limpeza inicial do texto
                full_text = self._clean_text(full_text)

                # Identificar tipo de documento
                doc_type = self._identify_doc_type(full_text)
                if not doc_type:
                    logger.warning(f"Tipo de documento não identificado: {numero_documento}")
                    doc_type = "DOCUMENTO"

                # Extrair data de validade
                validade = self._extract_date(full_text, 'validade')

                # Extrair metadados do documento (entidades, processo, etc.)
                metadata = self._extract_document_metadata(full_text)
                logger.info(f"DEBUG parse_pdf: Metadados extraídos com {len(metadata)} campos")
                if metadata.get('geradora_nome'):
                    logger.info(f"DEBUG parse_pdf: geradora_nome = '{metadata.get('geradora_nome')}'")
                else:
                    logger.info("DEBUG parse_pdf: geradora_nome NÃO foi extraído")

                # Tentar extração com nova lógica estruturada primeiro
                items = self._extract_residuos_enhanced(
                    full_text,
                    numero_documento,
                    doc_type,
                    validade,
                    metadata
                )

                logger.debug(f"Enhanced parsing found {len(items)} items for {numero_documento}")

                # Se não encontrou nada, tentar método antigo como fallback
                if not items:
                    logger.debug(f"Enhanced parsing failed, trying structured approach for {numero_documento}")
                    residuos_section = self._find_residuos_section(full_text)
                    if residuos_section:
                        items = self._extract_residuos_structured(
                            residuos_section,
                            numero_documento,
                            doc_type,
                            validade,
                            metadata
                        )
                    else:
                        logger.debug(f"Structured approach failed, trying alternative for {numero_documento}")
                        items = self._extract_residuos_alternative(
                            full_text,
                            numero_documento,
                            doc_type,
                            validade,
                            metadata
                        )

                # Validar e limpar itens extraídos
                items = self.validate_extraction(items)

                logger.info(f"📄 {numero_documento}: {len(items)} itens extraídos")

        except Exception as e:
            logger.error(f"Erro ao processar {pdf_path}: {e}")
            self.stats['errors'] += 1

        return items

    def _clean_text(self, text: str) -> str:
        """Limpar e normalizar texto"""
        # Normalizar espaços múltiplos mas manter quebras de linha
        text = re.sub(r'[ \t]+', ' ', text)  # Só espaços e tabs, não quebras
        # Normalizar quebras de linha múltiplas
        text = re.sub(r'\n{3,}', '\n\n', text)  # Máximo 2 quebras seguidas
        return text.strip()

    def _identify_doc_type(self, text: str) -> Optional[str]:
        """Identificar tipo de documento"""
        match = self.patterns['doc_type'].search(text)
        if match:
            doc_type = match.group(1).upper()
            # Normalizar
            if 'CADRI' in doc_type:
                return 'CADRI'
            elif 'CERTIFICADO' in doc_type or 'CERT' in doc_type:
                return 'CERT_MOV_RESIDUOS'
            elif 'LICENÇA' in doc_type:
                return 'LICENCA'
        return None

    def _extract_date(self, text: str, date_type: str) -> Optional[str]:
        """Extrair data do texto"""
        if date_type == 'validade':
            match = self.patterns['validade'].search(text)
            if match:
                date_str = match.group(2)
                # Normalizar formato
                date_str = date_str.replace('/', '-')
                try:
                    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                    return date_obj.strftime('%Y-%m-%d')
                except:
                    return date_str
        return None

    def _find_residuos_section(self, text: str) -> Optional[str]:
        """Localizar seção de resíduos no texto"""
        # Marcadores de início da seção de resíduos
        markers = [
            r'RESÍDUOS\s+AUTORIZADOS',
            r'RELAÇÃO\s+DE\s+RESÍDUOS',
            r'RESÍDUOS\s+CLASSE',
            r'CÓDIGO.*DESCRIÇÃO.*QUANTIDADE',
            r'ITEM.*RESÍDUO.*CLASSE',
            r'RESÍDUOS\s+A\s+SEREM\s+TRATADOS',
            r'LISTA\s+DE\s+RESÍDUOS'
        ]

        for marker in markers:
            pattern = re.compile(marker, re.IGNORECASE | re.DOTALL)
            match = pattern.search(text)
            if match:
                # Retornar texto a partir do marcador
                start_pos = match.start()
                # Pegar próximos 3000 caracteres
                section = text[start_pos:start_pos+3000]
                logger.debug(f"Seção encontrada com marcador: {marker}")
                return section

        return None

    def _extract_residuos_structured(
        self,
        section: str,
        numero_documento: str,
        doc_type: str,
        validade: Optional[str],
        metadata: Dict[str, str] = None
    ) -> List[Dict]:
        """Extrair resíduos de uma seção estruturada"""
        items = []
        lines = section.split('\n')
        current_item = {}

        for line_num, line in enumerate(lines):
            line = line.strip()

            if not line or len(line) < 5:
                # Linha vazia ou muito curta, salvar item se completo
                if current_item.get('codigo') and current_item.get('descricao'):
                    items.append(self._create_item_dict(
                        current_item, numero_documento, doc_type, validade
                    ))
                    current_item = {}
                continue

            # Procurar código de resíduo
            codigo_match = self.patterns['codigo'].search(line)
            if codigo_match:
                # Salvar item anterior se existir
                if current_item.get('codigo') and current_item.get('descricao'):
                    items.append(self._create_item_dict(
                        current_item, numero_documento, doc_type, validade
                    ))

                # Iniciar novo item
                current_item = {
                    'codigo': self._normalize_codigo(codigo_match.group(1)),
                    'descricao': line[codigo_match.end():].strip(),
                    'linha_original': line
                }

                # Procurar dados adicionais na mesma linha
                self._extract_additional_data(line, current_item)
                continue

            # Se já temos um código, adicionar à descrição
            if current_item.get('codigo'):
                if current_item.get('descricao'):
                    current_item['descricao'] += ' ' + line
                else:
                    current_item['descricao'] = line

                # Procurar dados adicionais
                self._extract_additional_data(line, current_item)

        # Salvar último item se existir
        if current_item.get('codigo') and current_item.get('descricao'):
            items.append(self._create_item_dict(
                current_item, numero_documento, doc_type, validade
            ))

        return items

    def _extract_residuos_alternative(
        self,
        text: str,
        numero_documento: str,
        doc_type: str,
        validade: Optional[str],
        metadata: Dict[str, str] = None
    ) -> List[Dict]:
        """Extração alternativa quando não há seção clara"""
        items = []

        # Procurar todos os códigos de resíduo
        codigos = self.patterns['codigo'].findall(text)
        codigos_unicos = list(set(codigos))

        for codigo in codigos_unicos:
            codigo_norm = self._normalize_codigo(codigo)

            # Tentar encontrar contexto ao redor do código
            escaped_codigo = re.escape(codigo)
            pattern = re.compile(
                rf'{escaped_codigo}[^\n]*(?:\n[^\n]*)?',
                re.IGNORECASE
            )
            match = pattern.search(text)

            if match:
                context = match.group(0)

                # Extrair descrição (texto após o código)
                codigo_pos = context.find(codigo)
                if codigo_pos >= 0:
                    descricao = context[codigo_pos + len(codigo):].strip()
                    descricao = re.sub(r'\s+', ' ', descricao)[:500]

                    item_data = {
                        'codigo': codigo_norm,
                        'descricao': descricao,
                        'linha_original': context.replace('\n', ' ')
                    }

                    # Extrair dados adicionais
                    self._extract_additional_data(context, item_data)

                    items.append(self._create_item_dict(
                        item_data, numero_documento, doc_type, validade
                    ))

        return items

    def _extract_additional_data(self, text: str, item_data: Dict):
        """Extrair quantidade, unidade e classe do texto"""
        # Procurar quantidade
        qtd_match = self.patterns['quantidade'].search(text)
        if qtd_match and not item_data.get('quantidade'):
            item_data['quantidade'] = qtd_match.group(1).replace(',', '.')
            item_data['unidade'] = qtd_match.group(2).lower()

        # Procurar classe
        classe_match = self.patterns['classe'].search(text)
        if classe_match and not item_data.get('classe'):
            item_data['classe'] = classe_match.group(1).replace(' ', '')

    def _normalize_codigo(self, codigo: str) -> str:
        """Normalizar código de resíduo para formato XX.XX.XXX"""
        # Remover espaços e caracteres especiais
        codigo_clean = re.sub(r'[^\d]', '', codigo)

        # Adicionar pontos no formato correto
        if len(codigo_clean) == 7:
            return f"{codigo_clean[:2]}.{codigo_clean[2:4]}.{codigo_clean[4:]}"

        return codigo

    def _create_item_dict(
        self,
        item_data: Dict,
        numero_documento: str,
        doc_type: str,
        validade: Optional[str]
    ) -> Dict:
        """Criar dicionário padronizado do item"""
        return {
            'numero_documento': numero_documento,
            'tipo_documento': doc_type,
            'codigo_residuo': item_data.get('codigo', ''),
            'descricao_residuo': item_data.get('descricao', ''),
            'quantidade': item_data.get('quantidade', ''),
            'unidade': item_data.get('unidade', ''),
            'classe': item_data.get('classe', ''),
            'data_validade': validade,
            'observacao': '',
            'linha_original': item_data.get('linha_original', ''),
            'data_extracao': datetime.now().isoformat()
        }

    def _extract_document_metadata(self, text: str) -> Dict[str, str]:
        """Extrair metadados do documento (entidades, processo, etc.)"""
        metadata = {}

        # Extrair dados da entidade geradora (padrão simplificado)
        geradora_match = self.patterns['entidade_geradora'].search(text)
        if geradora_match:
            # Grupos: 1=nome, 2=cadastro, 3=logradouro, 4=numero/complemento,
            # 5=bairro, 6=cep, 7=municipio, 8=atividade, 9=bacia, 10=funcionarios

            # Processar logradouro e número
            logradouro_completo = geradora_match.group(3).strip()
            numero_complemento = geradora_match.group(4).strip()

            # Separar número e complemento se necessário
            numero_parts = numero_complemento.split(None, 1)
            numero = numero_parts[0] if numero_parts else ''
            complemento = numero_parts[1] if len(numero_parts) > 1 else ''

            metadata.update({
                'geradora_nome': geradora_match.group(1).strip(),
                'geradora_cadastro_cetesb': geradora_match.group(2),
                'geradora_logradouro': logradouro_completo,
                'geradora_numero': numero,
                'geradora_complemento': complemento,
                'geradora_bairro': geradora_match.group(5).strip(),
                'geradora_cep': geradora_match.group(6),
                'geradora_municipio': geradora_match.group(7).strip(),
                'geradora_atividade': geradora_match.group(8).strip(),
                'geradora_bacia_hidrografica': geradora_match.group(9).strip(),
                'geradora_funcionarios': geradora_match.group(10),
                'geradora_uf': 'SP'
            })

            logger.debug(f"Entidade geradora encontrada: {metadata['geradora_nome']}")

        # Extrair dados da entidade de destinação
        # Procurar padrão após a entidade geradora
        destino_start = text.find('LEANDRO RAMIRES')
        if destino_start > 0:
            destino_section = text[destino_start-20:destino_start+500]
            destino_match = self.patterns['entidade_destinacao'].search(destino_section)

            if destino_match:
                # Grupos: 1=funcionarios_geradora, 2=nome, 3=cadastro, 4=logradouro,
                # 5=numero, 6=complemento, 7=bairro, 8=cep, 9=municipio,
                # 10=atividade, 11=bacia, 12=licenca, 13=data_licenca

                metadata.update({
                    'destino_entidade_nome': destino_match.group(2).strip(),
                    'destino_entidade_cadastro_cetesb': destino_match.group(3),
                    'destino_entidade_logradouro': destino_match.group(4).strip(),
                    'destino_entidade_numero': destino_match.group(5),
                    'destino_entidade_complemento': destino_match.group(6).strip() if destino_match.group(6) else '',
                    'destino_entidade_bairro': destino_match.group(7).strip(),
                    'destino_entidade_cep': destino_match.group(8),
                    'destino_entidade_municipio': destino_match.group(9).strip(),
                    'destino_entidade_atividade': destino_match.group(10).strip(),
                    'destino_entidade_bacia_hidrografica': destino_match.group(11).strip(),
                    'destino_entidade_licenca': destino_match.group(12),
                    'destino_entidade_data_licenca': destino_match.group(13),
                    'destino_entidade_uf': 'SP'
                })

                logger.debug(f"Entidade de destinação encontrada: {metadata['destino_entidade_nome']}")

        # Extrair número do processo
        processo_match = self.patterns['processo_numero'].search(text)
        if processo_match:
            metadata['numero_processo'] = processo_match.group(1)
            logger.debug(f"Número do processo: {metadata['numero_processo']}")

        # Extrair número do certificado
        cert_match = self.patterns['certificado_numero'].search(text)
        if cert_match:
            metadata['numero_certificado'] = cert_match.group(1)
            logger.debug(f"Número do certificado: {metadata['numero_certificado']}")

        # Extrair versão e data do documento
        versao_data_match = self.patterns['versao_data'].search(text)
        if versao_data_match:
            metadata['versao_documento'] = versao_data_match.group(1)
            metadata['data_documento'] = versao_data_match.group(2)
            logger.debug(f"Versão: {metadata['versao_documento']}, Data: {metadata['data_documento']}")

        return metadata

    def _extract_residuos_enhanced(
        self,
        text: str,
        numero_documento: str,
        doc_type: str,
        validade: Optional[str],
        metadata: Dict[str, str] = None
    ) -> List[Dict]:
        """
        Extração melhorada baseada na estrutura identificada na imagem

        Procura por blocos estruturados contendo:
        - Número do resíduo (ex: D099)
        - Descrição completa
        - Classe, Estado Físico, OII, Quantidade
        - Composição, Método, Cor/Cheiro/Aspecto
        - Acondicionamento e Destino
        """
        items = []

        # Procurar por padrões de início de resíduo: "01 Resíduo : D099 - ..."
        residuo_matches = self.patterns['residuo_linha'].finditer(text)

        for match in residuo_matches:
            item_numero = match.group(1)
            numero_residuo = match.group(2)
            descricao_inicial = match.group(3)

            # Encontrar o bloco completo do resíduo
            start_pos = match.start()

            # Procurar próximo resíduo ou fim do texto
            next_match = None
            for next_candidate in self.patterns['residuo_linha'].finditer(text[start_pos + len(match.group(0)):]):
                next_match = next_candidate
                break

            if next_match:
                end_pos = start_pos + len(match.group(0)) + next_match.start()
            else:
                # Se não há próximo, pegar próximos 2000 caracteres
                end_pos = min(start_pos + 2000, len(text))

            # Extrair bloco completo do resíduo
            residuo_block = text[start_pos:end_pos]

            # Extrair campos estruturados do bloco
            item_data = self._extract_structured_fields(residuo_block)

            # Dados básicos
            item_data.update({
                'item_numero': item_numero.zfill(2),
                'numero_residuo': numero_residuo,
                'descricao_residuo': descricao_inicial.strip(),
                'raw_fragment': residuo_block[:500]  # Primeiros 500 chars
            })

            # Criar item completo
            complete_item = self._create_enhanced_item_dict(
                item_data, numero_documento, doc_type, validade, metadata
            )

            # Debug: verificar se metadata está sendo passado
            if item_numero == '01':
                logger.info(f"DEBUG: metadata is None? {metadata is None}")
                if metadata:
                    logger.info(f"DEBUG: Metadata tem {len(metadata)} campos")
                    logger.info(f"DEBUG: geradora_nome em metadata: '{metadata.get('geradora_nome', 'NAO ENCONTRADO')}'")
                if complete_item:
                    logger.info(f"DEBUG: geradora_nome no item: '{complete_item.get('geradora_nome', 'NAO ENCONTRADO')}'")

            if complete_item:
                items.append(complete_item)
                logger.debug(f"Extraído resíduo {numero_residuo}: {descricao_inicial[:50]}...")

        return items

    def _extract_structured_fields(self, block: str) -> Dict[str, str]:
        """Extrair campos estruturados de um bloco de resíduo"""
        fields = {}

        # Classe, Estado Físico, OII, Quantidade (em uma linha)
        classe_match = self.patterns['classe_detalhada'].search(block)
        if classe_match:
            fields['classe_residuo'] = classe_match.group(1)
            fields['estado_fisico'] = classe_match.group(2).upper()
            fields['oii'] = classe_match.group(3)
            fields['quantidade'] = classe_match.group(4).replace(',', '.')
            # Normalizar unidade (remover " / ano" etc.)
            unidade_raw = classe_match.group(5).strip()
            fields['unidade'] = unidade_raw.split()[0] if unidade_raw else ''
        else:
            # Tentar padrões individuais

            # Quantidade e unidade
            qtd_match = self.patterns['quantidade_unidade'].search(block)
            if qtd_match:
                fields['quantidade'] = qtd_match.group(1).replace(',', '.')
                fields['unidade'] = qtd_match.group(2)

            # Classe
            classe_match = self.patterns['classe'].search(block)
            if classe_match:
                fields['classe_residuo'] = classe_match.group(1)

            # Estado físico
            estado_match = self.patterns['estado_fisico'].search(block)
            if estado_match:
                fields['estado_fisico'] = estado_match.group(1).upper()

            # OII
            oii_match = self.patterns['oii'].search(block)
            if oii_match:
                fields['oii'] = oii_match.group(1)

        # Composição aproximada
        comp_match = self.patterns['composicao'].search(block)
        if comp_match:
            fields['composicao_aproximada'] = comp_match.group(1).strip()

        # Método utilizado
        metodo_match = self.patterns['metodo'].search(block)
        if metodo_match:
            fields['metodo_utilizado'] = metodo_match.group(1).strip()

        # Cor, cheiro, aspecto
        cor_match = self.patterns['cor_cheiro_aspecto'].search(block)
        if cor_match:
            fields['cor_cheiro_aspecto'] = cor_match.group(1).strip()

        # Acondicionamento - extrair todos os códigos E## do bloco
        e_codes = re.findall(r'E\d{2}', block)
        if e_codes:
            # Remover duplicatas mantendo ordem
            unique_codes = []
            for code in e_codes:
                if code not in unique_codes:
                    unique_codes.append(code)

            fields['acondicionamento_codigos'] = ','.join(unique_codes)

            # Tentar extrair descrições correspondentes
            descricoes = []
            for code in unique_codes:
                # Procurar padrão "E## - descrição"
                desc_match = re.search(rf'{code}\s*-\s*([^A-Z]+?)(?=\s+[A-Z]|$)', block, re.IGNORECASE)
                if desc_match:
                    desc = desc_match.group(1).strip()
                    # Limpar descrição
                    desc = desc.split('Acondicionamento')[0].strip()
                    descricoes.append(desc)
                else:
                    descricoes.append('')

            fields['acondicionamento_descricoes'] = ' | '.join(filter(None, descricoes))

        # Destino
        destino_match = self.patterns['destino'].search(block)
        if destino_match:
            fields['destino_codigo'] = destino_match.group(1)
            fields['destino_descricao'] = destino_match.group(2).strip()

        return fields

    def _create_enhanced_item_dict(
        self,
        item_data: Dict,
        numero_documento: str,
        doc_type: str,
        validade: Optional[str],
        metadata: Dict[str, str] = None
    ) -> Optional[Dict]:
        """Criar dicionário de item com novos campos expandidos"""

        # Validação básica
        if not item_data.get('numero_residuo') or not item_data.get('descricao_residuo'):
            return None

        result = {
            'numero_documento': numero_documento,
            'item_numero': item_data.get('item_numero', '01'),
            'numero_residuo': item_data.get('numero_residuo', ''),
            'descricao_residuo': item_data.get('descricao_residuo', ''),
            'classe_residuo': item_data.get('classe_residuo', ''),
            'estado_fisico': item_data.get('estado_fisico', ''),
            'oii': item_data.get('oii', ''),
            'quantidade': item_data.get('quantidade', ''),
            'unidade': item_data.get('unidade', ''),
            'composicao_aproximada': item_data.get('composicao_aproximada', ''),
            'metodo_utilizado': item_data.get('metodo_utilizado', ''),
            'cor_cheiro_aspecto': item_data.get('cor_cheiro_aspecto', ''),
            'acondicionamento_codigos': item_data.get('acondicionamento_codigos', ''),
            'acondicionamento_descricoes': item_data.get('acondicionamento_descricoes', ''),
            'destino_codigo': item_data.get('destino_codigo', ''),
            'destino_descricao': item_data.get('destino_descricao', ''),
            'pagina_origem': 1,  # TODO: detectar página correta
            'raw_fragment': item_data.get('raw_fragment', ''),
            'tipo_documento': doc_type,
            'data_validade': validade,
        }

        # Adicionar metadados se disponíveis
        if metadata is not None:
            # Dados da entidade geradora
            result.update({
                'geradora_nome': metadata.get('geradora_nome', ''),
                'geradora_cadastro_cetesb': metadata.get('geradora_cadastro_cetesb', ''),
                'geradora_logradouro': metadata.get('geradora_logradouro', ''),
                'geradora_numero': metadata.get('geradora_numero', ''),
                'geradora_complemento': metadata.get('geradora_complemento', ''),
                'geradora_bairro': metadata.get('geradora_bairro', ''),
                'geradora_cep': metadata.get('geradora_cep', ''),
                'geradora_municipio': metadata.get('geradora_municipio', ''),
                'geradora_uf': metadata.get('geradora_uf', ''),
                'geradora_atividade': metadata.get('geradora_atividade', ''),
                'geradora_bacia_hidrografica': metadata.get('geradora_bacia_hidrografica', ''),
                'geradora_funcionarios': metadata.get('geradora_funcionarios', ''),
                # Dados da entidade de destinação
                'destino_entidade_nome': metadata.get('destino_entidade_nome', ''),
                'destino_entidade_cadastro_cetesb': metadata.get('destino_entidade_cadastro_cetesb', ''),
                'destino_entidade_logradouro': metadata.get('destino_entidade_logradouro', ''),
                'destino_entidade_numero': metadata.get('destino_entidade_numero', ''),
                'destino_entidade_complemento': metadata.get('destino_entidade_complemento', ''),
                'destino_entidade_bairro': metadata.get('destino_entidade_bairro', ''),
                'destino_entidade_cep': metadata.get('destino_entidade_cep', ''),
                'destino_entidade_municipio': metadata.get('destino_entidade_municipio', ''),
                'destino_entidade_uf': metadata.get('destino_entidade_uf', ''),
                'destino_entidade_atividade': metadata.get('destino_entidade_atividade', ''),
                'destino_entidade_bacia_hidrografica': metadata.get('destino_entidade_bacia_hidrografica', ''),
                'destino_entidade_licenca': metadata.get('destino_entidade_licenca', ''),
                'destino_entidade_data_licenca': metadata.get('destino_entidade_data_licenca', ''),
                # Dados do documento
                'numero_processo': metadata.get('numero_processo', ''),
                'numero_certificado': metadata.get('numero_certificado', ''),
                'versao_documento': metadata.get('versao_documento', ''),
                'data_documento': metadata.get('data_documento', ''),
            })
        else:
            # Adicionar campos vazios para manter consistência do schema
            result.update({
                'geradora_nome': '',
                'geradora_cadastro_cetesb': '',
                'geradora_logradouro': '',
                'geradora_numero': '',
                'geradora_complemento': '',
                'geradora_bairro': '',
                'geradora_cep': '',
                'geradora_municipio': '',
                'geradora_uf': '',
                'geradora_atividade': '',
                'geradora_bacia_hidrografica': '',
                'geradora_funcionarios': '',
                'destino_entidade_nome': '',
                'destino_entidade_cadastro_cetesb': '',
                'destino_entidade_logradouro': '',
                'destino_entidade_numero': '',
                'destino_entidade_complemento': '',
                'destino_entidade_bairro': '',
                'destino_entidade_cep': '',
                'destino_entidade_municipio': '',
                'destino_entidade_uf': '',
                'destino_entidade_atividade': '',
                'destino_entidade_bacia_hidrografica': '',
                'destino_entidade_licenca': '',
                'destino_entidade_data_licenca': '',
                'numero_processo': '',
                'numero_certificado': '',
                'versao_documento': '',
                'data_documento': '',
            })

        result['updated_at'] = datetime.now().isoformat()
        return result

    def validate_extraction(self, items: List[Dict]) -> List[Dict]:
        """Validar e limpar dados extraídos"""
        validated = []

        for item in items:
            # Validar se é novo schema (numero_residuo) ou antigo (codigo_residuo)
            numero_residuo = item.get('numero_residuo', '')
            codigo_residuo = item.get('codigo_residuo', '')

            # Para novo schema: validar formato [A-Z]\d{3} (D099, F001, etc.)
            if numero_residuo:
                if not re.match(r'^[A-Z]\d{3}$', numero_residuo):
                    logger.debug(f"Número de resíduo inválido ignorado: {numero_residuo}")
                    continue
            # Para schema antigo: validar formato XX.XX.XXX
            elif codigo_residuo:
                if not re.match(r'^\d{2}\.\d{2}\.\d{3}$', codigo_residuo):
                    logger.debug(f"Código de resíduo inválido ignorado: {codigo_residuo}")
                    continue
            else:
                logger.debug("Item sem código ou número de resíduo válido")
                continue

            # Limpar descrição
            descricao = item.get('descricao_residuo', '')
            if descricao:
                descricao = re.sub(r'\s+', ' ', descricao).strip()[:500]
                item['descricao_residuo'] = descricao

            # Normalizar unidade
            unidade = item.get('unidade', '').lower()
            unidade_map = {
                'kg': 'kg',
                'ton': 't',
                't': 't',
                'm3': 'm³',
                'm³': 'm³',
                'l': 'L',
                'litro': 'L',
                'litros': 'L',
                'unidade': 'un',
                'unidades': 'un',
                'un': 'un',
                'peça': 'un',
                'peças': 'un',
                'pç': 'un'
            }
            item['unidade'] = unidade_map.get(unidade, unidade)

            # Normalizar quantidade
            quantidade = item.get('quantidade', '')
            if quantidade:
                try:
                    # Converter para float e de volta para string para normalizar
                    quantidade_float = float(quantidade)
                    item['quantidade'] = str(quantidade_float)
                except:
                    pass

            validated.append(item)

        return validated

    def parse_all_pdfs(self, filter_type: str = None, force_reparse: bool = False) -> Dict[str, int]:
        """
        Processar todos os PDFs pendentes

        Args:
            filter_type: Filtrar por tipo de documento (opcional)

        Returns:
            Estatísticas do processamento
        """
        # Listar PDFs
        pdf_files = list(self.pdf_dir.glob("*.pdf"))

        # Filtrar já processados (a menos que force_reparse seja True)
        if force_reparse:
            pending = pdf_files
            logger.info("Modo força reprocessamento ativado - processando todos os PDFs")
        else:
            pending = [
                pdf for pdf in pdf_files
                if pdf.stem not in self.parsed_cache
            ]

        if not pending:
            logger.info("Nenhum PDF novo para processar")
            return self.stats

        # Filtrar por tipo se especificado
        if filter_type:
            # Carregar info dos documentos
            try:
                df_docs = pd.read_csv(CSV_CADRI_DOCS)
                # Filtrar PDFs por tipo
                docs_filtered = df_docs[df_docs['tipo_documento'] == filter_type]
                docs_numbers = set(docs_filtered['numero_documento'].astype(str))
                pending = [
                    pdf for pdf in pending
                    if pdf.stem in docs_numbers
                ]
                logger.info(f"Filtrado por tipo '{filter_type}': {len(pending)} PDFs")
            except Exception as e:
                logger.warning(f"Erro ao filtrar por tipo: {e}")

        logger.info(f"📄 {len(pending)} PDFs para processar")

        all_items = []

        for i, pdf_path in enumerate(pending, 1):
            logger.info(f"[{i}/{len(pending)}] Processando: {pdf_path.name}")

            try:
                items = self.parse_pdf(pdf_path)

                if items:
                    all_items.extend(items)
                    self.stats['items_extracted'] += len(items)
                else:
                    self.stats['no_items'] += 1

                self.stats['processed'] += 1

            except Exception as e:
                logger.error(f"Erro em {pdf_path.name}: {e}")
                self.stats['errors'] += 1

        # Salvar no CSV
        if all_items:
            self._save_items_to_csv(all_items)

        return self.stats

    def _save_items_to_csv(self, items: List[Dict]):
        """Salvar itens extraídos no CSV com novo schema expandido"""
        try:
            from store_csv import CSVStore, CSVSchemas

            # Garantir que o CSV existe com o schema correto
            CSVStore.ensure_csv(Path(CSV_CADRI_ITEMS), CSVSchemas.CADRI_ITEMS_COLS)

            df_new = pd.DataFrame(items)

            # Se o DataFrame não está vazio, usar upsert do CSVStore
            if not df_new.empty:
                # Usar número do documento + número do resíduo como chave única
                keys = ['numero_documento', 'numero_residuo']

                # Se numero_residuo não existe, usar campos antigos para compatibilidade
                if 'numero_residuo' not in df_new.columns and 'codigo_residuo' in df_new.columns:
                    keys = ['numero_documento', 'codigo_residuo']
                elif 'item_numero' in df_new.columns:
                    # Para novo schema expandido, usar documento + item_numero como chave
                    keys = ['numero_documento', 'item_numero']

                CSVStore.upsert(df_new, Path(CSV_CADRI_ITEMS), keys)
                logger.info(f"✅ Salvos {len(items)} itens em {CSV_CADRI_ITEMS}")
            else:
                logger.warning("Nenhum item para salvar")

        except Exception as e:
            logger.error(f"Erro ao salvar items: {e}")
            # Fallback para método antigo em caso de erro
            try:
                df_new = pd.DataFrame(items)
                df_new.to_csv(CSV_CADRI_ITEMS, index=False)
                logger.info(f"✅ Salvos {len(items)} itens em {CSV_CADRI_ITEMS} (fallback)")
            except Exception as e2:
                logger.error(f"Erro no fallback: {e2}")


def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(description='PDF Parser Standalone')
    parser.add_argument(
        '--type',
        type=str,
        help='Filtrar por tipo de documento (ex: CERT MOV RESIDUOS INT AMB)'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Processar apenas um arquivo específico'
    )
    parser.add_argument(
        '--document',
        type=str,
        help='Processar apenas um documento específico pelo número'
    )
    parser.add_argument(
        '--force-reparse',
        action='store_true',
        help='Forçar reprocessamento mesmo se já foi parseado'
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("PDF Parser Standalone - Extração Aprimorada de Dados de Resíduos")
    if args.type:
        logger.info(f"Tipo: {args.type}")
    if args.file:
        logger.info(f"Arquivo: {args.file}")
    if args.document:
        logger.info(f"Documento: {args.document}")
    if args.force_reparse:
        logger.info("Modo: Forçar reprocessamento")
    logger.info("=" * 70)

    parser_instance = PDFParserStandalone()

    if args.file:
        # Processar arquivo específico
        pdf_path = Path(args.file)
        if not pdf_path.exists():
            pdf_path = parser_instance.pdf_dir / args.file
            if not pdf_path.exists():
                logger.error(f"Arquivo não encontrado: {args.file}")
                return

        items = parser_instance.parse_pdf(pdf_path)
        if items:
            parser_instance._save_items_to_csv(items)

        stats = {
            'processed': 1,
            'items_extracted': len(items),
            'errors': 0 if items else 1,
            'no_items': 1 if not items else 0
        }
    elif args.document:
        # Processar documento específico pelo número
        pdf_path = parser_instance.pdf_dir / f"{args.document}.pdf"
        if not pdf_path.exists():
            logger.error(f"PDF não encontrado para documento: {args.document}")
            return

        items = parser_instance.parse_pdf(pdf_path)
        if items:
            parser_instance._save_items_to_csv(items)

        stats = {
            'processed': 1,
            'items_extracted': len(items),
            'errors': 0 if items else 1,
            'no_items': 1 if not items else 0
        }
    else:
        # Processar todos os PDFs
        stats = parser_instance.parse_all_pdfs(args.type, args.force_reparse)

    # Mostrar estatísticas
    logger.info("=" * 70)
    logger.info("📊 Estatísticas finais:")
    logger.info(f"   PDFs processados: {stats['processed']}")
    logger.info(f"   Itens extraídos: {stats['items_extracted']}")
    logger.info(f"   Sem itens: {stats['no_items']}")
    logger.info(f"   Erros: {stats['errors']}")
    logger.info("=" * 70)

    if stats['processed'] > 0:
        media_itens = stats['items_extracted'] / stats['processed']
        logger.info(f"   📈 Média de itens por PDF: {media_itens:.1f}")

    return stats


# Alias for backward compatibility with pipeline imports
PDFParser = PDFParserStandalone


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Processamento interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)