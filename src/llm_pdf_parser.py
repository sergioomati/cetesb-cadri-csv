#!/usr/bin/env python3
"""
LLM-based PDF Parser for CADRI documents using OpenRouter structured outputs
"""

import sys
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import hashlib
import re

# Add src to path for imports (matching pipeline pattern)
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import PyMuPDF for reliable PDF text extraction
try:
    import fitz  # pymupdf
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    fitz = None

from open_router_controller import OpenRouterController
from schemas import CADRIExtractionResult, ItemResiduoCADRI, flatten_item_to_dict
from store_csv import CSVStore, CSVSchemas
from logging_conf import logger
from config import (
    PDF_DIR, CSV_CADRI_ITEMS, LLM_DEFAULT_MODEL, LLM_MAX_TEXT_LENGTH,
    LLM_TEMPERATURE, LLM_BATCH_SIZE, LLM_PARSER_ENABLED
)


class LLMPDFParser:
    """Parser de PDFs CADRI usando LLM com structured outputs e Docling para extração de texto"""

    def __init__(self, model: str = None):
        # Verificar se LLM parser está habilitado
        if not LLM_PARSER_ENABLED:
            raise ImportError("LLM parser is disabled. Set LLM_PARSER_ENABLED=true in environment.")

        # Verificar se PyMuPDF está disponível
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF is not available. Install with: pip install PyMuPDF")

        self.pdf_dir = PDF_DIR
        self.model = model or LLM_DEFAULT_MODEL
        self.max_text_length = LLM_MAX_TEXT_LENGTH
        self.temperature = LLM_TEMPERATURE
        self.batch_size = LLM_BATCH_SIZE

        try:
            self.openrouter = OpenRouterController()
        except Exception as e:
            raise ImportError(f"Failed to initialize OpenRouterController: {e}")

        self.parsed_cache = self._load_parsed_cache()
        self.stats = {
            'processed': 0,
            'items_extracted': 0,
            'errors': 0,
            'llm_errors': 0,
            'fallback_used': 0,
            'cache_hits': 0,
            'pymupdf_used': 0
        }

        # System prompt para structured output
        self.system_prompt = self._create_system_prompt()

        logger.info(f"LLM Parser initialized with model: {self.model} and PyMuPDF text extraction")

    def _load_parsed_cache(self) -> set:
        """Carrega cache de documentos já processados"""
        try:
            # Tentar ler o CSV com tratamento de erros
            try:
                df = pd.read_csv(CSV_CADRI_ITEMS, dtype=str, on_bad_lines='skip')
            except Exception:
                # Se falhar, tentar com engine python e tratamento de quotes
                try:
                    df = pd.read_csv(CSV_CADRI_ITEMS, dtype=str, engine='python',
                                   on_bad_lines='skip', quoting=3)  # QUOTE_NONE
                except Exception as e:
                    logger.warning(f"Erro ao ler CSV cache, iniciando vazio: {e}")
                    return set()

            if not df.empty and 'numero_documento' in df.columns:
                # Usar hash do número do documento como cache key
                return set(df['numero_documento'].dropna().unique())
        except (FileNotFoundError, pd.errors.EmptyDataError):
            pass
        except Exception as e:
            logger.warning(f"Erro inesperado ao carregar cache: {e}")
        return set()

    def _create_system_prompt(self) -> str:
        """Cria o prompt de sistema para extração estruturada completa"""
        return """Você é um especialista em extração completa de dados de documentos CADRI da CETESB.

RETORNE APENAS JSON VÁLIDO, sem texto adicional antes ou depois.

ESTRUTURA OBRIGATÓRIA:
{
  "numero_documento": "número do documento",
  "total_items": número inteiro de itens encontrados,
  "items": [array de objetos com TODOS os campos especificados],
  "extraction_method": "llm",
  "processed_at": "2025-09-24T12:00:00"
}

CADA ITEM deve conter TODOS estes campos (use null se não encontrado):

=== IDENTIFICAÇÃO BÁSICA ===
- "numero_documento": "número do documento"
- "item_numero": "01", "02", etc.

=== IDENTIFICAÇÃO DO RESÍDUO ===
- "numero_residuo": "D099", "F001", "K001", "A021", etc.
- "descricao_residuo": "descrição completa do resíduo"
- "origem_residuo": "origem/fonte do resíduo"
- "classe_residuo": "I", "IIA", "IIB"
- "estado_fisico": "LIQUIDO", "SOLIDO", "GASOSO"
- "oii": "I", "O", "I/O" (Inorgânico/Orgânico)

=== CARACTERÍSTICAS TÉCNICAS ===
- "quantidade": "valor numérico" (ex: "50.0")
- "unidade": "t/ano", "kg", "L", "m³", etc.
- "composicao_aproximada": "composição química detalhada"
- "metodo_utilizado": "método de caracterização/análise"
- "cor_cheiro_aspecto": "características físicas observáveis"

=== LOGÍSTICA E ACONDICIONAMENTO ===
- "acondicionamento_codigos": "E01,E04,E05" (códigos de embalagem)
- "acondicionamento_descricoes": "descrições das embalagens"
- "destino_codigo": "T34", "R01", etc. (código do destino)
- "destino_descricao": "descrição do destino final"

=== METADADOS DE EXTRAÇÃO ===
- "pagina_origem": "número da página onde foi encontrado"
- "raw_fragment": "fragmento de texto original do PDF"

=== ENTIDADE GERADORA ===
- "entidade_geradora": {
    "nome": "razão social da empresa geradora",
    "cadastro_cetesb": "número de cadastro CETESB",
    "logradouro": "endereço - logradouro",
    "numero": "número do endereço",
    "complemento": "complemento do endereço",
    "bairro": "bairro",
    "cep": "CEP",
    "municipio": "município",
    "uf": "UF",
    "atividade": "descrição da atividade",
    "bacia_hidrografica": "bacia hidrográfica",
    "funcionarios": "número de funcionários"
  }

=== ENTIDADE DE DESTINAÇÃO ===
- "entidade_destinacao": {
    "nome": "razão social da empresa destinatária",
    "cadastro_cetesb": "número de cadastro CETESB",
    "logradouro": "endereço - logradouro",
    "numero": "número do endereço",
    "complemento": "complemento",
    "bairro": "bairro",
    "cep": "CEP",
    "municipio": "município",
    "uf": "UF",
    "atividade": "atividade do destinatário",
    "bacia_hidrografica": "bacia hidrográfica",
    "licenca": "número da licença ambiental",
    "data_licenca": "data da licença"
  }

=== DADOS DO DOCUMENTO ===
- "dados_documento": {
    "numero_processo": "número do processo CETESB",
    "numero_certificado": "número do certificado",
    "versao_documento": "versão do documento",
    "data_documento": "data de emissão",
    "data_validade": "data de validade",
    "tipo_documento": "CADRI" ou "Certificado"
  }

INSTRUÇÕES ESPECÍFICAS DE BUSCA:
- Códigos de resíduo: Procure por padrões como "D099", "F001", "K001", "A021" precedidos por números de item
- Acondicionamento: Busque códigos "E01", "E04", "E05" e suas descrições (Tambor, Tanque, Container)
- Entidades: Procure seções "ENTIDADE GERADORA" e "ENTIDADE DE DESTINAÇÃO"
- Classes: I (perigoso), IIA (não perigoso-não inerte), IIB (não perigoso-inerte)
- Quantidades: Valores numéricos seguidos de unidades (t/ano, kg/ano, L, m³)

FORMATO DE DATA/HORA:
- Use formato: "YYYY-MM-DDTHH:MM:SS"
- Exemplo: "2025-09-24T12:00:00"
- Nunca use :: (duplo)

IMPORTANTE:
- Extraia TODOS os campos listados
- Use null apenas para campos genuinamente ausentes
- Seja minucioso na busca de todas as informações
- Mantenha dados originais sem modificações desnecessárias"""


    def _extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extrai texto completo do PDF usando PyMuPDF (método comprovado)"""
        try:
            logger.debug(f"Extraindo texto com PyMuPDF: {pdf_path}")

            # Abrir PDF usando PyMuPDF (mesmo método do parser regex que funciona)
            doc = fitz.open(str(pdf_path))
            text_content = ""

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                page_text = page.get_text()
                if page_text.strip():
                    text_content += f"\n--- PÁGINA {page_num + 1} ---\n"
                    text_content += page_text + "\n"

            doc.close()
            self.stats['pymupdf_used'] += 1

            logger.debug(f"PyMuPDF extraiu {len(text_content)} caracteres de {pdf_path.name}")
            return text_content

        except Exception as e:
            logger.error(f"Erro ao extrair texto do PDF com PyMuPDF {pdf_path}: {e}")
            return ""

    def _create_extraction_prompt(self, pdf_text: str, numero_documento: str) -> str:
        """Cria o prompt para extração específica do documento"""

        # Limitar tamanho do texto se muito grande
        if len(pdf_text) > self.max_text_length:
            pdf_text = pdf_text[:self.max_text_length] + "\n... [TEXTO TRUNCADO] ..."

        return f"""Extraia dados estruturados do seguinte documento CADRI número {numero_documento}:

{pdf_text}

IMPORTANTE: Retorne um JSON válido seguindo o schema CADRIExtractionResult com todos os itens de resíduos encontrados."""

    def _clean_json_datetime_formats(self, json_text: str) -> str:
        """Limpa formatos de datetime comuns que causam erros de validação"""
        try:
            # Corrigir :: (dois pontos duplos) para : (um ponto) em timestamps
            json_text = re.sub(r'T(\d{1,2})::(\d{2})', r'T\1:\2:00', json_text)

            # Corrigir formatos incompletos como T12:00 para T12:00:00
            json_text = re.sub(r'T(\d{1,2}):(\d{2})"', r'T\1:\2:00"', json_text)

            # Adicionar segundos se faltando
            json_text = re.sub(r'T(\d{2}):(\d{2})([^:])"', r'T\1:\2:00\3"', json_text)

            logger.debug(f"JSON após limpeza de datetime: {json_text[:200]}...")
            return json_text

        except Exception as e:
            logger.warning(f"Erro na limpeza de datetime, usando texto original: {e}")
            return json_text

    def _parse_llm_response(self, response: str, numero_documento: str) -> Optional[CADRIExtractionResult]:
        """Parse da resposta LLM para objeto estruturado"""
        try:
            # Limpar resposta - remover possível texto antes/depois do JSON
            response = response.strip()

            # Tentar extrair JSON da resposta
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if not json_match:
                logger.error(f"Nenhum JSON encontrado na resposta LLM para {numero_documento}")
                logger.debug(f"Resposta recebida: {response[:500]}")
                return None

            json_text = json_match.group(0)

            # PRÉ-VALIDAÇÃO: Limpar problemas comuns de formato antes do JSON parsing
            json_text = self._clean_json_datetime_formats(json_text)

            data = json.loads(json_text)

            # Log para debug
            logger.debug(f"JSON parseado com sucesso: {list(data.keys())}")

            # Verificar se tem a estrutura mínima esperada
            if 'numero_documento' not in data:
                data['numero_documento'] = numero_documento

            if 'total_items' not in data and 'items' in data:
                data['total_items'] = len(data['items'])

            if 'items' not in data:
                logger.warning(f"Resposta sem campo 'items' para {numero_documento}")
                data['items'] = []
                data['total_items'] = 0

            # Garantir que cada item tem numero_documento
            for item in data.get('items', []):
                if 'numero_documento' not in item:
                    item['numero_documento'] = numero_documento

            # Validar usando Pydantic
            result = CADRIExtractionResult(**data)
            logger.info(f"LLM extraiu {result.total_items} itens do documento {numero_documento}")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Erro ao parsear JSON da resposta LLM para {numero_documento}: {e}")
            logger.debug(f"JSON inválido: {json_text[:500] if 'json_text' in locals() else response[:500]}")
            return None
        except Exception as e:
            logger.error(f"Erro ao validar dados extraídos para {numero_documento}: {e}")
            logger.debug(f"Dados problemáticos: {data if 'data' in locals() else 'não disponível'}")
            return None

    def _fallback_to_regex_parser(self, pdf_path: Path) -> List[Dict]:
        """Fallback para parser baseado em regex em caso de erro do LLM"""
        try:
            logger.warning(f"Usando fallback regex parser para {pdf_path.name}")

            # Import do parser original
            from pdf_parser_standalone import PDFParserStandalone

            with PDFParserStandalone() as parser:
                items = parser.parse_pdf(pdf_path)
                self.stats['fallback_used'] += 1
                return items

        except Exception as e:
            logger.error(f"Erro no fallback parser para {pdf_path}: {e}")
            # Fallback final: tentar usar Docling parser diretamente
            try:
                logger.warning(f"Tentando fallback final com Docling parser para {pdf_path.name}")
                sys.path.insert(0, str(Path(__file__).parent.parent))
                from docling_parser import DoclingPDFParser, DOCLING_AVAILABLE

                if DOCLING_AVAILABLE:
                    docling_parser = DoclingPDFParser()
                    fallback_parser = DoclingPDFParser()
                    return fallback_parser.parse_pdf(pdf_path)
            except Exception as e2:
                logger.error(f"Fallback final também falhou para {pdf_path}: {e2}")

            return []

    def parse_pdf(self, pdf_path: Path, force_reparse: bool = False) -> List[Dict]:
        """
        Parse um PDF CADRI usando LLM structured output

        Args:
            pdf_path: Caminho para o PDF
            force_reparse: Se True, reprocessa mesmo se já estiver no cache

        Returns:
            Lista de dicionários com dados extraídos
        """
        numero_documento = pdf_path.stem

        # Verificar cache
        if not force_reparse and numero_documento in self.parsed_cache:
            logger.info(f"PDF {numero_documento} já processado (cache)")
            self.stats['cache_hits'] += 1
            return []

        logger.info(f"Processando PDF com LLM: {numero_documento}")

        # Extrair texto do PDF
        pdf_text = self._extract_text_from_pdf(pdf_path)
        if not pdf_text.strip():
            logger.error(f"Nenhum texto extraído do PDF {numero_documento}")
            self.stats['errors'] += 1
            return []

        try:
            # Criar prompt de extração
            prompt = self._create_extraction_prompt(pdf_text, numero_documento)

            # Chamar LLM via OpenRouter
            response = self.openrouter.single_request(
                system_prompt=self.system_prompt,
                message=prompt,
                model=self.model,
                temperature=self.temperature
            )

            if not response:
                logger.error(f"Resposta vazia do LLM para {numero_documento}")
                self.stats['llm_errors'] += 1
                return self._fallback_to_regex_parser(pdf_path)

            # Parse da resposta estruturada
            extraction_result = self._parse_llm_response(response, numero_documento)

            if not extraction_result:
                logger.error(f"Falha ao parsear resposta LLM para {numero_documento}")
                self.stats['llm_errors'] += 1
                return self._fallback_to_regex_parser(pdf_path)

            # Converter para formato de dicionários compatível com CSV
            items_data = []
            for item in extraction_result.items:
                item_dict = flatten_item_to_dict(item)
                items_data.append(item_dict)

            self.stats['processed'] += 1
            self.stats['items_extracted'] += len(items_data)

            # Adicionar ao cache
            self.parsed_cache.add(numero_documento)

            logger.info(f"LLM extraiu {len(items_data)} itens do documento {numero_documento}")

            return items_data

        except Exception as e:
            logger.error(f"Erro geral no processamento LLM de {numero_documento}: {e}")
            self.stats['errors'] += 1
            return self._fallback_to_regex_parser(pdf_path)

    def parse_all_pdfs(self, filter_type: str = None, force_reparse: bool = False) -> Dict[str, int]:
        """
        Parse todos os PDFs usando LLM

        Args:
            filter_type: Filtro por tipo de documento (não usado no LLM parser)
            force_reparse: Se True, reprocessa todos os PDFs

        Returns:
            Dicionário com estatísticas de processamento
        """
        logger.info("=== Iniciando LLM PDF Parser ===")

        # Garantir que o CSV existe
        CSVStore.ensure_csv(Path(CSV_CADRI_ITEMS), CSVSchemas.CADRI_ITEMS_COLS)

        # Encontrar PDFs para processar
        pdf_files = list(self.pdf_dir.glob("*.pdf"))

        if not pdf_files:
            logger.warning(f"Nenhum PDF encontrado em {self.pdf_dir}")
            return self.stats

        logger.info(f"Encontrados {len(pdf_files)} PDFs para processar")

        all_items = []
        processed_count = 0

        for pdf_path in pdf_files:
            try:
                items = self.parse_pdf(pdf_path, force_reparse)

                if items:
                    all_items.extend(items)
                    processed_count += 1

                    # Salvar em batches para não perder dados
                    if len(all_items) >= self.batch_size:
                        self._save_items_batch(all_items)
                        all_items = []

            except Exception as e:
                logger.error(f"Erro ao processar {pdf_path}: {e}")
                self.stats['errors'] += 1
                continue

        # Salvar itens restantes
        if all_items:
            self._save_items_batch(all_items)

        # Atualizar estatísticas finais
        self.stats['total_pdfs'] = len(pdf_files)
        self.stats['processed_pdfs'] = processed_count

        logger.info(f"=== LLM Parser Concluído ===")
        logger.info(f"PDFs processados: {processed_count}/{len(pdf_files)}")
        logger.info(f"Itens extraídos: {self.stats['items_extracted']}")
        logger.info(f"PyMuPDF usado: {self.stats['pymupdf_used']}")
        logger.info(f"Erros LLM: {self.stats['llm_errors']}")
        logger.info(f"Fallbacks usados: {self.stats['fallback_used']}")
        logger.info(f"Cache hits: {self.stats['cache_hits']}")

        return self.stats

    def _save_items_batch(self, items: List[Dict]) -> None:
        """Salva um lote de itens no CSV"""
        if not items:
            return

        try:
            df_items = pd.DataFrame(items)

            # Garantir que todas as colunas esperadas existem
            for col in CSVSchemas.CADRI_ITEMS_COLS:
                if col not in df_items.columns:
                    df_items[col] = None

            # Reordenar colunas conforme schema
            df_items = df_items[CSVSchemas.CADRI_ITEMS_COLS]

            # Fazer upsert no CSV
            rows_upserted = CSVStore.upsert(
                df_items,
                Path(CSV_CADRI_ITEMS),
                keys=['numero_documento', 'item_numero', 'numero_residuo']
            )

            logger.info(f"Salvos {rows_upserted} itens no CSV")

        except Exception as e:
            logger.error(f"Erro ao salvar batch de itens: {e}")


def main():
    """Função principal para execução standalone"""
    import argparse

    parser = argparse.ArgumentParser(description='LLM PDF Parser para documentos CADRI')
    parser.add_argument('--document', help='Número específico do documento para processar')
    parser.add_argument('--force-reparse', action='store_true', help='Reprocessar PDFs já processados')
    parser.add_argument('--model', default='gpt-5-mini',
                       choices=['cost-optimized', 'flagship', 'free-gemini', 'grok-4-fast-free', 'deepseek', 'gpt-4o-mini', 'gpt-5-mini'],
                       help='Modelo LLM a usar')

    args = parser.parse_args()

    llm_parser = LLMPDFParser(model=args.model)

    if args.document:
        # Processar documento específico
        pdf_path = Path(PDF_DIR) / f"{args.document}.pdf"
        if pdf_path.exists():
            items = llm_parser.parse_pdf(pdf_path, args.force_reparse)
            print(f"Extraídos {len(items)} itens do documento {args.document}")
        else:
            print(f"PDF {args.document} não encontrado")
    else:
        # Processar todos os PDFs
        stats = llm_parser.parse_all_pdfs(force_reparse=args.force_reparse)
        print(f"Processamento concluído: {stats}")


if __name__ == "__main__":
    main()