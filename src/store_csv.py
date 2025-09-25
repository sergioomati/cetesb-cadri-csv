import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import hashlib
from logging_conf import logger


class CSVStore:
    """CSV storage manager with idempotent upsert operations"""

    @staticmethod
    def ensure_csv(file_path: Path, columns: List[str]) -> None:
        """Ensure CSV exists with proper headers"""
        if not file_path.exists():
            df = pd.DataFrame(columns=columns)
            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"Created CSV: {file_path}")

    @staticmethod
    def load_csv(file_path: Path) -> pd.DataFrame:
        """Load CSV file, return empty DataFrame if not exists"""
        if file_path.exists():
            try:
                return pd.read_csv(file_path, encoding='utf-8', dtype=str)
            except pd.errors.EmptyDataError:
                return pd.DataFrame()
        return pd.DataFrame()

    @staticmethod
    def upsert(
        df_new: pd.DataFrame,
        target_csv: Path,
        keys: List[str],
        update_timestamp: bool = True
    ) -> int:
        """
        Upsert (insert or update) records in CSV with deduplication

        Args:
            df_new: DataFrame with new/updated records
            target_csv: Path to target CSV file
            keys: List of column names to use as unique keys
            update_timestamp: Whether to add/update 'updated_at' column

        Returns:
            Number of records upserted
        """
        if df_new.empty:
            return 0

        # Add timestamp if requested
        if update_timestamp:
            df_new['updated_at'] = datetime.now().isoformat()

        # Load existing data
        df_existing = CSVStore.load_csv(target_csv)

        if df_existing.empty:
            # First time, just save
            df_new.to_csv(target_csv, index=False, encoding='utf-8')
            logger.info(f"Saved {len(df_new)} new records to {target_csv.name}")
            return len(df_new)

        # Perform merge (upsert)
        df_merged = pd.concat([df_existing, df_new], ignore_index=True)

        # Remove duplicates, keeping last (most recent)
        df_merged = df_merged.drop_duplicates(subset=keys, keep='last')

        # Sort by keys for consistency
        df_merged = df_merged.sort_values(by=keys)

        # Save back
        df_merged.to_csv(target_csv, index=False, encoding='utf-8')

        new_count = len(df_merged) - len(df_existing)
        updated_count = len(df_new) - new_count

        logger.info(
            f"Upserted to {target_csv.name}: "
            f"{new_count} new, {updated_count} updated"
        )

        return len(df_new)

    @staticmethod
    def append_if_new(
        record: Dict[str, Any],
        target_csv: Path,
        keys: List[str]
    ) -> bool:
        """
        Append single record if it doesn't exist

        Returns:
            True if record was added, False if already exists
        """
        df_new = pd.DataFrame([record])
        df_existing = CSVStore.load_csv(target_csv)

        if not df_existing.empty:
            # Check if record already exists
            mask = True
            for key in keys:
                mask = mask & (df_existing[key] == record.get(key))

            if mask.any():
                return False

        # Append new record
        CSVStore.upsert(df_new, target_csv, keys)
        return True


class CSVSchemas:
    """Define CSV schemas and ensure they exist"""

    EMPRESAS_COLS = [
        'cnpj',
        'razao_social',
        'logradouro',
        'complemento',
        'bairro',
        'municipio',
        'uf',
        'cep',
        'numero_cadastro_cetesb',
        'descricao_atividade',
        'numero_s_numero',
        'url_detalhe',
        'data_source',  # 'results_page' or 'detail_page'
        'updated_at'
    ]

    CADRI_DOCS_COLS = [
        'numero_documento',
        'tipo_documento',
        'cnpj',
        'razao_social',
        'data_emissao',
        'url_detalhe',
        'url_pdf',
        'status_pdf',
        'pdf_hash',
        'sd_numero',  # SD Nº from results table
        'data_sd',    # Data da SD
        'numero_processo',  # Nº Processo
        'objeto_solicitacao',  # Objeto da Solicitação
        'situacao',   # Situação
        'data_desde', # Desde
        'data_source',  # 'results_page' or 'detail_page'
        'updated_at'
    ]

    CADRI_ITEMS_COLS = [
        'numero_documento',
        'item_numero',                # Número sequencial do item (01, 02, etc.)
        'numero_residuo',             # Código do resíduo (D099, F001, etc.)
        'descricao_residuo',          # Descrição completa do resíduo
        'origem_residuo',             # Origem do resíduo
        'classe_residuo',             # Classe (I, II, IIA, IIB, etc.)
        'estado_fisico',              # Estado físico (LÍQUIDO, SÓLIDO, GASOSO)
        'oii',                        # Campo OII
        'quantidade',                 # Quantidade numérica
        'unidade',                    # Unidade de medida (t/ano, kg, etc.)
        'composicao_aproximada',      # Composição aproximada
        'metodo_utilizado',           # Método utilizado
        'cor_cheiro_aspecto',         # Características físicas
        'acondicionamento_codigos',   # Códigos de acondicionamento (E01,E04,E05)
        'acondicionamento_descricoes', # Descrições do acondicionamento
        'destino_codigo',             # Código de destino (T34, etc.)
        'destino_descricao',          # Descrição do destino
        'pagina_origem',              # Página do PDF onde foi encontrado
        'raw_fragment',               # Fragmento de texto original
        'tipo_documento',             # Tipo do documento de origem
        'data_validade',              # Data de validade do documento
        # Dados da Entidade Geradora
        'geradora_nome',
        'geradora_cadastro_cetesb',
        'geradora_logradouro',
        'geradora_numero',
        'geradora_complemento',
        'geradora_bairro',
        'geradora_cep',
        'geradora_municipio',
        'geradora_uf',
        'geradora_atividade',
        'geradora_bacia_hidrografica',
        'geradora_funcionarios',
        # Dados da Entidade de Destinação
        'destino_entidade_nome',
        'destino_entidade_cadastro_cetesb',
        'destino_entidade_logradouro',
        'destino_entidade_numero',
        'destino_entidade_complemento',
        'destino_entidade_bairro',
        'destino_entidade_cep',
        'destino_entidade_municipio',
        'destino_entidade_uf',
        'destino_entidade_atividade',
        'destino_entidade_bacia_hidrografica',
        'destino_entidade_licenca',
        'destino_entidade_data_licenca',
        # Dados do Documento
        'numero_processo',
        'numero_certificado',
        'versao_documento',
        'data_documento',
        'updated_at'
    ]

    @classmethod
    def init_all(cls):
        """Initialize all CSV files with proper schemas"""
        from config import CSV_EMPRESAS, CSV_CADRI_DOCS, CSV_CADRI_ITEMS

        CSVStore.ensure_csv(CSV_EMPRESAS, cls.EMPRESAS_COLS)
        CSVStore.ensure_csv(CSV_CADRI_DOCS, cls.CADRI_DOCS_COLS)
        CSVStore.ensure_csv(CSV_CADRI_ITEMS, cls.CADRI_ITEMS_COLS)

        logger.info("All CSV schemas initialized")


def hash_file(file_path: Path) -> str:
    """Calculate SHA256 hash of a file"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def filter_by_date_cutoff(df: pd.DataFrame, years_cutoff: int = 7) -> pd.DataFrame:
    """
    Filter documents by date cutoff (only keep recent documents)

    Args:
        df: DataFrame with 'data_desde' column
        years_cutoff: Number of years back to keep (default: 7)

    Returns:
        Filtered DataFrame with only recent documents
    """
    if df.empty:
        return df

    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=365 * years_cutoff)
    cutoff_str = cutoff_date.strftime('%Y-%m-%d')

    logger.info(f"Aplicando filtro de data: documentos >= {cutoff_str} ({years_cutoff} anos)")

    # Convert data_desde to datetime
    df['data_desde_dt'] = pd.to_datetime(df['data_desde'], errors='coerce')

    # Count before filtering
    total_before = len(df)
    valid_dates = df['data_desde_dt'].notna().sum()
    invalid_dates = total_before - valid_dates

    # Filter recent documents (keep if data_desde >= cutoff_date OR data_desde is null/invalid)
    # Keep invalid dates to avoid losing documents that might be important
    recent_docs = df[
        (df['data_desde_dt'] >= cutoff_date) |
        (df['data_desde_dt'].isna())
    ]

    # Clean up temporary column
    recent_docs = recent_docs.drop(columns=['data_desde_dt'])

    # Log statistics
    filtered_out = total_before - len(recent_docs)
    logger.info(f"Filtro de data aplicado:")
    logger.info(f"  - Total de documentos: {total_before}")
    logger.info(f"  - Datas válidas: {valid_dates}")
    logger.info(f"  - Datas inválidas/vazias: {invalid_dates}")
    logger.info(f"  - Documentos filtrados (muito antigos): {filtered_out}")
    logger.info(f"  - Documentos restantes: {len(recent_docs)}")

    return recent_docs


def get_pending_pdfs(apply_date_filter: bool = True, years_cutoff: int = 7) -> List[Dict[str, str]]:
    """
    Get list of PDFs that need to be downloaded (only those with valid URLs)

    Args:
        apply_date_filter: Whether to apply date filtering (default: True)
        years_cutoff: Number of years back to keep (default: 7)

    Returns:
        List of documents ready for download
    """
    from config import CSV_CADRI_DOCS

    df = CSVStore.load_csv(CSV_CADRI_DOCS)

    if df.empty:
        logger.info("Nenhum documento encontrado no CSV")
        return []

    # Apply date filter first if requested
    if apply_date_filter:
        df = filter_by_date_cutoff(df, years_cutoff)

    # Filter where status_pdf is not 'downloaded' AND has valid URL
    pending = df[
        ((df['status_pdf'].isna()) | (df['status_pdf'] != 'downloaded')) &
        (df['url_pdf'].notna()) &
        (df['url_pdf'] != '') &
        (df['url_pdf'].str.contains('autenticidade.cetesb', na=False))
    ]

    logger.info(f"PDFs pendentes para download: {len(pending)}")

    return pending[['numero_documento', 'url_pdf']].to_dict('records')


def get_unparsed_pdfs() -> List[str]:
    """Get list of downloaded PDFs not yet parsed"""
    from config import CSV_CADRI_DOCS, CSV_CADRI_ITEMS

    df_docs = CSVStore.load_csv(CSV_CADRI_DOCS)
    df_items = CSVStore.load_csv(CSV_CADRI_ITEMS)

    # Get downloaded docs
    downloaded = df_docs[df_docs['status_pdf'] == 'downloaded']['numero_documento']

    # Get already parsed docs
    parsed = df_items['numero_documento'].unique() if not df_items.empty else []

    # Return difference
    unparsed = set(downloaded) - set(parsed)
    return list(unparsed)


def mark_pdf_status(numero_documento: str, status: str, pdf_hash: Optional[str] = None):
    """Update PDF download status in CSV"""
    from config import CSV_CADRI_DOCS

    df = CSVStore.load_csv(CSV_CADRI_DOCS)

    mask = df['numero_documento'] == numero_documento
    df.loc[mask, 'status_pdf'] = status

    if pdf_hash:
        df.loc[mask, 'pdf_hash'] = pdf_hash

    df.loc[mask, 'updated_at'] = datetime.now().isoformat()

    df.to_csv(CSV_CADRI_DOCS, index=False, encoding='utf-8')
    logger.debug(f"Updated PDF status for {numero_documento}: {status}")


def analyze_documents_by_date(years_cutoff: int = 7) -> Dict[str, any]:
    """
    Analyze document distribution by date and show filtering impact

    Args:
        years_cutoff: Number of years back to analyze (default: 7)

    Returns:
        Dictionary with analysis results
    """
    from config import CSV_CADRI_DOCS

    df = CSVStore.load_csv(CSV_CADRI_DOCS)

    if df.empty:
        return {"error": "No documents found"}

    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=365 * years_cutoff)
    cutoff_str = cutoff_date.strftime('%Y-%m-%d')

    # Convert data_desde to datetime
    df['data_desde_dt'] = pd.to_datetime(df['data_desde'], errors='coerce')

    # Basic statistics
    total_docs = len(df)
    valid_dates = df['data_desde_dt'].notna().sum()
    invalid_dates = total_docs - valid_dates

    # Documents by year
    df_with_dates = df[df['data_desde_dt'].notna()].copy()
    df_with_dates['year'] = df_with_dates['data_desde_dt'].dt.year
    year_counts = df_with_dates['year'].value_counts().sort_index()

    # Apply filtering
    recent_docs = df[
        (df['data_desde_dt'] >= cutoff_date) |
        (df['data_desde_dt'].isna())
    ]

    # Documents by type
    type_counts = df['tipo_documento'].value_counts()
    type_counts_recent = recent_docs['tipo_documento'].value_counts()

    # URLs available
    with_urls = df[
        df['url_pdf'].notna() &
        (df['url_pdf'] != '') &
        (df['url_pdf'] != 'pending')
    ]
    with_urls_recent = recent_docs[
        recent_docs['url_pdf'].notna() &
        (recent_docs['url_pdf'] != '') &
        (recent_docs['url_pdf'] != 'pending')
    ]

    # Prepare results
    analysis = {
        'cutoff_date': cutoff_str,
        'years_cutoff': years_cutoff,
        'total_documents': total_docs,
        'documents_with_valid_dates': int(valid_dates),
        'documents_with_invalid_dates': invalid_dates,
        'documents_after_cutoff': len(recent_docs),
        'documents_filtered_out': total_docs - len(recent_docs),
        'documents_with_urls': len(with_urls),
        'documents_with_urls_recent': len(with_urls_recent),
        'urls_filtered_out': len(with_urls) - len(with_urls_recent),
        'year_distribution': year_counts.to_dict(),
        'type_distribution_all': type_counts.to_dict(),
        'type_distribution_recent': type_counts_recent.to_dict()
    }

    # Log summary
    logger.info("=== ANÁLISE DE DOCUMENTOS POR DATA ===")
    logger.info(f"Data de corte: {cutoff_str} ({years_cutoff} anos)")
    logger.info(f"Total de documentos: {total_docs}")
    logger.info(f"Documentos com datas válidas: {valid_dates}")
    logger.info(f"Documentos filtrados (antigos): {analysis['documents_filtered_out']}")
    logger.info(f"Documentos restantes: {analysis['documents_after_cutoff']}")
    logger.info(f"URLs disponíveis (antes): {len(with_urls)}")
    logger.info(f"URLs disponíveis (depois): {len(with_urls_recent)}")
    logger.info(f"URLs economizadas: {analysis['urls_filtered_out']}")

    if year_counts.empty:
        logger.warning("Nenhum documento com data válida encontrado")
    else:
        logger.info("Distribuição por ano:")
        for year, count in year_counts.tail(10).items():  # Last 10 years
            marker = " (incluído)" if year >= cutoff_date.year else " (filtrado)"
            logger.info(f"  {year}: {count} documentos{marker}")

    return analysis