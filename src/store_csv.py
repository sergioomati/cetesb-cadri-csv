import pandas as pd
from pathlib import Path
from datetime import datetime
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
        'residuo',
        'classe',
        'estado_fisico',
        'quantidade',
        'unidade',
        'pagina_origem',
        'raw_fragment',
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


def get_pending_pdfs() -> List[Dict[str, str]]:
    """Get list of PDFs that need to be downloaded"""
    from config import CSV_CADRI_DOCS

    df = CSVStore.load_csv(CSV_CADRI_DOCS)

    # Filter where status_pdf is not 'downloaded'
    pending = df[
        (df['status_pdf'].isna()) |
        (df['status_pdf'] != 'downloaded')
    ]

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