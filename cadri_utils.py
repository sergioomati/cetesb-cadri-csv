#!/usr/bin/env python3
"""
CADRI Utils - Utilitários para gerenciar o pipeline CADRI
"""

import asyncio
import sys
import argparse
from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from config import CSV_CADRI_DOCS, CSV_CADRI_ITENS, PDF_DIR, CSV_EMPRESAS
from logging_conf import logger


class CADRIUtils:
    """Utilitários para o pipeline CADRI"""

    def __init__(self):
        self.csv_docs = CSV_CADRI_DOCS
        self.csv_itens = CSV_CADRI_ITENS
        self.csv_empresas = CSV_EMPRESAS
        self.pdf_dir = PDF_DIR

    def list_document_types(self) -> List[str]:
        """Lista todos os tipos de documento disponíveis"""
        try:
            df = pd.read_csv(self.csv_docs)
            return sorted(df['tipo_documento'].unique().tolist())
        except Exception as e:
            logger.error(f"Erro ao listar tipos de documento: {e}")
            return []

    def count_by_status(self, doc_type: str = None) -> Dict[str, int]:
        """Conta documentos por status de PDF"""
        try:
            df = pd.read_csv(self.csv_docs)

            if doc_type:
                df = df[df['tipo_documento'] == doc_type]

            if 'status_pdf' not in df.columns:
                return {'total': len(df), 'no_status_column': len(df)}

            status_counts = df['status_pdf'].value_counts().to_dict()
            status_counts['total'] = len(df)

            return status_counts
        except Exception as e:
            logger.error(f"Erro ao contar por status: {e}")
            return {}

    def reset_pdf_status(self, doc_type: str = None, status_filter: str = None):
        """Reset status de PDF para reprocessamento"""
        try:
            df = pd.read_csv(self.csv_docs)

            # Filtros
            mask = pd.Series([True] * len(df))

            if doc_type:
                mask &= (df['tipo_documento'] == doc_type)

            if status_filter:
                mask &= (df['status_pdf'] == status_filter)

            if not mask.any():
                logger.info("Nenhum documento encontrado com os filtros especificados")
                return 0

            # Reset status
            df.loc[mask, 'status_pdf'] = 'pending'
            if 'pdf_hash' in df.columns:
                df.loc[mask, 'pdf_hash'] = ''
            if 'updated_at' in df.columns:
                from datetime import datetime
                df.loc[mask, 'updated_at'] = datetime.now().isoformat()

            # Salvar
            df.to_csv(self.csv_docs, index=False)

            count = mask.sum()
            logger.info(f"Status resetado para {count} documentos")
            return count

        except Exception as e:
            logger.error(f"Erro ao resetar status: {e}")
            return 0

    def cleanup_invalid_pdfs(self, min_size_kb: int = 10) -> int:
        """Remove PDFs muito pequenos (provavelmente inválidos)"""
        try:
            if not self.pdf_dir.exists():
                logger.info("Diretório de PDFs não existe")
                return 0

            removed = 0
            min_size_bytes = min_size_kb * 1024

            for pdf_file in self.pdf_dir.glob("*.pdf"):
                if pdf_file.stat().st_size < min_size_bytes:
                    logger.info(f"Removendo PDF pequeno: {pdf_file.name} ({pdf_file.stat().st_size} bytes)")
                    pdf_file.unlink()
                    removed += 1

                    # Atualizar status no CSV
                    numero_doc = pdf_file.stem
                    self._update_pdf_status(numero_doc, 'invalid_size')

            logger.info(f"Removidos {removed} PDFs inválidos")
            return removed

        except Exception as e:
            logger.error(f"Erro ao limpar PDFs inválidos: {e}")
            return 0

    def _update_pdf_status(self, numero_documento: str, status: str):
        """Atualiza status de um documento específico"""
        try:
            df = pd.read_csv(self.csv_docs)
            mask = df['numero_documento'] == numero_documento

            if mask.any():
                df.loc[mask, 'status_pdf'] = status
                if 'updated_at' in df.columns:
                    from datetime import datetime
                    df.loc[mask, 'updated_at'] = datetime.now().isoformat()
                df.to_csv(self.csv_docs, index=False)

        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")

    def export_failed_documents(self, output_file: str = "failed_documents.csv") -> int:
        """Exporta documentos que falharam no download"""
        try:
            df = pd.read_csv(self.csv_docs)

            # Filtrar falhas
            failed_statuses = ['not_found', 'timeout', 'error', 'invalid_download', 'invalid_size']
            failed_docs = df[df['status_pdf'].isin(failed_statuses)]

            if failed_docs.empty:
                logger.info("Nenhum documento com falha encontrado")
                return 0

            # Exportar
            failed_docs.to_csv(output_file, index=False)
            logger.info(f"Exportados {len(failed_docs)} documentos com falha para: {output_file}")

            return len(failed_docs)

        except Exception as e:
            logger.error(f"Erro ao exportar documentos com falha: {e}")
            return 0

    def validate_data_consistency(self) -> Dict[str, List[str]]:
        """Valida consistência dos dados"""
        issues = {
            'missing_files': [],
            'orphan_pdfs': [],
            'invalid_statuses': [],
            'missing_data': []
        }

        try:
            # Carregar dados
            df_docs = pd.read_csv(self.csv_docs) if self.csv_docs.exists() else pd.DataFrame()

            if df_docs.empty:
                issues['missing_data'].append("CSV de documentos vazio ou inexistente")
                return issues

            # Verificar PDFs órfãos
            if self.pdf_dir.exists():
                pdf_files = {f.stem for f in self.pdf_dir.glob("*.pdf")}
                doc_numbers = set(df_docs['numero_documento'].dropna().astype(str))

                orphan_pdfs = pdf_files - doc_numbers
                issues['orphan_pdfs'].extend(orphan_pdfs)

                # Verificar arquivos faltantes
                downloaded_docs = df_docs[df_docs['status_pdf'] == 'downloaded']['numero_documento']
                for doc_num in downloaded_docs:
                    if str(doc_num) not in pdf_files:
                        issues['missing_files'].append(str(doc_num))

            # Verificar status inválidos
            if 'status_pdf' in df_docs.columns:
                valid_statuses = ['pending', 'downloaded', 'not_found', 'timeout', 'error', 'invalid_download', 'invalid_size']
                invalid_status_docs = df_docs[
                    ~df_docs['status_pdf'].isin(valid_statuses + [None, ''])
                ]['numero_documento'].tolist()
                issues['invalid_statuses'].extend(invalid_status_docs)

        except Exception as e:
            logger.error(f"Erro na validação: {e}")
            issues['missing_data'].append(f"Erro: {e}")

        return issues

    async def run_quick_download(self, doc_type: str = "CERT MOV RESIDUOS INT AMB", limit: int = 10):
        """Executa download rápido de alguns documentos para teste"""
        logger.info(f"Executando download rápido de {limit} documentos do tipo: {doc_type}")

        try:
            # Import downloaders
            sys.path.append(str(Path(__file__).parent))
            from cert_mov_direct_downloader import CertMovDirectDownloader

            async with CertMovDirectDownloader() as downloader:
                # Modificar temporariamente para modo teste
                original_method = downloader.download_cert_mov_documents

                async def limited_download():
                    # Ler documentos
                    df = pd.read_csv(self.csv_docs)
                    cert_mov = df[df['tipo_documento'] == doc_type]

                    # Filtrar com URLs válidas e sem status de sucesso
                    need_download = cert_mov[
                        (cert_mov['url_pdf'].notna()) &
                        (cert_mov['url_pdf'] != '') &
                        (cert_mov['url_pdf'] != 'pending') &
                        (cert_mov['status_pdf'] != 'downloaded')
                    ].head(limit)

                    if need_download.empty:
                        logger.info("Nenhum documento encontrado para download")
                        return {"success": 0, "failed": 0}

                    documents = need_download.to_dict('records')
                    logger.info(f"Tentando download de {len(documents)} documentos...")

                    stats = {"success": 0, "failed": 0, "skipped": 0, "already_exists": 0}

                    for i, doc in enumerate(documents, 1):
                        logger.info(f"[{i}/{len(documents)}] Processando: {doc['numero_documento']}")
                        success = await downloader.download_pdf(doc)

                        if success:
                            stats["success"] += 1
                        else:
                            stats["failed"] += 1

                        # Rate limiting
                        if i < len(documents):
                            await asyncio.sleep(1.0)

                    return stats

                stats = await limited_download()

                logger.info("Resultado do download rápido:")
                logger.info(f"  ✅ Sucesso: {stats['success']}")
                logger.info(f"  ❌ Falhas: {stats['failed']}")

                return stats

        except ImportError as e:
            logger.error(f"Não foi possível importar o downloader: {e}")
            return {"success": 0, "failed": 0}
        except Exception as e:
            logger.error(f"Erro no download rápido: {e}")
            return {"success": 0, "failed": 0}


async def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Utilitários para o pipeline CADRI')

    subparsers = parser.add_subparsers(dest='command', help='Comandos disponíveis')

    # List types
    list_parser = subparsers.add_parser('list-types', help='Listar tipos de documento')

    # Count status
    count_parser = subparsers.add_parser('count', help='Contar documentos por status')
    count_parser.add_argument('--type', help='Filtrar por tipo de documento')

    # Reset status
    reset_parser = subparsers.add_parser('reset', help='Resetar status de PDF')
    reset_parser.add_argument('--type', help='Filtrar por tipo de documento')
    reset_parser.add_argument('--status', help='Filtrar por status específico')

    # Cleanup
    cleanup_parser = subparsers.add_parser('cleanup', help='Limpar PDFs inválidos')
    cleanup_parser.add_argument('--min-size', type=int, default=10, help='Tamanho mínimo em KB')

    # Export failed
    export_parser = subparsers.add_parser('export-failed', help='Exportar documentos com falha')
    export_parser.add_argument('--output', default='failed_documents.csv', help='Arquivo de saída')

    # Validate
    validate_parser = subparsers.add_parser('validate', help='Validar consistência dos dados')

    # Quick download
    download_parser = subparsers.add_parser('quick-download', help='Download rápido para teste')
    download_parser.add_argument('--type', default='CERT MOV RESIDUOS INT AMB', help='Tipo de documento')
    download_parser.add_argument('--limit', type=int, default=10, help='Limite de documentos')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    utils = CADRIUtils()

    if args.command == 'list-types':
        types = utils.list_document_types()
        print("\n📋 Tipos de documento disponíveis:")
        for doc_type in types:
            print(f"   - {doc_type}")

    elif args.command == 'count':
        counts = utils.count_by_status(args.type)
        doc_filter = f" ({args.type})" if args.type else ""
        print(f"\n📊 Contagem por status{doc_filter}:")
        for status, count in counts.items():
            print(f"   - {status}: {count:,}")

    elif args.command == 'reset':
        count = utils.reset_pdf_status(args.type, args.status)
        print(f"\n🔄 Status resetado para {count} documentos")

    elif args.command == 'cleanup':
        count = utils.cleanup_invalid_pdfs(args.min_size)
        print(f"\n🧹 Removidos {count} PDFs inválidos")

    elif args.command == 'export-failed':
        count = utils.export_failed_documents(args.output)
        print(f"\n📤 Exportados {count} documentos com falha")

    elif args.command == 'validate':
        issues = utils.validate_data_consistency()
        print("\n🔍 Validação de consistência:")

        for issue_type, items in issues.items():
            if items:
                print(f"\n   ⚠️  {issue_type}: {len(items)} problemas")
                for item in items[:5]:  # Show first 5
                    print(f"      - {item}")
                if len(items) > 5:
                    print(f"      ... e mais {len(items) - 5}")
            else:
                print(f"   ✅ {issue_type}: OK")

    elif args.command == 'quick-download':
        stats = await utils.run_quick_download(args.type, args.limit)
        print(f"\n⬇️  Download rápido concluído:")
        print(f"   ✅ Sucesso: {stats['success']}")
        print(f"   ❌ Falhas: {stats['failed']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Operação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro: {e}")
        sys.exit(1)