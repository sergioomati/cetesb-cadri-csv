#!/usr/bin/env python3
"""
Monitor Progress - Script para monitorar progresso dos downloads e parsing de PDFs
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import argparse

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from config import CSV_CADRI_DOCS, CSV_CADRI_ITENS, PDF_DIR
from logging_conf import logger


class ProgressMonitor:
    """Monitor do progresso do pipeline CADRI"""

    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.csv_docs = CSV_CADRI_DOCS
        self.csv_itens = CSV_CADRI_ITENS

    def get_document_stats(self) -> Dict:
        """Estatísticas dos documentos"""
        try:
            df = pd.read_csv(self.csv_docs)

            stats = {
                'total_documents': len(df),
                'by_type': df['tipo_documento'].value_counts().to_dict(),
                'by_status': df['status_pdf'].value_counts().to_dict() if 'status_pdf' in df.columns else {},
                'with_urls': len(df[df['url_pdf'].notna() & (df['url_pdf'] != '') & (df['url_pdf'] != 'pending')]),
                'without_urls': len(df[df['url_pdf'].isna() | (df['url_pdf'] == '') | (df['url_pdf'] == 'pending')])
            }

            return stats
        except Exception as e:
            logger.error(f"Erro ao ler estatísticas de documentos: {e}")
            return {}

    def get_pdf_stats(self) -> Dict:
        """Estatísticas dos PDFs baixados"""
        try:
            # Contar PDFs físicos
            pdf_files = list(self.pdf_dir.glob("*.pdf")) if self.pdf_dir.exists() else []

            # Estatísticas de tamanho
            total_size = sum(f.stat().st_size for f in pdf_files)
            sizes = [f.stat().st_size for f in pdf_files]

            stats = {
                'total_pdfs': len(pdf_files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'avg_size_kb': round((total_size / len(pdf_files)) / 1024, 2) if pdf_files else 0,
                'min_size_kb': round(min(sizes) / 1024, 2) if sizes else 0,
                'max_size_mb': round(max(sizes) / (1024 * 1024), 2) if sizes else 0
            }

            return stats
        except Exception as e:
            logger.error(f"Erro ao ler estatísticas de PDFs: {e}")
            return {}

    def get_parsing_stats(self) -> Dict:
        """Estatísticas do parsing de PDFs"""
        try:
            if not self.csv_itens.exists():
                return {'total_parsed': 0, 'total_items': 0}

            df = pd.read_csv(self.csv_itens)

            stats = {
                'total_parsed': len(df['numero_documento'].unique()) if not df.empty else 0,
                'total_items': len(df) if not df.empty else 0,
                'avg_items_per_doc': round(len(df) / len(df['numero_documento'].unique()), 2) if not df.empty else 0,
                'by_waste_class': df['classe_residuo'].value_counts().to_dict() if 'classe_residuo' in df.columns and not df.empty else {}
            }

            return stats
        except Exception as e:
            logger.error(f"Erro ao ler estatísticas de parsing: {e}")
            return {}

    def get_progress_by_doc_type(self, doc_type: str = "CERT MOV RESIDUOS INT AMB") -> Dict:
        """Progresso específico por tipo de documento"""
        try:
            df = pd.read_csv(self.csv_docs)

            # Filtrar por tipo
            type_docs = df[df['tipo_documento'] == doc_type]

            if type_docs.empty:
                return {'doc_type': doc_type, 'total': 0}

            # URLs válidas
            with_urls = type_docs[
                type_docs['url_pdf'].notna() &
                (type_docs['url_pdf'] != '') &
                (type_docs['url_pdf'] != 'pending')
            ]

            # Status de download
            status_counts = type_docs['status_pdf'].value_counts().to_dict() if 'status_pdf' in type_docs.columns else {}

            # PDFs baixados (checkar arquivos físicos)
            pdf_files = []
            if self.pdf_dir.exists():
                doc_numbers = type_docs['numero_documento'].dropna().tolist()
                pdf_files = [f for f in self.pdf_dir.glob("*.pdf")
                           if f.stem in doc_numbers]

            progress = {
                'doc_type': doc_type,
                'total': len(type_docs),
                'with_urls': len(with_urls),
                'without_urls': len(type_docs) - len(with_urls),
                'status_breakdown': status_counts,
                'pdfs_downloaded': len(pdf_files),
                'download_rate': round((len(pdf_files) / len(with_urls)) * 100, 1) if with_urls.any() else 0
            }

            return progress

        except Exception as e:
            logger.error(f"Erro ao calcular progresso para {doc_type}: {e}")
            return {}

    def check_missing_pdfs(self, doc_type: str = None) -> List[str]:
        """Lista documentos que deveriam ter PDFs mas não têm"""
        try:
            df = pd.read_csv(self.csv_docs)

            # Filtrar por tipo se especificado
            if doc_type:
                df = df[df['tipo_documento'] == doc_type]

            # Documentos com URLs mas sem status de sucesso
            should_have_pdfs = df[
                (df['url_pdf'].notna()) &
                (df['url_pdf'] != '') &
                (df['url_pdf'] != 'pending') &
                (df['status_pdf'] != 'downloaded')
            ]

            missing = []
            for _, row in should_have_pdfs.iterrows():
                numero = row['numero_documento']
                pdf_path = self.pdf_dir / f"{numero}.pdf"

                if not pdf_path.exists():
                    missing.append({
                        'numero_documento': numero,
                        'tipo_documento': row['tipo_documento'],
                        'status_pdf': row.get('status_pdf', 'pending'),
                        'url_pdf': row['url_pdf']
                    })

            return missing

        except Exception as e:
            logger.error(f"Erro ao verificar PDFs faltantes: {e}")
            return []

    def generate_report(self, doc_type: str = None) -> str:
        """Gera relatório completo"""
        report = []
        report.append("=" * 70)
        report.append("CETESB CADRI - Progress Monitor Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 70)

        # Estatísticas gerais
        doc_stats = self.get_document_stats()
        if doc_stats:
            report.append("\n📊 DOCUMENT STATISTICS")
            report.append(f"   Total documents: {doc_stats.get('total_documents', 0):,}")
            report.append(f"   With URLs: {doc_stats.get('with_urls', 0):,}")
            report.append(f"   Without URLs: {doc_stats.get('without_urls', 0):,}")

            report.append("\n   By document type:")
            for doc_type_name, count in doc_stats.get('by_type', {}).items():
                report.append(f"     - {doc_type_name}: {count:,}")

            if doc_stats.get('by_status'):
                report.append("\n   By PDF status:")
                for status, count in doc_stats.get('by_status', {}).items():
                    report.append(f"     - {status}: {count:,}")

        # Estatísticas de PDFs
        pdf_stats = self.get_pdf_stats()
        if pdf_stats:
            report.append("\n📁 PDF DOWNLOAD STATISTICS")
            report.append(f"   Total PDFs downloaded: {pdf_stats.get('total_pdfs', 0):,}")
            report.append(f"   Total size: {pdf_stats.get('total_size_mb', 0):.1f} MB")
            report.append(f"   Average size: {pdf_stats.get('avg_size_kb', 0):.1f} KB")
            report.append(f"   Size range: {pdf_stats.get('min_size_kb', 0):.1f} KB - {pdf_stats.get('max_size_mb', 0):.1f} MB")

        # Estatísticas de parsing
        parse_stats = self.get_parsing_stats()
        if parse_stats:
            report.append("\n🔍 PDF PARSING STATISTICS")
            report.append(f"   Documents parsed: {parse_stats.get('total_parsed', 0):,}")
            report.append(f"   Waste items extracted: {parse_stats.get('total_items', 0):,}")
            report.append(f"   Average items per document: {parse_stats.get('avg_items_per_doc', 0):.1f}")

            if parse_stats.get('by_waste_class'):
                report.append("\n   By waste class:")
                for waste_class, count in parse_stats.get('by_waste_class', {}).items():
                    report.append(f"     - {waste_class}: {count:,}")

        # Progresso específico para CERT MOV
        cert_progress = self.get_progress_by_doc_type("CERT MOV RESIDUOS INT AMB")
        if cert_progress:
            report.append("\n🎯 CERT MOV RESIDUOS INT AMB PROGRESS")
            report.append(f"   Total documents: {cert_progress.get('total', 0):,}")
            report.append(f"   With URLs: {cert_progress.get('with_urls', 0):,}")
            report.append(f"   PDFs downloaded: {cert_progress.get('pdfs_downloaded', 0):,}")
            report.append(f"   Download rate: {cert_progress.get('download_rate', 0):.1f}%")

        # PDFs faltantes
        missing = self.check_missing_pdfs(doc_type)
        if missing:
            report.append(f"\n❌ MISSING PDFS ({len(missing)} documents)")
            for doc in missing[:10]:  # Show first 10
                report.append(f"   - {doc['numero_documento']} ({doc['status_pdf']})")
            if len(missing) > 10:
                report.append(f"   ... and {len(missing) - 10} more")

        report.append("\n" + "=" * 70)

        return "\n".join(report)

    def save_report(self, filename: str = None, doc_type: str = None):
        """Salva relatório em arquivo"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"progress_report_{timestamp}.txt"

        report = self.generate_report(doc_type)

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"Relatório salvo em: {filename}")
        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {e}")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Monitor de progresso do pipeline CADRI')

    parser.add_argument(
        '--doc-type',
        type=str,
        help='Filtrar por tipo de documento (ex: CERT MOV RESIDUOS INT AMB)'
    )

    parser.add_argument(
        '--save',
        type=str,
        help='Salvar relatório em arquivo'
    )

    parser.add_argument(
        '--missing-only',
        action='store_true',
        help='Mostrar apenas PDFs faltantes'
    )

    args = parser.parse_args()

    monitor = ProgressMonitor()

    if args.missing_only:
        # Mostrar apenas PDFs faltantes
        missing = monitor.check_missing_pdfs(args.doc_type)
        print(f"\n❌ MISSING PDFS: {len(missing)} documents")
        for doc in missing:
            print(f"   - {doc['numero_documento']} ({doc['tipo_documento']}) - Status: {doc['status_pdf']}")
    else:
        # Relatório completo
        report = monitor.generate_report(args.doc_type)
        print(report)

        if args.save:
            monitor.save_report(args.save, args.doc_type)


if __name__ == "__main__":
    main()