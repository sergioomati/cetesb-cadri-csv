#!/usr/bin/env python3
"""
Test script for date filtering functionality
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from store_csv import analyze_documents_by_date, get_pending_pdfs
from logging_conf import logger, setup_logging

def main():
    """Test date filtering functionality"""

    # Setup logging
    setup_logging(level='INFO')

    print("=" * 60)
    print("TESTE DE FILTRO DE DATA - DOCUMENTOS CETESB")
    print("=" * 60)

    # 1. Analyze current documents
    print("\n1. Analisando distribuição atual de documentos...")
    analysis = analyze_documents_by_date(years_cutoff=7)

    if "error" in analysis:
        print(f"Erro: {analysis['error']}")
        return

    print(f"\nRESUMO DA ANALISE:")
    print(f"   Data de corte: {analysis['cutoff_date']}")
    print(f"   Total de documentos: {analysis['total_documents']}")
    print(f"   Documentos filtrados: {analysis['documents_filtered_out']}")
    print(f"   Documentos restantes: {analysis['documents_after_cutoff']}")
    print(f"   URLs economizadas: {analysis['urls_filtered_out']}")

    # 2. Test get_pending_pdfs with and without filter
    print("\n2. Testando get_pending_pdfs()...")

    # Without filter
    print("\n   Sem filtro de data:")
    pending_all = get_pending_pdfs(apply_date_filter=False)
    print(f"   PDFs pendentes (sem filtro): {len(pending_all)}")

    # With filter
    print("\n   Com filtro de data (7 anos):")
    pending_filtered = get_pending_pdfs(apply_date_filter=True, years_cutoff=7)
    print(f"   PDFs pendentes (com filtro): {len(pending_filtered)}")

    economy = len(pending_all) - len(pending_filtered)
    if len(pending_all) > 0:
        economy_pct = (economy / len(pending_all)) * 100
        print(f"   Downloads economizados: {economy} ({economy_pct:.1f}%)")

    # 3. Show sample of filtered documents
    if pending_filtered:
        print(f"\n3. Amostra de documentos para download:")
        for i, doc in enumerate(pending_filtered[:3]):
            print(f"   {i+1}. {doc['numero_documento']}")

    print("\n" + "=" * 60)
    print("TESTE CONCLUIDO COM SUCESSO")
    print("=" * 60)

if __name__ == "__main__":
    main()