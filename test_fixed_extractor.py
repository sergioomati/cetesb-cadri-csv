#!/usr/bin/env python3
"""
Teste do Extractor Corrigido

Testa as correções implementadas no ResultsPageExtractor
usando os arquivos HTML reais da sessão de teste.
"""

import sys
from pathlib import Path
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from results_extractor import ResultsPageExtractor, extract_company_and_documents
from logging_conf import setup_logging, logger


def test_html_file(html_file: Path):
    """Test extraction from a specific HTML file"""
    print(f"\n{'='*60}")
    print(f"Testing: {html_file.name}")
    print('='*60)

    # Read HTML content
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Extract URL from comment if present
    import re
    url_match = re.search(r'<!-- Description: Page content for (.*?) -->', html_content)
    url = url_match.group(1) if url_match else "Unknown"

    print(f"URL: {url[:100]}...")

    # Test extraction
    try:
        extractor = ResultsPageExtractor(html_content, url)

        # Check page type detection
        page_type = extractor._detect_page_type()
        print(f"\nPage Type: {page_type}")

        # Extract data
        company, documents = extract_company_and_documents(html_content, url)

        # Display company data
        print("\n--- Company Data ---")
        fields_extracted = 0
        for key, value in company.items():
            if value and value != "results_page":
                print(f"  {key}: {value}")
                fields_extracted += 1

        if fields_extracted == 0:
            print("  [No company data extracted]")

        # Display documents
        print(f"\n--- Documents ({len(documents)}) ---")
        for i, doc in enumerate(documents, 1):
            print(f"\n  Document {i}:")
            print(f"    Número: {doc.get('numero_documento', 'N/A')}")
            print(f"    Tipo: {doc.get('tipo_documento', 'N/A')}")
            print(f"    SD Nº: {doc.get('sd_numero', 'N/A')}")
            print(f"    Data SD: {doc.get('data_sd', 'N/A')}")
            print(f"    Situação: {doc.get('situacao', 'N/A')}")

        # Summary
        print("\n--- Extraction Summary ---")
        print(f"  Fields extracted: {fields_extracted}")
        print(f"  Documents found: {len(documents)}")
        print(f"  Has CNPJ: {bool(company.get('cnpj'))}")
        print(f"  Has Razão Social: {bool(company.get('razao_social'))}")
        print(f"  Has Address: {bool(company.get('logradouro'))}")

        return {
            'file': html_file.name,
            'page_type': page_type,
            'fields_extracted': fields_extracted,
            'documents_found': len(documents),
            'success': fields_extracted > 0 or len(documents) > 0
        }

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            'file': html_file.name,
            'error': str(e),
            'success': False
        }


def main():
    """Main test function"""
    setup_logging(level='INFO')

    # Test session directory
    session_dir = Path("data/test_results/session_20250921_123833")

    if not session_dir.exists():
        print(f"Session directory not found: {session_dir}")
        return

    # Find all HTML files
    html_files = list(session_dir.glob("*_page.html"))

    if not html_files:
        print(f"No HTML files found in {session_dir}")
        return

    print(f"Found {len(html_files)} HTML files to test")

    results = []

    # Test each file
    for html_file in html_files:
        result = test_html_file(html_file)
        results.append(result)

    # Final summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print('='*60)

    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]

    print(f"\nSuccessful: {len(successful)}/{len(results)}")
    for r in successful:
        print(f"  OK {r['file']}: {r.get('fields_extracted', 0)} fields, {r.get('documents_found', 0)} docs")

    if failed:
        print(f"\nFailed: {len(failed)}/{len(results)}")
        for r in failed:
            print(f"  FAIL {r['file']}: {r.get('error', 'No data extracted')}")

    # Save results
    results_file = session_dir / "test_fixed_results.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to: {results_file}")


if __name__ == "__main__":
    main()