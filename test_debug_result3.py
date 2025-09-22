#!/usr/bin/env python3
"""
Debug específico para result_3_page.html
"""

import sys
from pathlib import Path
import re

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from results_extractor import ResultsPageExtractor
from bs4 import BeautifulSoup


# Read HTML file
html_file = Path("data/test_results/session_20250921_123833/result_3_page.html")

with open(html_file, 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

# Find the section with company data
print("=== SEARCHING FOR COMPANY DATA ===\n")

# Look for "Dados do Cadastramento"
dados_elements = soup.find_all(string=re.compile(r'Dados.*Cadastramento', re.IGNORECASE))
print(f"Found {len(dados_elements)} elements with 'Dados do Cadastramento'")

if dados_elements:
    for elem in dados_elements:
        parent = elem.parent
        while parent and parent.name not in ['table', 'div']:
            parent = parent.parent

        if parent:
            print(f"\nFound parent container: {parent.name}")

            # Get text content
            text = parent.get_text()
            normalized = ' '.join(text.split())

            print(f"\nNormalized text (first 500 chars):")
            print(normalized[:500])

            # Look for specific patterns
            print("\n=== CHECKING PATTERNS ===")

            # Check Razão Social
            patterns = [
                (r'Razão\s+Social\s*[-–]\s*([^•]+?)(?=Nº|Logradouro|Complemento|$)', 'Razão Social'),
                (r'Logradouro\s*[-–]\s*([^•]+?)(?=Nº|Complemento|CEP|$)', 'Logradouro'),
                (r'Município\s*[-–]\s*([^•]+?)(?=CNPJ|Nº do Cadastro|$)', 'Município'),
                (r'CNPJ\s*[-–]\s*(\d{2}[.\d/\-]*\d{2})', 'CNPJ'),
            ]

            for pattern, name in patterns:
                match = re.search(pattern, normalized, re.IGNORECASE)
                if match:
                    print(f"  {name}: FOUND - '{match.group(1)}'")
                else:
                    print(f"  {name}: NOT FOUND")
                    # Try simpler pattern
                    simple = re.search(f'{name.split()[0]}.*?[-–]\\s*([^\\n]+)', normalized, re.IGNORECASE)
                    if simple:
                        print(f"    Simple match: '{simple.group(0)[:100]}'")

# Also look for specific table with company data
print("\n=== LOOKING FOR COMPANY TABLE ===")

tables = soup.find_all('table')
for i, table in enumerate(tables):
    table_text = table.get_text()

    # Check if this table has company data
    if 'ALAN DA FRAGA' in table_text:
        print(f"\nTable {i} contains company data!")

        # Find the specific row
        rows = table.find_all('tr')
        for row in rows:
            if 'ALAN DA FRAGA' in row.get_text():
                print(f"\nRow HTML:")
                print(str(row)[:300])

                # Extract from this row
                row_text = row.get_text()
                print(f"\nRow text:")
                print(row_text)

                # Try to extract
                normalized = ' '.join(row_text.split())
                print(f"\nNormalized:")
                print(normalized)

                # Pattern matching
                match = re.search(r'Razão\s+Social.*?[-–]\s*([^•]+?)(?=$)', normalized, re.IGNORECASE)
                if match:
                    print(f"\nExtracted Razão Social: '{match.group(1)}'")

                break