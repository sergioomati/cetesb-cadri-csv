#!/usr/bin/env python3
"""
Debug específico para padrões da SUZAQUIM
"""

import re
import fitz

# Carregar PDF e extrair texto exato
doc = fitz.open('data/pdfs/32007998.pdf')
text = doc[0].get_text()
doc.close()

# Padrões de teste
patterns = {
    'suzaquim_especifico': re.compile(
        r'(SUZAQUIM\s+INDÚSTRIAS\s+QUÍMICAS\s+LTDA)\s+(\d+-\d+-\d+)\s+'
        r'(RUA\s+[A-Z\s]+)\s+(\d+)\s+'
        r'([A-Z\s]+)\s+([\d-]+)\s+([A-Z]+)\s+'
        r'(Fabricação[^\n]+)\s+'
        r'(\d+\s*-\s*[A-Z\s]+)\s+'
        r'(\d+)\s+(\d{2}/\d{2}/\d{4})',
        re.IGNORECASE | re.DOTALL
    ),

    'generico_pos_nestle': re.compile(
        r'NESTLE\s+BRASIL\s+LTDA\s+\d+-\d+-\d+.*?'
        r'([A-Z\s]+(?:LTDA|S\.?A\.?))\s+(\d+-\d+-\d+)\s+'
        r'([A-Z\s]+)\s+(\d+)\s+'
        r'([A-Z\s]+)\s+([\d-]+)\s+([A-Z]+)\s+'
        r'([^\n]+)\s+'
        r'(\d+\s*-\s*[A-Z\s]+)\s+'
        r'(\d+)\s+(\d{2}/\d{2}/\d{4})',
        re.IGNORECASE | re.DOTALL
    ),

    'suzaquim_simples': re.compile(
        r'SUZAQUIM.*?(\d+-\d+-\d+)',
        re.IGNORECASE
    ),

    'pos_nestle_simples': re.compile(
        r'NESTLE.*?LTDA.*?(\d+-\d+-\d+).*?(SUZAQUIM.*?LTDA).*?(\d+-\d+-\d+)',
        re.IGNORECASE | re.DOTALL
    ),

    'suzaquim_completo_por_posicao': re.compile(
        r'SUZAQUIM\s+INDÚSTRIAS\s+QUÍMICAS\s+LTDA.*?'
        r'(\d+-\d+-\d+).*?'
        r'(RUA\s+[A-Z\s]+?).*?'
        r'(\d+).*?'
        r'(CHACARAS\s+CERES).*?'
        r'([\d-]+).*?'
        r'(SUZANO).*?'
        r'(Fabricação[^\n]+?).*?'
        r'(\d+\s*-\s*[A-Z\s]+?).*?'
        r'(\d+).*?'
        r'(\d{2}/\d{2}/\d{4})',
        re.IGNORECASE | re.DOTALL
    )
}

print("=== DEBUG PADRÕES SUZAQUIM ===")

for name, pattern in patterns.items():
    print(f"\n--- Testando {name} ---")
    match = pattern.search(text)
    if match:
        print(f"OK MATCH encontrado!")
        for i, group in enumerate(match.groups(), 1):
            print(f"  Grupo {i}: '{group}'")
    else:
        print("ERRO Sem match")

print("\n=== CONTEXT SUZAQUIM NO TEXTO ===")
lines = text.split('\n')
for i, line in enumerate(lines):
    if 'SUZAQUIM' in line.upper():
        print(f"Linha {i}: '{line}'")
        # Mostrar 5 linhas antes e depois
        for j in range(max(0, i-5), min(len(lines), i+6)):
            marker = '>>> ' if j == i else '    '
            print(f"{marker}{j}: '{lines[j]}'")
        break