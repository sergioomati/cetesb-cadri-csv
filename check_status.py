#!/usr/bin/env python3
"""Check status of collected data"""

import pandas as pd
from pathlib import Path

# Read CSVs
df_empresas = pd.read_csv('data/csv/empresas.csv')
df_docs = pd.read_csv('data/csv/cadri_documentos.csv')

print('=== STATUS ATUAL DOS DADOS ===')
print(f'Empresas coletadas: {len(df_empresas):,}')
print(f'Documentos CADRI encontrados: {len(df_docs):,}')

# Check pending PDFs
pending_mask = pd.isna(df_docs['status_pdf']) | (df_docs['status_pdf'] != 'downloaded')
pending = df_docs[pending_mask]
print(f'PDFs pendentes para download: {len(pending):,}')

# Check valid URLs
urls_validas = df_docs['url_pdf'].notna().sum()
print(f'URLs de PDF válidas: {urls_validas:,}')

print('\n📁 Arquivos CSV salvos em:')
print('   - data/csv/empresas.csv')
print('   - data/csv/cadri_documentos.csv')
print('\n✨ Pipeline modificado com sucesso!')
print('   Próximos passos:')
print('   1. Use interactive_pdf_downloader.py para baixar PDFs')
print('   2. Use pdf_parser_standalone.py para parsear PDFs')