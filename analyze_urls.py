#!/usr/bin/env python3
"""Analisar impacto das mudanças nas URLs"""

import pandas as pd

# Ler dados atuais
df = pd.read_csv('data/csv/cadri_documentos.csv')

print('=== IMPACTO DAS MUDANÇAS ===')
print(f'Total de documentos: {len(df)}')

# URLs válidas
urls_validas = df[
    (df['url_pdf'].notna()) &
    (df['url_pdf'] != '') &
    (df['url_pdf'].str.contains('autenticidade.cetesb', na=False))
]
print(f'Documentos com URLs válidas: {len(urls_validas)}')

# URLs vazias ou inválidas
urls_invalidas = df[
    (df['url_pdf'].isna()) |
    (df['url_pdf'] == '') |
    (~df['url_pdf'].str.contains('autenticidade.cetesb', na=False))
]
print(f'Documentos sem URL válida: {len(urls_invalidas)}')

# Tipos de documento com/sem URL
print(f'\n=== ANÁLISE POR TIPO ===')
for tipo in df['tipo_documento'].value_counts().head(5).index:
    tipo_df = df[df['tipo_documento'] == tipo]
    com_url = len(tipo_df[
        (tipo_df['url_pdf'].notna()) &
        (tipo_df['url_pdf'] != '') &
        (tipo_df['url_pdf'].str.contains('autenticidade.cetesb', na=False))
    ])
    print(f'{tipo}: {com_url}/{len(tipo_df)} com URL')

print(f'\n✅ Apenas {len(urls_validas)} documentos serão processados no download interativo')
print(f'❌ {len(urls_invalidas)} documentos sem URL real foram filtrados')

# Verificar números muito curtos (provavelmente gerados)
numeros_curtos = df[df['numero_documento'].astype(str).str.len() <= 3]
print(f'\n📊 Documentos com números curtos (provavelmente gerados): {len(numeros_curtos)}')

# Testar nova função get_pending_pdfs
print(f'\n=== TESTE DA FUNÇÃO get_pending_pdfs() ===')
try:
    import sys
    sys.path.insert(0, 'src')
    from store_csv import get_pending_pdfs

    pending = get_pending_pdfs()
    print(f'PDFs pendentes (nova função): {len(pending)}')

    if len(pending) > 0:
        print(f'Exemplo de URL válida: {pending[0]["url_pdf"]}')

except Exception as e:
    print(f'Erro ao testar: {e}')