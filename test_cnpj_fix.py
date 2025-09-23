#!/usr/bin/env python3
"""
# DEBUG_CNPJ: Script de teste para verificar se a correção do campo CGC funciona
TEMPORÁRIO - Para testar a correção
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scrape_list import ListScraper
from seeds import SeedManager


async def test_cnpj_search():
    """# DEBUG_CNPJ: Testar busca por CNPJ com campo CGC corrigido"""
    print("# DEBUG_CNPJ: Testando busca por CNPJ com campo CGC...")

    # CNPJ que estava falhando
    test_cnpj = "60409075002953"

    # Criar scraper
    seed_manager = SeedManager()
    scraper = ListScraper(seed_manager)

    print(f"# DEBUG_CNPJ: CNPJ teste: {test_cnpj}")
    print(f"# DEBUG_CNPJ: Seletor CNPJ configurado: {scraper.FORM_SELECTORS['cnpj']}")

    try:
        # Executar busca
        results = await scraper.search_by_cnpj(test_cnpj)

        print(f"# DEBUG_CNPJ: Busca concluída!")
        print(f"# DEBUG_CNPJ: Resultados encontrados: {len(results)}")

        if results:
            print(f"# DEBUG_CNPJ: ✅ SUCESSO! Busca por CNPJ funcionou")
            for i, result in enumerate(results[:3], 1):  # Mostrar primeiros 3
                print(f"# DEBUG_CNPJ: Resultado {i}: {result.get('razao_social', 'N/A')} - {result.get('url', 'N/A')}")
        else:
            print(f"# DEBUG_CNPJ: ⚠️ Busca executou mas não retornou resultados")
            print(f"# DEBUG_CNPJ: Isso pode ser normal se CNPJ não tem processos na CETESB")

    except Exception as e:
        print(f"# DEBUG_CNPJ: ❌ ERRO na busca: {e}")
        import traceback
        print(f"# DEBUG_CNPJ: Stack trace: {traceback.format_exc()}")

    print(f"# DEBUG_CNPJ: Teste concluído")


if __name__ == "__main__":
    print("# DEBUG_CNPJ: Executando teste de correção do campo CGC...")
    asyncio.run(test_cnpj_search())