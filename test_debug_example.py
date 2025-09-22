#!/usr/bin/env python3
"""
Exemplo de como usar o script de debug para testar empresas específicas

Este script demonstra como usar o debug_single_company.py para
investigar por que não estão sendo encontrados documentos CADRI.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from debug_single_company import DebugSingleCompany
from logging_conf import setup_logging, logger


async def test_examples():
    """Test com exemplos reais"""

    # Configure logging para ver tudo
    setup_logging(level='DEBUG')

    debug = DebugSingleCompany()

    logger.info("=== INICIANDO TESTES DE DEBUG ===")

    # Exemplo 1: Testar com CNPJ conhecido (do CSV de empresas)
    logger.info("\n" + "="*60)
    logger.info("TESTE 1: CNPJ específico")
    logger.info("="*60)

    # Pegar primeiro CNPJ válido da lista
    test_cnpj = "00000326000119"  # Do CSV de empresas
    logger.info(f"Testando CNPJ: {test_cnpj}")

    company_info, documents = debug.debug_detail_page(
        f"https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc={test_cnpj}"
    )

    logger.info(f"Resultado CNPJ {test_cnpj}:")
    logger.info(f"  - Nome: {company_info.get('razao_social', 'Não encontrado')}")
    logger.info(f"  - Documentos: {len(documents)}")

    # Exemplo 2: Buscar por termo genérico
    logger.info("\n" + "="*60)
    logger.info("TESTE 2: Busca por termo")
    logger.info("="*60)

    search_term = "PETROBRAS"  # Empresa conhecida que pode ter CADRI
    logger.info(f"Buscando por: {search_term}")

    try:
        results = await debug.search_company(search_term)

        if results:
            # Testar primeira empresa encontrada
            first_company = results[0]
            logger.info(f"Testando primeira empresa: {first_company.get('razao_social', 'N/A')}")

            company_info, documents = debug.debug_detail_page(first_company['url'])

            logger.info(f"Resultado busca {search_term}:")
            logger.info(f"  - Nome: {company_info.get('razao_social', 'Não encontrado')}")
            logger.info(f"  - Documentos: {len(documents)}")
        else:
            logger.warning(f"Nenhuma empresa encontrada para '{search_term}'")

    except Exception as e:
        logger.error(f"Erro na busca: {e}")

    # Exemplo 3: Teste com URL direta (se conhecer alguma)
    logger.info("\n" + "="*60)
    logger.info("TESTE 3: URL direta")
    logger.info("="*60)

    # URL de exemplo - pode não existir
    test_url = "https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc=33000167000101"
    logger.info(f"Testando URL direta: {test_url}")

    company_info, documents = debug.debug_detail_page(test_url)

    logger.info(f"Resultado URL direta:")
    logger.info(f"  - Nome: {company_info.get('razao_social', 'Não encontrado')}")
    logger.info(f"  - Documentos: {len(documents)}")

    # Resumo final
    logger.info("\n" + "="*60)
    logger.info("RESUMO DOS TESTES")
    logger.info("="*60)
    logger.info(f"Arquivos de debug salvos em: {debug.session_dir}")
    logger.info("\nPróximos passos:")
    logger.info("1. Analise os arquivos HTML salvos para ver a estrutura real das páginas")
    logger.info("2. Verifique os arquivos JSON para entender o que foi extraído")
    logger.info("3. Use o html_analyzer.py para análise mais detalhada:")
    logger.info(f"   python -m src.html_analyzer --dir {debug.session_dir}")


def show_usage_examples():
    """Mostra exemplos de uso do script de debug"""
    print("\n" + "="*70)
    print("EXEMPLOS DE USO DO DEBUG_SINGLE_COMPANY.PY")
    print("="*70)

    print("\n1. Testar com CNPJ específico:")
    print("   python debug_single_company.py --cnpj 12345678000100")

    print("\n2. Buscar por nome da empresa:")
    print("   python debug_single_company.py --search PETROBRAS")

    print("\n3. Testar URL direta:")
    print("   python debug_single_company.py --url 'https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc=12345678000100'")

    print("\n4. Com log detalhado:")
    print("   python debug_single_company.py --search CEMIG --log-level DEBUG")

    print("\n" + "="*70)
    print("ANÁLISE DOS RESULTADOS")
    print("="*70)

    print("\nApós executar o debug, você pode analisar os resultados:")

    print("\n1. Verificar arquivos HTML salvos:")
    print("   - Abra os arquivos .html em um navegador")
    print("   - Procure por tabelas ou estruturas com documentos")

    print("\n2. Analisar dados extraídos (arquivos JSON):")
    print("   - company_info.json: Dados da empresa extraídos")
    print("   - documents.json: Documentos CADRI encontrados")
    print("   - extraction_patterns.json: Padrões tentados")

    print("\n3. Usar o analisador HTML:")
    print("   python -m src.html_analyzer --dir data/debug/session_XXXXXXXX")

    print("\n4. Problemas comuns e soluções:")
    print("   - Página vazia: Verifique se CNPJ existe")
    print("   - Sem documentos: Página pode não ter CADRI ou estrutura diferente")
    print("   - Erro de rede: Verificar conectividade e rate limiting")

    print("\n" + "="*70)


async def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Exemplo de uso do debug de empresa única")
    parser.add_argument('--run-tests', action='store_true', help='Executar testes de exemplo')
    parser.add_argument('--show-usage', action='store_true', help='Mostrar exemplos de uso')

    args = parser.parse_args()

    if args.run_tests:
        await test_examples()
    elif args.show_usage:
        show_usage_examples()
    else:
        print("Uso:")
        print("  python test_debug_example.py --run-tests      # Executar testes")
        print("  python test_debug_example.py --show-usage     # Mostrar exemplos")


if __name__ == "__main__":
    asyncio.run(main())