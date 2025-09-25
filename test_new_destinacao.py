#!/usr/bin/env python3
"""
Script de teste para novos patterns de destinação baseados na imagem real
"""

import re
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

# Exemplo de texto da entidade de destinação baseado na imagem fornecida
test_text_new = """
ENTIDADE DE DESTINAÇÃO
Nome                                                          Cadastro na CETESB
SUZAQUIM INDÚSTRIAS QUÍMICAS LTDA                            672-000343-7
Logradouro                           Número      Complemento
RUA RAPHAEL DA ANUNCIACAO FONTES     349
Bairro                CEP        Município
CHACARAS CERES       08655-243   SUZANO
Descrição da Atividade
Fabricação de outros produtos químicos não especificados anteriormente
Bacia Hidrográfica                N°LIC./CERT.FUNCION.    Data LIC./CERTIFIC.
1 - TIETÊ ALTO CABECEIRAS         26004251                  17/12/2013
"""

# Padrões progressivos do simples para o completo
patterns_new = {
    # Pattern completo que captura todos os campos
    'entidade_destinacao': re.compile(
        r'([A-Z\s]+LTDA)\s+(\d+-\d+-\d+).*?'
        r'(RUA\s+[A-Z\s]+)\s+(\d+).*?'
        r'([A-Z\s]+)\s+([\d-]+)\s+([A-Z]+).*?'
        r'(Fabricação[^\n]+).*?'
        r'(\d+\s*-\s*[A-Z\s]+)\s+(\d+)\s+(\d{2}/\d{2}/\d{4})',
        re.DOTALL | re.IGNORECASE
    ),

    # Pattern genérico que funcionou
    'entidade_destinacao_simples': re.compile(
        r'([A-Z\s]+LTDA)\s+(\d+-\d+-\d+)',
        re.IGNORECASE
    ),

    # Pattern super simples para debug
    'entidade_debug': re.compile(
        r'SUZAQUIM.*?(\d+-\d+-\d+)',
        re.IGNORECASE
    ),
}

def test_new_patterns():
    print("Testando novos patterns de destinação...")
    print("=" * 60)

    # Testar pattern principal
    match = patterns_new['entidade_destinacao'].search(test_text_new)
    if match:
        print("SUCESSO: Pattern principal encontrou match!")
        print("Dados extraidos:")
        print(f"  Nome: '{match.group(1)}'")
        print(f"  Cadastro: '{match.group(2)}'")
        print(f"  Logradouro: '{match.group(3)}'")
        print(f"  Numero: '{match.group(4)}'")
        print(f"  Bairro: '{match.group(5)}'")
        print(f"  CEP: '{match.group(6)}'")
        print(f"  Municipio: '{match.group(7)}'")
        print(f"  Atividade: '{match.group(8)}'")
        print(f"  Bacia: '{match.group(9)}'")
        print(f"  Licenca: '{match.group(10)}'")
        print(f"  Data Licenca: '{match.group(11)}'")
    else:
        print("ERRO: Pattern principal nao encontrou match")

    print("\n" + "=" * 60)

    # Testar pattern simples
    match_simples = patterns_new['entidade_destinacao_simples'].search(test_text_new)
    if match_simples:
        print("SUCESSO: Pattern simples encontrou match!")
        print("Dados extraidos:")
        for i, group in enumerate(match_simples.groups(), 1):
            print(f"  Grupo {i}: '{group}'")
    else:
        print("ERRO: Pattern simples nao encontrou match")

    print("\n" + "=" * 60)

    # Testar pattern debug
    match_debug = patterns_new['entidade_debug'].search(test_text_new)
    if match_debug:
        print("SUCESSO: Pattern debug encontrou match!")
        print(f"Cadastro: {match_debug.group(1)}")
    else:
        print("ERRO: Pattern debug nao encontrou match")

if __name__ == "__main__":
    test_new_patterns()