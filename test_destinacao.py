#!/usr/bin/env python3
"""
Script de teste para patterns de destinação
"""

import re
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

# Exemplo de texto da entidade de destinação baseado na imagem
test_text = """
ENTIDADE DE DESTINAÇÃO
Nome                                      Cadastro na CETESB
LEANDRO RAMIRES GARCIA LTDA - ME         521-100472-1
Logradouro                    Número      Complemento
Rua Vereador Francisco Coelho  745
Bairro                CEP        Município
Parque Industrial     16306-536   PENÁPOLIS
Descrição da Atividade
Tambores e bombonas plásticas para embalagem; recuperação de
Bacia Hidrográfica                N°LIC./CERT.FUNCION.    Data LIC./CERTIFIC.
22 - TIETÊ BAIXO                 13004293                  24/04/2024
"""

# Pattern melhorado e mais específico
pattern_especifico = re.compile(
    r'ENTIDADE\s+DE\s+DESTINAÇÃO.*?'
    r'([A-Z][A-Z\s&.-]+?LTDA\s*-?\s*ME)\s+(\d+-\d+-\d+).*?'
    r'Rua\s+([A-Za-z\s]+?)\s+(\d+).*?'
    r'([A-Z][A-Za-z\s]+?)\s+([\d-]+)\s+([A-ZÁÊÔÕÂÍÚ]+).*?'
    r'([^\n]+?embalagem[^\n]*?).*?'
    r'(\d+\s*-\s*[A-ZÁÊÔÕÂÍÚ\s]+?)\s+(\d{7,8})\s+([\d/]+)',
    re.DOTALL | re.IGNORECASE
)

# Padrões do parser antigo
patterns = {
    'entidade_destinacao': re.compile(
        r'ENTIDADE\s+DE\s+DESTINAÇÃO.*?'
        r'([A-Z][A-Z\s&.-]+?LTDA\s*-?\s*ME)\s+(\d+-\d+-\d+).*?'
        r'([A-Z][A-Za-z\s]+?)\s+(\d+).*?'
        r'([A-Z][A-Za-z\s]+?)\s+([\d-]+)\s+([A-Z]+).*?'
        r'([^\n]+?embalagem[^\n]*?).*?'
        r'(\d+\s*-\s*[A-ZÁÊÔÕÂÍÚ\s]+?).*?'
        r'(\d{7,8})\s+([\d/]+)',
        re.DOTALL | re.IGNORECASE
    ),

    'entidade_destinacao_simples': re.compile(
        r'([A-Z][A-Z\s&.-]+?LTDA\s*-?\s*ME|\b[A-Z][A-Z\s&.-]+?S\.?A\.?\b)\s+(\d+-\d+-\d+)'
        r'.*?([A-Z][A-Za-z\s]+?)\s+(\d+)'
        r'.*?([A-Z][A-Za-z\s]+?)\s+([\d-]+)\s+([A-Z]+)'
        r'.*?([^\n]+?recuperação[^\n]*?)'
        r'.*?(\d+\s*-\s*[A-ZÁÊÔÕÂ\s]+)'
        r'.*?(\d{7,8})\s+([\d/]+)',
        re.DOTALL | re.IGNORECASE
    )
}

def test_patterns():
    print("Testando patterns de destinação...")
    print("=" * 50)

    # Testar pattern específico primeiro
    match = pattern_especifico.search(test_text)
    if match:
        print("SUCESSO: Pattern especifico encontrou match!")
        print(f"  Nome: {match.group(1)}")
        print(f"  Cadastro: {match.group(2)}")
        print(f"  Logradouro: Rua {match.group(3)}")
        print(f"  Numero: {match.group(4)}")
        print(f"  Bairro: {match.group(5)}")
        print(f"  CEP: {match.group(6)}")
        print(f"  Municipio: {match.group(7)}")
        print(f"  Atividade: {match.group(8)}")
        print(f"  Bacia: {match.group(9)}")
        print(f"  Licenca: {match.group(10)}")
        print(f"  Data Licenca: {match.group(11)}")
    else:
        print("ERRO: Pattern especifico nao encontrou match")

    print("\n" + "=" * 50)

    # Testar pattern estruturado antigo
    match = patterns['entidade_destinacao'].search(test_text)
    if match:
        print("SUCESSO: Pattern estruturado encontrou match!")
        for i, group in enumerate(match.groups(), 1):
            print(f"  Grupo {i}: '{group}'")

        print("\nDados extraidos:")
        # Pattern corrigido tem apenas 11 grupos, não 12
        print(f"  Nome: {match.group(1)}")
        print(f"  Cadastro: {match.group(2)}")
        print(f"  Logradouro: {match.group(3)}")
        print(f"  Numero: {match.group(4)}")
        print(f"  Bairro: {match.group(5)}")
        print(f"  CEP: {match.group(6)}")
        print(f"  Municipio: {match.group(7)}")
        print(f"  Atividade: {match.group(8)}")
        print(f"  Bacia: {match.group(9)}")
        print(f"  Licenca: {match.group(10)}")
        print(f"  Data Licenca: {match.group(11)}")
    else:
        print("ERRO: Pattern estruturado nao encontrou match")

    print("\n" + "=" * 50)

    # Testar pattern simples
    match_simples = patterns['entidade_destinacao_simples'].search(test_text)
    if match_simples:
        print("SUCESSO: Pattern simples encontrou match!")
        for i, group in enumerate(match_simples.groups(), 1):
            print(f"  Grupo {i}: '{group}'")
    else:
        print("ERRO: Pattern simples nao encontrou match")

if __name__ == "__main__":
    test_patterns()