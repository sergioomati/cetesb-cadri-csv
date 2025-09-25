#!/usr/bin/env python3
"""
Debug pattern construction step by step
"""

import re

test_text = """
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

# Test each part separately
patterns = {
    'nome_cadastro': re.compile(r'([A-Z\s]+LTDA)\s+(\d+-\d+-\d+)', re.IGNORECASE),
    'logradouro_num': re.compile(r'(RUA\s+[A-Z\s]+)\s+(\d+)', re.IGNORECASE),
    'bairro_cep_mun': re.compile(r'([A-Z\s]+)\s+([\d-]+)\s+([A-Z]+)', re.IGNORECASE),
    'atividade': re.compile(r'(Fabricação[^\n]+)', re.IGNORECASE),
    'bacia_lic_data': re.compile(r'(\d+\s*-\s*[A-Z\s]+)\s+(\d+)\s+(\d{2}/\d{2}/\d{4})', re.IGNORECASE),
}

def test_parts():
    for name, pattern in patterns.items():
        match = pattern.search(test_text)
        if match:
            print(f"OK {name}: {match.groups()}")
        else:
            print(f"FAIL {name}: No match")

if __name__ == "__main__":
    test_parts()