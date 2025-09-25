"""
PDF URL Builder - Constrói URLs de PDFs baseado no padrão descoberto
"""

from typing import Optional, Tuple
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs


def extract_idocmn_from_url(url: str) -> Optional[str]:
    """
    Extrai o parâmetro idocmn de uma URL de autenticidade

    Args:
        url: URL de autenticidade (ex: autentica.php?idocmn=27&ndocmn=16000520)

    Returns:
        idocmn ou None se não encontrado
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get('idocmn', [None])[0]
    except:
        return None


def format_date_to_ddmmyyyy(date_str: str) -> Optional[str]:
    """
    Converte data para formato DDMMAAAA

    Args:
        date_str: Data em formato DD/MM/AAAA, DD-MM-AAAA ou AAAA-MM-DD

    Returns:
        Data no formato DDMMAAAA ou None se inválida
    """
    if not date_str:
        return None

    # Remover espaços
    date_str = date_str.strip()

    # Tentar diferentes formatos
    formats = [
        ('%d/%m/%Y', '%d%m%Y'),  # DD/MM/AAAA
        ('%d-%m-%Y', '%d%m%Y'),  # DD-MM-AAAA
        ('%Y-%m-%d', '%d%m%Y'),  # AAAA-MM-DD
        ('%d/%m/%y', '%d%m%Y'),  # DD/MM/AA (2 dígitos)
        ('%d-%m-%y', '%d%m%Y'),  # DD-MM-AA (2 dígitos)
    ]

    for input_fmt, output_fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, input_fmt)
            return date_obj.strftime(output_fmt)
        except ValueError:
            continue

    # Se já estiver no formato DDMMAAAA
    if re.match(r'^\d{8}$', date_str):
        return date_str

    return None


def build_pdf_url(
    idocmn: str,
    ndocmn: str,
    data_emissao: str,
    versao: str = "01"
) -> Optional[str]:
    """
    Constrói URL do PDF baseado no padrão descoberto

    Padrão: https://autenticidade.cetesb.sp.gov.br/pdf/{idocmn}{ndocmn}{versao}{data}.pdf

    Args:
        idocmn: ID do tipo de documento (ex: "12" para CADRI, "27" para outros)
        ndocmn: Número do documento (ex: "16000520")
        data_emissao: Data de emissão em qualquer formato comum
        versao: Versão do documento (padrão "01")

    Returns:
        URL completa do PDF ou None se dados inválidos

    Example:
        >>> build_pdf_url("27", "16000520", "09/11/2010", "01")
        'https://autenticidade.cetesb.sp.gov.br/pdf/27160005200109112010.pdf'
    """
    # Validar entradas
    if not all([idocmn, ndocmn, data_emissao]):
        return None

    # Formatar data
    data_formatted = format_date_to_ddmmyyyy(data_emissao)
    if not data_formatted:
        return None

    # Limpar números (remover espaços, zeros à esquerda desnecessários)
    idocmn = idocmn.strip()
    ndocmn = ndocmn.strip()
    versao = versao.strip()

    # Construir código
    codigo = f"{idocmn}{ndocmn.zfill(8)}{versao}{data_formatted}"

    # Retornar URL
    return f"https://autenticidade.cetesb.sp.gov.br/pdf/{codigo}.pdf"


def build_pdf_url_with_fallback(
    idocmn: str,
    ndocmn: str,
    data_emissao: str,
    max_version: int = 3
) -> list:
    """
    Constrói lista de possíveis URLs do PDF com diferentes versões

    Args:
        idocmn: ID do tipo de documento
        ndocmn: Número do documento
        data_emissao: Data de emissão
        max_version: Número máximo de versões para tentar

    Returns:
        Lista de URLs para tentar em ordem
    """
    urls = []

    for version in range(1, max_version + 1):
        versao_str = str(version).zfill(2)  # 01, 02, 03...
        url = build_pdf_url(idocmn, ndocmn, data_emissao, versao_str)
        if url:
            urls.append(url)

    return urls


def parse_autenticidade_url(url: str) -> Optional[Tuple[str, str]]:
    """
    Extrai idocmn e ndocmn de uma URL de autenticidade

    Args:
        url: URL completa de autenticidade

    Returns:
        Tupla (idocmn, ndocmn) ou None
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        idocmn = params.get('idocmn', [None])[0]
        ndocmn = params.get('ndocmn', [None])[0]

        if idocmn and ndocmn:
            return (idocmn, ndocmn)
    except:
        pass

    return None


def get_default_idocmn(tipo_documento: str) -> str:
    """
    Retorna o idocmn padrão baseado no tipo de documento

    Args:
        tipo_documento: Tipo do documento (ex: "CADRI", "CERTIFICADO")

    Returns:
        idocmn padrão
    """
    # Mapear tipos conhecidos
    tipo_map = {
        'CADRI': '12',
        'CERTIFICADO': '27',
        'LICENCA': '15',
        'DOCUMENTO': '12',  # Padrão para tipo genérico
    }

    # Normalizar tipo
    tipo_upper = tipo_documento.upper() if tipo_documento else 'DOCUMENTO'

    # Buscar no mapa
    for key, value in tipo_map.items():
        if key in tipo_upper:
            return value

    # Padrão
    return '12'


if __name__ == "__main__":
    # Testes
    print("Testando PDF URL Builder...")

    # Teste 1: Construir URL básica
    url = build_pdf_url("27", "16000520", "09/11/2010", "01")
    print(f"URL gerada: {url}")
    assert url == "https://autenticidade.cetesb.sp.gov.br/pdf/27160005200109112010.pdf"

    # Teste 2: Diferentes formatos de data
    dates = ["09/11/2010", "09-11-2010", "2010-11-09", "09112010"]
    for date in dates:
        formatted = format_date_to_ddmmyyyy(date)
        print(f"Data {date} -> {formatted}")
        assert formatted == "09112010"

    # Teste 3: Múltiplas versões
    urls = build_pdf_url_with_fallback("12", "57000087", "15/03/2023")
    print(f"URLs com fallback: {urls}")

    # Teste 4: Extrair de URL
    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=27&ndocmn=16000520"
    idocmn, ndocmn = parse_autenticidade_url(test_url)
    print(f"Extraído da URL: idocmn={idocmn}, ndocmn={ndocmn}")

    print("✅ Todos os testes passaram!")