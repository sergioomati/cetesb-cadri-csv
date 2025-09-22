import re
import unicodedata
from typing import Optional, List, Tuple


def normalize_text(text: str) -> str:
    """Normalize text: remove accents, extra spaces, uppercase"""
    if not text:
        return ""

    # Remove accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')

    # Uppercase and clean spaces
    text = ' '.join(text.upper().split())

    return text


def clean_cnpj(cnpj: str) -> str:
    """Clean CNPJ, keeping only digits and zero-padding to 14"""
    if not cnpj:
        return ""

    # Keep only digits
    cnpj = re.sub(r'\D', '', cnpj)

    # Zero-pad to 14 digits
    return cnpj.zfill(14)


def format_cnpj(cnpj: str) -> str:
    """Format CNPJ with dots and slashes"""
    cnpj = clean_cnpj(cnpj)
    if len(cnpj) != 14:
        return cnpj

    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def extract_document_number(text: str) -> Optional[str]:
    """Extract document number from various formats"""
    if not text:
        return None

    # Try various patterns
    patterns = [
        r'(\d{3,})',  # Simple sequence of 3+ digits
        r'CAD[A-Z]*\s*[:-]?\s*(\d+)',  # CADRI format
        r'N[º°]?\s*(\d+)',  # Number with degree symbol
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


def parse_date_br(date_str: str) -> Optional[str]:
    """Parse Brazilian date format (DD/MM/YYYY) to ISO format"""
    if not date_str:
        return None

    match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    return None


def extract_quantity_unit(text: str) -> Tuple[str, str]:
    """
    Extract quantity and unit from text
    Returns: (quantity, unit)
    """
    if not text:
        return "", ""

    # Clean text
    text = text.strip()

    # Common patterns
    patterns = [
        # Number with decimal and unit
        r'([\d.,]+)\s*(kg|t|ton|tonelada|m3|m³|l|litro|unidade|un)?',
        # Written numbers
        r'(um|uma|dois|duas|tres|quatro|cinco)\s*(kg|t|ton|tonelada|m3|m³|l|litro|unidade|un)?',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            quantity = match.group(1).replace(',', '.')
            unit = match.group(2) if match.group(2) else ""

            # Normalize units
            unit_map = {
                'tonelada': 't',
                'ton': 't',
                'litro': 'L',
                'l': 'L',
                'm3': 'm³',
                'unidade': 'un',
            }
            unit = unit_map.get(unit.lower(), unit)

            return quantity, unit

    # If no pattern matches, return the whole text as quantity
    return text, ""


def create_pdf_search_patterns() -> dict:
    """Create regex patterns for PDF parsing with variations"""
    patterns = {
        # Resíduo variations
        'residuo': re.compile(
            r'(?:res[ií]duo|material|subst[âa]ncia)[:\s-]*([^\n]{3,100})',
            re.IGNORECASE | re.MULTILINE
        ),

        # Classe variations
        'classe': re.compile(
            r'(?:classe|classifica[çc][ãa]o|tipo)[:\s-]*([IVX]+(?:[AB])?|\d+[AB]?)',
            re.IGNORECASE
        ),

        # Estado físico variations
        'estado_fisico': re.compile(
            r'(?:estado\s*f[ií]sico|forma|apresenta[çc][ãa]o)[:\s-]*'
            r'(s[óo]lido|l[ií]quido|gasoso|pastoso|lama|p[óo]|gel)',
            re.IGNORECASE
        ),

        # Quantidade variations
        'quantidade': re.compile(
            r'(?:quantidade|qtd|qtde|volume|peso|massa)[:\s-]*'
            r'([\d.,]+\s*(?:kg|t|ton|tonelada|m3|m³|l|litro|unidade|un)?)',
            re.IGNORECASE
        ),

        # Table row pattern (for structured data)
        'table_row': re.compile(
            r'^([^\t\|]+)[\t\|]+([IVX]+[AB]?)[\t\|]+'
            r'(s[óo]lido|l[ií]quido|gasoso|pastoso)[\t\|]+'
            r'([\d.,]+\s*\w*)',
            re.IGNORECASE | re.MULTILINE
        )
    }

    return patterns


def is_valid_residue_name(text: str) -> bool:
    """Check if text is a valid residue name"""
    if not text or len(text) < 3:
        return False

    # Remove common false positives
    blacklist = [
        'página', 'pagina', 'data', 'número', 'numero',
        'documento', 'certificado', 'cadri', 'cetesb'
    ]

    text_lower = text.lower()
    for word in blacklist:
        if word in text_lower:
            return False

    # Must contain at least one letter
    if not re.search(r'[a-zA-Z]', text):
        return False

    return True


def normalize_classe(classe: str) -> str:
    """Normalize waste class notation"""
    if not classe:
        return ""

    classe = classe.upper().strip()

    # Convert Roman to standard notation
    roman_map = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5'
    }

    for roman, arabic in roman_map.items():
        classe = classe.replace(roman, arabic)

    # Ensure format like "2A", "2B", etc.
    match = re.match(r'(\d+)\s*([AB])?', classe)
    if match:
        num = match.group(1)
        letter = match.group(2) or ""
        return f"{num}{letter}"

    return classe


def extract_trigrams(text: str) -> List[str]:
    """Extract all 3-letter sequences from text"""
    text = normalize_text(text)
    # Remove non-alphanumeric
    text = re.sub(r'[^A-Z0-9]', '', text)

    if len(text) < 3:
        return []

    trigrams = []
    for i in range(len(text) - 2):
        trigram = text[i:i+3]
        # Only include if it has at least one letter
        if re.search(r'[A-Z]', trigram):
            trigrams.append(trigram)

    return list(set(trigrams))  # Remove duplicates