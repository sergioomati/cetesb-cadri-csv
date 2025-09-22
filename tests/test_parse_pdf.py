import pytest
import sys
from pathlib import Path
import fitz  # PyMuPDF

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parse_pdf import PDFParser
from utils_text import normalize_classe, extract_quantity_unit, is_valid_residue_name


class TestPDFParser:
    """Test PDF parsing functionality"""

    @pytest.fixture
    def parser(self):
        """Create parser instance"""
        return PDFParser()

    @pytest.fixture
    def sample_pdf_content(self):
        """Sample PDF content for testing"""
        return """
        CERTIFICADO DE MOVIMENTAÇÃO DE RESÍDUOS DE INTERESSE AMBIENTAL

        Empresa: INDUSTRIA EXEMPLO LTDA
        CNPJ: 12.345.678/0001-90

        RELAÇÃO DE RESÍDUOS AUTORIZADOS:

        Resíduo                     Classe    Estado Físico    Quantidade
        --------------------------------------------------------------
        Óleo Lubrificante Usado     I         Líquido         1000 L
        Lodo de ETE                 IIA       Pastoso         50 t
        Papel e Papelão             IIB       Sólido          10 ton
        Sucata Metálica             IIB       Sólido          5000 kg
        """

    @pytest.fixture
    def create_test_pdf(self, tmp_path, sample_pdf_content):
        """Create a test PDF file"""
        pdf_path = tmp_path / "test_cadri.pdf"

        # Create PDF with PyMuPDF
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 50), sample_pdf_content, fontsize=11)
        doc.save(pdf_path)
        doc.close()

        return pdf_path

    def test_parse_table_row(self, parser):
        """Test parsing individual table rows"""
        # Tab-separated
        row = "Óleo Usado\tI\tLíquido\t1000 L"
        item = parser.parse_table_row(row, 1, 1)

        assert item is not None
        assert item['residuo'] == "Óleo Usado"
        assert item['classe'] == "1"
        assert item['estado_fisico'] == "líquido"
        assert item['quantidade'] == "1000"
        assert item['unidade'] == "L"

        # Pipe-separated
        row = "Lodo de ETE|IIA|Pastoso|50 toneladas"
        item = parser.parse_table_row(row, 1, 2)

        assert item is not None
        assert item['residuo'] == "Lodo de ETE"
        assert item['classe'] == "2A"
        assert item['estado_fisico'] == "pastoso"

    def test_extract_from_table(self, parser):
        """Test table extraction from text"""
        text = """
        Resíduo         Classe  Estado      Quantidade
        Óleo Usado      I       Líquido     1000 L
        Lodo ETE        IIA     Pastoso     50 t
        """

        items = parser.extract_from_table(text, 1)

        assert len(items) >= 1
        assert any(item['residuo'] == "Óleo Usado" for item in items)

    def test_deduplicate_items(self, parser):
        """Test item deduplication"""
        items = [
            {'residuo': 'Óleo', 'classe': 'I', 'estado_fisico': 'líquido', 'quantidade': '100'},
            {'residuo': 'Óleo', 'classe': 'I', 'estado_fisico': 'líquido', 'quantidade': '100'},
            {'residuo': 'Lodo', 'classe': 'IIA', 'estado_fisico': 'pastoso', 'quantidade': '50'}
        ]

        unique = parser.deduplicate_items(items)

        assert len(unique) == 2
        assert unique[0]['residuo'] == 'Óleo'
        assert unique[1]['residuo'] == 'Lodo'

    def test_parse_pdf_integration(self, parser, create_test_pdf):
        """Test complete PDF parsing"""
        items = parser.parse_pdf(create_test_pdf)

        assert len(items) > 0

        # Check if key items were extracted
        residuos = [item['residuo'] for item in items]
        assert any('Óleo' in r or 'óleo' in r.lower() for r in residuos)


class TestUtilityFunctions:
    """Test PDF parsing utility functions"""

    def test_normalize_classe(self):
        """Test class normalization"""
        assert normalize_classe("I") == "1"
        assert normalize_classe("IIA") == "2A"
        assert normalize_classe("IIB") == "2B"
        assert normalize_classe("II A") == "2A"
        assert normalize_classe("2 B") == "2B"

    def test_extract_quantity_unit(self):
        """Test quantity and unit extraction"""
        qty, unit = extract_quantity_unit("1000 kg")
        assert qty == "1000"
        assert unit == "kg"

        qty, unit = extract_quantity_unit("50 toneladas")
        assert qty == "50"
        assert unit == "t"

        qty, unit = extract_quantity_unit("100,5 L")
        assert qty == "100.5"
        assert unit == "L"

        qty, unit = extract_quantity_unit("10 m³")
        assert qty == "10"
        assert unit == "m³"

    def test_is_valid_residue_name(self):
        """Test residue name validation"""
        assert is_valid_residue_name("Óleo Lubrificante Usado")
        assert is_valid_residue_name("Lodo de ETE")
        assert is_valid_residue_name("Papel e Papelão")

        # Invalid names
        assert not is_valid_residue_name("")
        assert not is_valid_residue_name("AB")  # Too short
        assert not is_valid_residue_name("Página 1")  # Contains blacklisted word
        assert not is_valid_residue_name("123")  # No letters


@pytest.fixture
def mock_pdf_with_variations(tmp_path):
    """Create PDF with various text variations"""
    content = """
    CADRI - CERTIFICADO

    1. Informações da Empresa
    Razão Social: TESTE INDUSTRIA LTDA

    2. Resíduos Autorizados

    Nº  Descrição do Resíduo          Classificação   Estado    Qtde/Ano
    01  Borra Oleosa                  Classe I        Pastoso   100 ton
    02  Embalagens Contaminadas       Classe I        Sólido    5 t
    03  Restos de Varrição            Classe II-B     Sólido    20 toneladas

    Observações:
    - Estado Físico pode ser: Sólido, Líquido, Pastoso, ou Gasoso
    - Quantidade expressa em toneladas/ano
    """

    pdf_path = tmp_path / "variations.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), content, fontsize=10)
    doc.save(pdf_path)
    doc.close()

    return pdf_path


def test_pdf_with_variations(mock_pdf_with_variations):
    """Test PDF with various formatting variations"""
    parser = PDFParser()
    items = parser.parse_pdf(mock_pdf_with_variations)

    # Should extract items despite variations
    assert len(items) > 0

    # Check for specific items
    residuos = [item['residuo'] for item in items]
    assert any('Borra' in r for r in residuos)

    # Check class variations
    classes = [item['classe'] for item in items]
    assert any(c in ['1', '2B'] for c in classes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])