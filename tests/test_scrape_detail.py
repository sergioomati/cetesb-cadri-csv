import pytest
import sys
from pathlib import Path
from bs4 import BeautifulSoup

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scrape_detail import DetailScraper
from utils_text import clean_cnpj


class TestDetailScraper:
    """Test detail page scraping"""

    @pytest.fixture
    def scraper(self):
        """Create scraper instance"""
        return DetailScraper()

    @pytest.fixture
    def sample_html_with_cadri(self):
        """Sample HTML with CADRI document"""
        return """
        <html>
        <body>
            <h1>EMPRESA EXEMPLO LTDA</h1>
            <p><b>CNPJ:</b> 12.345.678/0001-90</p>
            <p><b>Município:</b> São Paulo - SP</p>

            <table>
                <tr>
                    <th>Tipo Documento</th>
                    <th>Número</th>
                    <th>Data</th>
                    <th>Ação</th>
                </tr>
                <tr>
                    <td>CERT MOV RESIDUOS INT AMB</td>
                    <td>123456</td>
                    <td>01/01/2024</td>
                    <td><a href="detalhe.asp?doc=123456">Ver</a></td>
                </tr>
                <tr>
                    <td>LICENÇA OPERAÇÃO</td>
                    <td>789012</td>
                    <td>15/03/2024</td>
                    <td><a href="detalhe.asp?doc=789012">Ver</a></td>
                </tr>
            </table>
        </body>
        </html>
        """

    @pytest.fixture
    def sample_html_alternative_format(self):
        """Sample HTML with alternative format"""
        return """
        <html>
        <body>
            <div class="empresa">
                <strong>Razão Social:</strong> INDUSTRIA TESTE SA<br>
                <strong>CNPJ:</strong> 98765432000199<br>
                <strong>Endereço:</strong> Campinas - SP
            </div>

            <div class="documentos">
                <h3>Documentos Emitidos</h3>
                <ul>
                    <li>
                        CERT MOV RESIDUOS INT AMB - Nº 555666
                        <span>Emitido em 10/02/2024</span>
                        <a href="doc.php?id=555666">Download</a>
                    </li>
                    <li>
                        ALVARÁ - Nº 111222
                        <span>Emitido em 05/01/2024</span>
                    </li>
                </ul>
            </div>
        </body>
        </html>
        """

    def test_extract_cnpj_from_url(self, scraper):
        """Test CNPJ extraction from URL"""
        url = "https://site.com/processo.asp?cgc=12345678000190"
        cnpj = scraper.extract_cnpj_from_url(url)
        assert cnpj == "12345678000190"

        # Test with zeros
        url = "https://site.com/processo.asp?cgc=1234567890"
        cnpj = scraper.extract_cnpj_from_url(url)
        assert cnpj == "00001234567890"  # Zero-padded

    def test_extract_company_info(self, scraper, sample_html_with_cadri):
        """Test company information extraction"""
        soup = BeautifulSoup(sample_html_with_cadri, 'html.parser')
        info = scraper.extract_company_info(soup, "http://test.com?cgc=12345678000190")

        assert info['cnpj'] == "12345678000190"
        assert "EMPRESA EXEMPLO" in info['razao_social']
        assert info['municipio'] == "São Paulo"
        assert info['uf'] == "SP"

    def test_extract_cadri_documents(self, scraper, sample_html_with_cadri):
        """Test CADRI document extraction from table"""
        soup = BeautifulSoup(sample_html_with_cadri, 'html.parser')
        company_info = {'cnpj': '12345678000190', 'razao_social': 'EMPRESA EXEMPLO'}

        docs = scraper.extract_cadri_documents(soup, company_info)

        assert len(docs) == 1  # Only CADRI document
        assert docs[0]['numero_documento'] == '123456'
        assert docs[0]['tipo_documento'] == 'CERT MOV RESIDUOS INT AMB'
        assert docs[0]['data_emissao'] == '2024-01-01'
        assert 'autentica.php' in docs[0]['url_pdf']

    def test_extract_cadri_alternative_format(self, scraper, sample_html_alternative_format):
        """Test CADRI extraction from alternative HTML format"""
        soup = BeautifulSoup(sample_html_alternative_format, 'html.parser')
        company_info = {'cnpj': '98765432000199', 'razao_social': 'INDUSTRIA TESTE'}

        docs = scraper.extract_cadri_documents(soup, company_info)

        assert len(docs) == 1
        assert docs[0]['numero_documento'] == '555666'
        assert docs[0]['tipo_documento'] == 'CERT MOV RESIDUOS INT AMB'

    def test_parse_document_row(self, scraper):
        """Test parsing individual document row"""
        html = """
        <tr>
            <td>CERT MOV RESIDUOS INT AMB</td>
            <td>999888</td>
            <td>31/12/2023</td>
            <td><a href="view.asp?id=999888">Visualizar</a></td>
        </tr>
        """
        soup = BeautifulSoup(html, 'html.parser')
        row = soup.find('tr')
        company_info = {'cnpj': '11111111000111'}

        doc = scraper.parse_document_row(row, company_info)

        assert doc is not None
        assert doc['numero_documento'] == '999888'
        assert doc['tipo_documento'] == 'CERT MOV RESIDUOS INT AMB'
        assert doc['data_emissao'] == '2023-12-31'

    def test_parse_document_row_wrong_type(self, scraper):
        """Test that non-CADRI documents are filtered out"""
        html = """
        <tr>
            <td>LICENÇA DE OPERAÇÃO</td>
            <td>111222</td>
            <td>01/01/2024</td>
        </tr>
        """
        soup = BeautifulSoup(html, 'html.parser')
        row = soup.find('tr')
        company_info = {'cnpj': '11111111000111'}

        doc = scraper.parse_document_row(row, company_info)

        # Should return None because it's not CADRI
        assert doc is None


class TestCNPJProcessing:
    """Test CNPJ cleaning and formatting"""

    def test_clean_cnpj(self):
        """Test CNPJ cleaning"""
        assert clean_cnpj("12.345.678/0001-90") == "12345678000190"
        assert clean_cnpj("12345678000190") == "12345678000190"
        assert clean_cnpj("123456780001") == "00123456780001"  # Zero-pad
        assert clean_cnpj("") == ""


@pytest.fixture
def mock_response_no_cadri():
    """Mock HTML response with no CADRI documents"""
    return """
    <html>
    <body>
        <h2>Processos da Empresa</h2>
        <table>
            <tr>
                <th>Tipo</th>
                <th>Número</th>
            </tr>
            <tr>
                <td>LICENÇA PRÉVIA</td>
                <td>111</td>
            </tr>
            <tr>
                <td>LICENÇA INSTALAÇÃO</td>
                <td>222</td>
            </tr>
        </table>
    </body>
    </html>
    """


def test_no_cadri_found(mock_response_no_cadri):
    """Test behavior when no CADRI documents are found"""
    scraper = DetailScraper()
    soup = BeautifulSoup(mock_response_no_cadri, 'html.parser')
    company_info = {'cnpj': '12345678000190'}

    docs = scraper.extract_cadri_documents(soup, company_info)

    assert len(docs) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])