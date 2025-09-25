#!/usr/bin/env python3
"""
CERT MOV RESIDUOS Direct Downloader - Download direto usando padrão descoberto
"""

import asyncio
import sys
from pathlib import Path
import httpx
import hashlib
import pandas as pd
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from logging_conf import logger
from config import PDF_DIR, USER_AGENT, CSV_CADRI_DOCS


class CertMovDirectDownloader:
    """Download direto de documentos CERT MOV RESIDUOS INT AMB"""

    TARGET_DOC_TYPE = 'CERT MOV RESIDUOS INT AMB'

    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.pdf_dir.mkdir(exist_ok=True)
        self.client = httpx.AsyncClient(
            headers={'User-Agent': USER_AGENT},
            timeout=60.0,
            follow_redirects=True
        )
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'already_exists': 0,
            'no_url': 0,
            'parse_error': 0
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    def parse_auth_url(self, url: str) -> Optional[Tuple[str, str]]:
        """
        Extrai idocmn e ndocmn de uma URL de autenticidade

        Args:
            url: URL de autenticidade (ex: autentica.php?idocmn=27&ndocmn=16000520)

        Returns:
            Tupla (idocmn, ndocmn) ou None se falhar
        """
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)

            idocmn = params.get('idocmn', [None])[0]
            ndocmn = params.get('ndocmn', [None])[0]

            if idocmn and ndocmn:
                return (idocmn, ndocmn)
        except Exception as e:
            logger.debug(f"Erro ao fazer parse da URL {url}: {e}")

        return None

    def format_date_ddmmyyyy(self, date_str: str) -> Optional[str]:
        """
        Converte data para formato DDMMAAAA

        Args:
            date_str: Data em formato variado

        Returns:
            Data no formato DDMMAAAA ou None
        """
        if not date_str:
            return None

        # Remover espaços
        date_str = date_str.strip()

        # Tentar diferentes formatos
        formats = [
            ('%Y-%m-%d', '%d%m%Y'),  # 2010-11-09
            ('%d/%m/%Y', '%d%m%Y'),  # 09/11/2010
            ('%d-%m-%Y', '%d%m%Y'),  # 09-11-2010
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
        self,
        idocmn: str,
        ndocmn: str,
        data_desde: str,
        versao: str = "01"
    ) -> Optional[str]:
        """
        Constrói URL do PDF usando o padrão descoberto

        Args:
            idocmn: ID do tipo de documento
            ndocmn: Número do documento
            data_desde: Data desde (campo correto para código PDF)
            versao: Versão do documento

        Returns:
            URL do PDF ou None
        """
        # Formatar data
        data_formatted = self.format_date_ddmmyyyy(data_desde)
        if not data_formatted:
            return None

        # Construir código
        codigo = f"{idocmn}{ndocmn.zfill(8)}{versao}{data_formatted}"

        # Retornar URL
        return f"https://autenticidade.cetesb.sp.gov.br/pdf/{codigo}.pdf"

    def build_pdf_urls_with_versions(
        self,
        idocmn: str,
        ndocmn: str,
        data_desde: str,
        max_versions: int = 3
    ) -> List[str]:
        """
        Constrói lista de URLs com diferentes versões

        Args:
            idocmn: ID do tipo de documento
            ndocmn: Número do documento
            data_desde: Data desde (campo correto para código PDF)
            max_versions: Número máximo de versões

        Returns:
            Lista de URLs para tentar
        """
        urls = []

        for version in range(1, max_versions + 1):
            versao_str = str(version).zfill(2)  # 01, 02, 03...
            url = self.build_pdf_url(idocmn, ndocmn, data_desde, versao_str)
            if url:
                urls.append(url)

        return urls

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calcula hash MD5 de um arquivo"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5.update(chunk)
        return md5.hexdigest()

    async def download_pdf(self, doc_info: Dict) -> bool:
        """
        Tenta baixar PDF de um documento

        Args:
            doc_info: Dicionário com informações do documento

        Returns:
            True se sucesso, False se falha
        """
        numero_documento = doc_info.get('numero_documento')
        url_pdf = doc_info.get('url_pdf')
        data_desde = doc_info.get('data_desde')

        if not numero_documento:
            logger.warning("Documento sem número, pulando...")
            self.stats['skipped'] += 1
            return False

        # Verificar se PDF já existe
        pdf_path = self.pdf_dir / f"{numero_documento}.pdf"
        if pdf_path.exists():
            logger.info(f"PDF já existe: {numero_documento}")
            self.stats['already_exists'] += 1
            return True

        # Verificar se tem URL
        if not url_pdf or pd.isna(url_pdf):
            logger.warning(f"Documento {numero_documento} sem URL, pulando...")
            self.stats['no_url'] += 1
            return False

        # Parse da URL para extrair parâmetros
        parsed = self.parse_auth_url(url_pdf)
        if not parsed:
            logger.error(f"Erro ao fazer parse da URL: {url_pdf}")
            self.stats['parse_error'] += 1
            return False

        idocmn, ndocmn = parsed

        # Verificar se tem data
        if not data_desde or pd.isna(data_desde):
            logger.warning(f"Documento {numero_documento} sem data_desde, pulando...")
            self.stats['skipped'] += 1
            return False

        # Gerar URLs possíveis
        urls = self.build_pdf_urls_with_versions(idocmn, ndocmn, data_desde)

        if not urls:
            logger.error(f"Não foi possível gerar URLs para {numero_documento}")
            self.stats['failed'] += 1
            return False

        # Tentar cada URL
        for i, url in enumerate(urls, 1):
            try:
                logger.info(f"Tentando download ({i}/{len(urls)}): {numero_documento}")
                logger.debug(f"URL: {url}")

                # Download PDF
                response = await self.client.get(url)

                # Check status
                if response.status_code == 404:
                    logger.debug(f"PDF não encontrado (404): versão {i:02d}")
                    continue

                response.raise_for_status()

                # Check if response is PDF
                content_type = response.headers.get('content-type', '')
                if 'pdf' not in content_type.lower() and len(response.content) < 1000:
                    logger.debug(f"Resposta não parece ser PDF: {content_type}")
                    continue

                # Check if content is valid PDF
                if not response.content.startswith(b'%PDF'):
                    logger.debug(f"Conteúdo não é PDF válido")
                    continue

                # Save PDF
                pdf_path.write_bytes(response.content)
                file_size = len(response.content)
                file_hash = self._calculate_file_hash(pdf_path)

                logger.info(f"✅ PDF baixado: {numero_documento} ({file_size:,} bytes, versão {i:02d})")

                # Update CSV status
                self._update_pdf_status(numero_documento, 'downloaded', file_hash, url)
                self.stats['success'] += 1
                return True

            except httpx.HTTPStatusError as e:
                if e.response.status_code != 404:
                    logger.error(f"HTTP error {e.response.status_code} para {numero_documento}: {e}")
            except Exception as e:
                logger.error(f"Erro no download de {numero_documento}: {e}")

        # Se chegou aqui, todas as tentativas falharam
        logger.warning(f"❌ Falha no download de {numero_documento} após {len(urls)} tentativas")
        self._update_pdf_status(numero_documento, 'not_found')
        self.stats['failed'] += 1
        return False

    def _update_pdf_status(
        self,
        numero_documento: str,
        status: str,
        pdf_hash: Optional[str] = None,
        url_used: Optional[str] = None
    ):
        """Atualiza status do PDF no CSV"""
        try:
            df = pd.read_csv(CSV_CADRI_DOCS)

            # Encontrar documento
            mask = df['numero_documento'] == numero_documento
            if mask.any():
                df.loc[mask, 'status_pdf'] = status
                if pdf_hash:
                    df.loc[mask, 'pdf_hash'] = pdf_hash
                if url_used:
                    df.loc[mask, 'url_pdf_real'] = url_used
                df.loc[mask, 'updated_at'] = datetime.now().isoformat()

                # Salvar
                df.to_csv(CSV_CADRI_DOCS, index=False)
                logger.debug(f"Status atualizado: {numero_documento} -> {status}")
        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")

    async def download_cert_mov_documents(self, test_mode: bool = False) -> Dict[str, int]:
        """
        Download todos os documentos CERT MOV RESIDUOS INT AMB

        Args:
            test_mode: Se True, processa apenas os primeiros 5 documentos

        Returns:
            Estatísticas do download
        """
        try:
            # Import the date filtering function
            import sys
            from pathlib import Path
            sys.path.append(str(Path(__file__).parent / "src"))
            from store_csv import filter_by_date_cutoff

            # Ler documentos do CSV
            df = pd.read_csv(CSV_CADRI_DOCS)

            # Filtrar por tipo
            cert_mov = df[df['tipo_documento'] == self.TARGET_DOC_TYPE]
            logger.info(f"Total documentos {self.TARGET_DOC_TYPE}: {len(cert_mov)}")

            # Apply date filter (7 years cutoff)
            cert_mov = filter_by_date_cutoff(cert_mov, years_cutoff=7)

            # Filtrar com URLs válidas
            with_urls = cert_mov[
                cert_mov['url_pdf'].notna() &
                (cert_mov['url_pdf'] != '') &
                (cert_mov['url_pdf'] != 'pending')
            ]
            logger.info(f"Com URLs válidas: {len(with_urls)}")

            if with_urls.empty:
                logger.warning("Nenhum documento com URL válida!")
                return self.stats

            # Modo teste - apenas primeiros documentos
            if test_mode:
                with_urls = with_urls.head(5)
                logger.info(f"MODO TESTE: Processando apenas {len(with_urls)} documentos")

            # Converter para lista de dicts
            documents = with_urls.to_dict('records')

            # Download batch
            for i, doc in enumerate(documents, 1):
                numero = doc.get('numero_documento')
                logger.info(f"[{i}/{len(documents)}] Processando: {numero}")

                # Tentar download
                await self.download_pdf(doc)

                # Rate limiting
                if i < len(documents):
                    await asyncio.sleep(1.0)  # 1 segundo entre downloads

            return self.stats

        except Exception as e:
            logger.error(f"Erro ao processar documentos: {e}")
            return self.stats


async def main():
    """Função principal"""
    logger.info("=" * 70)
    logger.info("CERT MOV RESIDUOS INT AMB - Direct Downloader")
    logger.info("Usando padrão descoberto para download direto")
    logger.info("=" * 70)

    # Verificar se deve executar em modo teste
    test_mode = "--test" in sys.argv

    async with CertMovDirectDownloader() as downloader:
        stats = await downloader.download_cert_mov_documents(test_mode=test_mode)

        # Mostrar estatísticas
        logger.info("=" * 70)
        logger.info("📊 Estatísticas finais:")
        logger.info(f"   ✅ Sucesso: {stats['success']}")
        logger.info(f"   ❌ Falhas: {stats['failed']}")
        logger.info(f"   ⏭️  Pulados: {stats['skipped']}")
        logger.info(f"   📁 Já existentes: {stats['already_exists']}")
        logger.info(f"   🚫 Sem URL: {stats['no_url']}")
        logger.info(f"   ⚠️  Erro parse: {stats['parse_error']}")
        logger.info(f"   📄 Total processado: {sum(stats.values())}")
        logger.info("=" * 70)

        # Taxa de sucesso
        processados = stats['success'] + stats['failed']
        if processados > 0:
            taxa_sucesso = (stats['success'] / processados) * 100
            logger.info(f"   📈 Taxa de sucesso: {taxa_sucesso:.1f}%")

    return stats


if __name__ == "__main__":
    print("CERT MOV RESIDUOS Direct Downloader")
    print("Use --test para modo teste (apenas 5 documentos)")
    print()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Download interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)