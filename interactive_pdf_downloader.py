#!/usr/bin/env python3
"""
Interactive PDF Downloader - Download PDFs usando automação Playwright como fallback
"""

import asyncio
import sys
from pathlib import Path
import random
import hashlib
import pandas as pd
from typing import Optional, List, Dict
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from browser import BrowserManager, FormHelper
from logging_conf import logger
from config import PDF_DIR, CSV_CADRI_DOCS, RATE_MIN, RATE_MAX
from pdf_url_builder import parse_autenticidade_url


class InteractivePDFDownloader:
    """Download PDFs usando automação Playwright para casos onde download direto falha"""

    def __init__(self):
        self.browser_manager = BrowserManager()
        self.pdf_dir = PDF_DIR
        self.pdf_dir.mkdir(exist_ok=True)
        self.download_timeout = 30000  # 30 segundos
        self.consultar_wait = 4000  # 4 segundos após clicar Consultar
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'already_exists': 0,
            'no_consultar_button': 0,
            'no_visualize_button': 0,
            'timeout': 0,
            'error': 0
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'browser_manager'):
            await self.browser_manager.__aexit__(exc_type, exc_val, exc_tb)

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calcula hash MD5 de um arquivo"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5.update(chunk)
        return md5.hexdigest()

    async def download_pdf_interactive(
        self,
        url: str,
        numero_documento: str
    ) -> bool:
        """
        Download de um PDF via interação com a página

        Args:
            url: URL de autenticidade do CETESB
            numero_documento: Número do documento para nome do arquivo

        Returns:
            True se sucesso, False se falha
        """
        page = None
        try:
            # Verificar se PDF já existe
            pdf_path = self.pdf_dir / f"{numero_documento}.pdf"
            if pdf_path.exists():
                logger.info(f"PDF já existe: {numero_documento}")
                self.stats['already_exists'] += 1
                return True

            # Criar nova página
            async with self.browser_manager as browser:
                page = await browser.new_page()

                # Navegar para URL
                logger.info(f"Navegando para: {url}")
                await page.goto(url, wait_until='networkidle')

                # Aguardar página carregar
                await page.wait_for_load_state('domcontentloaded')

                # Procurar e clicar botão "Consultar"
                consultar_button = await self._find_consultar_button(page)
                if consultar_button:
                    logger.info("Clicando botão 'Consultar'...")
                    await consultar_button.click()

                    # Aguardar processamento
                    logger.info(f"Aguardando {self.consultar_wait}ms...")
                    await page.wait_for_timeout(self.consultar_wait)

                    # Aguardar seção "Obtenha uma cópia" aparecer
                    try:
                        await page.wait_for_selector(
                            "text=Obtenha uma cópia",
                            timeout=10000
                        )
                    except:
                        logger.warning("Seção 'Obtenha uma cópia' não encontrada")

                    # Procurar link "Visualize" para PDF
                    visualize_link = await self._find_visualize_link(page)
                    if visualize_link:
                        logger.info("Clicando em 'Visualize'...")

                        # Preparar para download
                        async with page.expect_download() as download_info:
                            await visualize_link.click()

                        # Processar download
                        download = await download_info.value
                        await download.save_as(pdf_path)

                        # Verificar se é PDF válido
                        if pdf_path.exists() and pdf_path.stat().st_size > 0:
                            with open(pdf_path, 'rb') as f:
                                if f.read(4).startswith(b'%PDF'):
                                    file_hash = self._calculate_file_hash(pdf_path)
                                    logger.info(f"✅ PDF baixado: {numero_documento} ({pdf_path.stat().st_size:,} bytes)")
                                    self._update_pdf_status(numero_documento, 'downloaded', file_hash)
                                    self.stats['success'] += 1
                                    return True

                        # Se chegou aqui, download falhou
                        if pdf_path.exists():
                            pdf_path.unlink()  # Remove arquivo inválido

                        logger.error(f"Download inválido para {numero_documento}")
                        self._update_pdf_status(numero_documento, 'invalid_download')
                        self.stats['failed'] += 1
                        return False
                    else:
                        logger.error("Link 'Visualize' não encontrado")
                        self._update_pdf_status(numero_documento, 'no_visualize_button')
                        self.stats['no_visualize_button'] += 1
                        return False
                else:
                    logger.error("Botão 'Consultar' não encontrado")
                    self._update_pdf_status(numero_documento, 'no_consultar_button')
                    self.stats['no_consultar_button'] += 1
                    return False

        except asyncio.TimeoutError:
            logger.error(f"Timeout no download de {numero_documento}")
            self._update_pdf_status(numero_documento, 'timeout')
            self.stats['timeout'] += 1
            return False
        except Exception as e:
            logger.error(f"Erro no download de {numero_documento}: {e}")
            self._update_pdf_status(numero_documento, 'error')
            self.stats['error'] += 1
            return False

    async def _find_consultar_button(self, page):
        """Localizar botão Consultar com múltiplas estratégias"""
        selectors = [
            "input[type='submit'][value*='Consulte']",
            "input[type='submit'][value*='Consultar']",
            "button:has-text('Consultar')",
            "input[value='Consulte ...']",
            "input[type='submit'][value='Consulte']"
        ]

        for selector in selectors:
            try:
                button = await page.query_selector(selector)
                if button:
                    return button
            except:
                continue
        return None

    async def _find_visualize_link(self, page):
        """Localizar link Visualize para PDF"""
        selectors = [
            "a:has-text('Visualize')",
            "a[href*='Adobe']:has-text('Visualize')",
            "td:has-text('Imagem da Licença') ~ td a",
            "a[onclick*='window.open']",
            "a[href*='pdf']"
        ]

        for selector in selectors:
            try:
                link = await page.query_selector(selector)
                if link:
                    return link
            except:
                continue
        return None

    def _update_pdf_status(
        self,
        numero_documento: str,
        status: str,
        pdf_hash: Optional[str] = None
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
                df.loc[mask, 'updated_at'] = datetime.now().isoformat()

                # Salvar
                df.to_csv(CSV_CADRI_DOCS, index=False)
                logger.debug(f"Status atualizado: {numero_documento} -> {status}")
        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")

    async def download_failed_documents(self, doc_type: str = None) -> Dict[str, int]:
        """
        Download documentos que falharam no download direto

        Args:
            doc_type: Filtrar por tipo de documento (opcional)

        Returns:
            Estatísticas do download
        """
        try:
            # Import the date filtering function
            from store_csv import filter_by_date_cutoff

            # Ler documentos do CSV
            df = pd.read_csv(CSV_CADRI_DOCS)

            # Apply date filter first (7 years cutoff)
            df = filter_by_date_cutoff(df, years_cutoff=7)

            # Filtrar documentos que falharam no download direto
            failed_statuses = ['not_found', 'timeout', 'error', 'invalid_download']
            failed_docs = df[df['status_pdf'].isin(failed_statuses)]

            # Filtrar por tipo se especificado
            if doc_type:
                failed_docs = failed_docs[failed_docs['tipo_documento'] == doc_type]

            # Filtrar apenas com URLs válidas
            failed_docs = failed_docs[
                failed_docs['url_pdf'].notna() &
                (failed_docs['url_pdf'] != '') &
                (failed_docs['url_pdf'] != 'pending')
            ]

            if failed_docs.empty:
                logger.info("Nenhum documento com falha para reprocessar")
                return self.stats

            logger.info(f"📥 {len(failed_docs)} documentos com falha para reprocessar")

            # Converter para lista de dicts
            documents = failed_docs.to_dict('records')

            # Download batch
            for i, doc in enumerate(documents, 1):
                numero = doc.get('numero_documento')
                url = doc.get('url_pdf')

                logger.info(f"[{i}/{len(documents)}] Reprocessando: {numero}")

                # Tentar download interativo
                await self.download_pdf_interactive(url, numero)

                # Rate limiting
                if i < len(documents):
                    delay = RATE_MIN + (RATE_MAX - RATE_MIN) * random.random()
                    await asyncio.sleep(delay)

            return self.stats

        except Exception as e:
            logger.error(f"Erro ao processar documentos com falha: {e}")
            return self.stats

    async def download_all_pending(self, doc_type: str = None) -> Dict[str, int]:
        """
        Download todos os PDFs pendentes

        Args:
            doc_type: Filtrar por tipo de documento (opcional)

        Returns:
            Estatísticas do download
        """
        try:
            # Ler documentos do CSV
            df = pd.read_csv(CSV_CADRI_DOCS)

            # Filtrar pendentes
            pending = df[
                (df['status_pdf'].isna()) |
                (df['status_pdf'] == 'pending') |
                (df['status_pdf'] == '')
            ]

            # Filtrar por tipo se especificado
            if doc_type:
                pending = pending[pending['tipo_documento'] == doc_type]

            # Filtrar apenas com URLs válidas
            pending = pending[
                pending['url_pdf'].notna() &
                (pending['url_pdf'] != '') &
                (pending['url_pdf'] != 'pending')
            ]

            if pending.empty:
                logger.info("Nenhum PDF pendente para download")
                return self.stats

            logger.info(f"📥 {len(pending)} PDFs pendentes para download")

            # Converter para lista de dicts
            documents = pending.to_dict('records')

            # Download batch
            for i, doc in enumerate(documents, 1):
                numero = doc.get('numero_documento')
                url = doc.get('url_pdf')

                logger.info(f"[{i}/{len(documents)}] Processando: {numero}")

                # Tentar download interativo
                await self.download_pdf_interactive(url, numero)

                # Rate limiting
                if i < len(documents):
                    delay = RATE_MIN + (RATE_MAX - RATE_MIN) * random.random()
                    await asyncio.sleep(delay)

            return self.stats

        except Exception as e:
            logger.error(f"Erro ao processar documentos: {e}")
            return self.stats


async def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(description='Interactive PDF Downloader')
    parser.add_argument(
        '--type',
        type=str,
        help='Filtrar por tipo de documento (ex: CERT MOV RESIDUOS INT AMB)'
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Reprocessar apenas documentos que falharam'
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("Interactive PDF Downloader - Playwright Automation")
    if args.type:
        logger.info(f"Tipo: {args.type}")
    if args.retry_failed:
        logger.info("Modo: Reprocessar falhas")
    logger.info("=" * 70)

    async with InteractivePDFDownloader() as downloader:
        if args.retry_failed:
            stats = await downloader.download_failed_documents(args.type)
        else:
            stats = await downloader.download_all_pending(args.type)

        # Mostrar estatísticas
        logger.info("=" * 70)
        logger.info("📊 Estatísticas finais:")
        logger.info(f"   Sucesso: {stats['success']}")
        logger.info(f"   Falhas: {stats['failed']}")
        logger.info(f"   Pulados: {stats['skipped']}")
        logger.info(f"   Já existentes: {stats['already_exists']}")
        logger.info(f"   Sem botão Consultar: {stats['no_consultar_button']}")
        logger.info(f"   Sem link Visualize: {stats['no_visualize_button']}")
        logger.info(f"   Timeout: {stats['timeout']}")
        logger.info(f"   Erro: {stats['error']}")
        logger.info(f"   Total processado: {sum(stats.values())}")
        logger.info("=" * 70)

        # Taxa de sucesso
        processados = stats['success'] + stats['failed'] + stats['no_consultar_button'] + stats['no_visualize_button'] + stats['timeout'] + stats['error']
        if processados > 0:
            taxa_sucesso = (stats['success'] / processados) * 100
            logger.info(f"   📈 Taxa de sucesso: {taxa_sucesso:.1f}%")

    return stats


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Download interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)