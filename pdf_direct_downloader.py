#!/usr/bin/env python3
"""
PDF Direct Downloader - Download PDFs usando o padrão descoberto de URLs
"""

import asyncio
import sys
from pathlib import Path
import httpx
import hashlib
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from logging_conf import logger
from pdf_url_builder import build_pdf_url, build_pdf_url_with_fallback, get_default_idocmn
from store_csv import CSVStore
from config import PDF_DIR, USER_AGENT, CSV_CADRI_DOCS


class PDFDirectDownloader:
    """Download PDFs usando o padrão descoberto de URLs"""

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
            'already_exists': 0
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calcula hash MD5 de um arquivo"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5.update(chunk)
        return md5.hexdigest()

    async def download_pdf(
        self,
        numero_documento: str,
        idocmn: str,
        data_emissao: str,
        max_versions: int = 3
    ) -> bool:
        """
        Tenta baixar PDF usando o padrão descoberto

        Args:
            numero_documento: Número do documento
            idocmn: ID do tipo de documento
            data_emissao: Data de emissão
            max_versions: Número máximo de versões para tentar

        Returns:
            True se sucesso, False se falha
        """
        pdf_path = self.pdf_dir / f"{numero_documento}.pdf"

        # Skip if already exists
        if pdf_path.exists():
            logger.info(f"PDF já existe: {numero_documento}")
            self.stats['already_exists'] += 1
            return True

        # Gerar URLs possíveis
        urls = build_pdf_url_with_fallback(
            idocmn, numero_documento, data_emissao, max_versions
        )

        if not urls:
            logger.error(f"Não foi possível gerar URLs para {numero_documento}")
            self.stats['failed'] += 1
            return False

        # Tentar cada URL
        for i, url in enumerate(urls, 1):
            try:
                logger.info(f"Tentando download ({i}/{len(urls)}): {url}")

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
                    logger.warning(f"Resposta não parece ser PDF para {numero_documento}")
                    continue

                # Check if content is valid PDF (starts with %PDF)
                if not response.content.startswith(b'%PDF'):
                    logger.warning(f"Conteúdo não é PDF válido para {numero_documento}")
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
        logger.error(f"❌ Falha no download de {numero_documento} após {len(urls)} tentativas")
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

    async def download_batch(self, documents: List[Dict]) -> Dict[str, int]:
        """
        Download batch de documentos

        Args:
            documents: Lista de dicionários com informações dos documentos

        Returns:
            Estatísticas do download
        """
        for i, doc in enumerate(documents, 1):
            numero = doc.get('numero_documento')
            data = doc.get('data_emissao')
            tipo = doc.get('tipo_documento', 'DOCUMENTO')

            if not numero:
                logger.warning(f"Documento sem número, pulando...")
                self.stats['skipped'] += 1
                continue

            if not data:
                logger.warning(f"Documento {numero} sem data, pulando...")
                self.stats['skipped'] += 1
                continue

            # Determinar idocmn
            idocmn = get_default_idocmn(tipo)

            logger.info(f"[{i}/{len(documents)}] Processando: {numero}")

            # Tentar download
            await self.download_pdf(numero, idocmn, data)

            # Rate limiting
            if i < len(documents):
                await asyncio.sleep(1.5)  # Delay entre downloads

        return self.stats

    async def download_all_pending(self) -> Dict[str, int]:
        """Download todos os PDFs pendentes do CSV"""
        try:
            # Ler documentos do CSV
            df = pd.read_csv(CSV_CADRI_DOCS)

            # Filtrar pendentes
            pending = df[
                (df['status_pdf'].isna()) |
                (df['status_pdf'] == 'pending') |
                (df['status_pdf'] == '')
            ]

            if pending.empty:
                logger.info("Nenhum PDF pendente para download")
                return self.stats

            logger.info(f"📥 {len(pending)} PDFs pendentes para download")

            # Converter para lista de dicts
            documents = pending.to_dict('records')

            # Download batch
            stats = await self.download_batch(documents)

            return stats

        except Exception as e:
            logger.error(f"Erro ao ler documentos: {e}")
            return self.stats


async def main():
    """Função principal"""
    logger.info("=" * 60)
    logger.info("PDF Direct Downloader - Usando padrão descoberto")
    logger.info("=" * 60)

    async with PDFDirectDownloader() as downloader:
        stats = await downloader.download_all_pending()

        # Mostrar estatísticas
        logger.info("=" * 60)
        logger.info("📊 Estatísticas finais:")
        logger.info(f"   ✅ Sucesso: {stats['success']}")
        logger.info(f"   ❌ Falhas: {stats['failed']}")
        logger.info(f"   ⏭️  Pulados: {stats['skipped']}")
        logger.info(f"   📁 Já existentes: {stats['already_exists']}")
        logger.info(f"   📄 Total processado: {sum(stats.values())}")
        logger.info("=" * 60)

    return stats


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Download interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)