#!/usr/bin/env python3
"""
CNPJ Loader Module

Carrega e valida CNPJs de arquivos XLSX para consulta no sistema CETESB.
"""

import re
from pathlib import Path
from typing import List, Optional
import pandas as pd
from logging_conf import logger


class CNPJLoader:
    """Carregador e validador de CNPJs de arquivos XLSX"""

    def __init__(self):
        self.cnpj_pattern = re.compile(r'^\d{14}$')

    def validate_cnpj(self, cnpj: str) -> bool:
        """
        Valida formato de CNPJ (14 dígitos)

        Args:
            cnpj: String do CNPJ a ser validado

        Returns:
            True se válido, False caso contrário
        """
        if not cnpj or not isinstance(cnpj, str):
            return False

        # Remove pontuação e espaços
        clean_cnpj = re.sub(r'[^\d]', '', cnpj.strip())

        # Verifica se tem 14 dígitos
        return self.cnpj_pattern.match(clean_cnpj) is not None

    def normalize_cnpj(self, cnpj: str) -> str:
        """
        Normaliza CNPJ removendo pontuação

        Args:
            cnpj: CNPJ com ou sem formatação

        Returns:
            CNPJ com apenas dígitos (14 caracteres)
        """
        if not cnpj:
            return ""

        # Remove tudo que não for dígito
        clean_cnpj = re.sub(r'[^\d]', '', str(cnpj).strip())

        # Preenche com zeros à esquerda se necessário
        return clean_cnpj.zfill(14)

    def load_cnpjs_from_xlsx(self, file_path: Path, column_name: str = "cnpj") -> List[str]:
        """
        Carrega CNPJs de arquivo XLSX

        Args:
            file_path: Caminho para arquivo XLSX
            column_name: Nome da coluna com CNPJs (padrão: "cnpj")

        Returns:
            Lista de CNPJs válidos e normalizados

        Raises:
            FileNotFoundError: Se arquivo não existe
            ValueError: Se coluna não encontrada ou arquivo inválido
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        logger.info(f"Carregando CNPJs de: {file_path}")

        try:
            # Tentar ler arquivo Excel
            df = pd.read_excel(file_path, dtype=str)

            # Verificar se a coluna existe
            if column_name not in df.columns:
                available_columns = ", ".join(df.columns)
                raise ValueError(
                    f"Coluna '{column_name}' não encontrada. "
                    f"Colunas disponíveis: {available_columns}"
                )

            # Extrair CNPJs da coluna
            raw_cnpjs = df[column_name].dropna().astype(str).tolist()

            if not raw_cnpjs:
                logger.warning(f"Nenhum CNPJ encontrado na coluna '{column_name}'")
                return []

            logger.info(f"Encontrados {len(raw_cnpjs)} registros na coluna '{column_name}'")

        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo XLSX: {e}")

        # Validar e normalizar CNPJs
        valid_cnpjs = []
        invalid_cnpjs = []

        for i, raw_cnpj in enumerate(raw_cnpjs, start=1):
            normalized = self.normalize_cnpj(raw_cnpj)

            if self.validate_cnpj(normalized):
                valid_cnpjs.append(normalized)
            else:
                invalid_cnpjs.append(f"Linha {i}: '{raw_cnpj}'")

        # Remover duplicatas mantendo ordem
        unique_cnpjs = []
        seen = set()
        for cnpj in valid_cnpjs:
            if cnpj not in seen:
                unique_cnpjs.append(cnpj)
                seen.add(cnpj)

        # Log de resultados
        removed_duplicates = len(valid_cnpjs) - len(unique_cnpjs)

        logger.info(f"CNPJs processados:")
        logger.info(f"  - Válidos: {len(valid_cnpjs)}")
        logger.info(f"  - Únicos: {len(unique_cnpjs)}")
        if removed_duplicates > 0:
            logger.info(f"  - Duplicatas removidas: {removed_duplicates}")
        if invalid_cnpjs:
            logger.warning(f"  - Inválidos: {len(invalid_cnpjs)}")
            for invalid in invalid_cnpjs[:5]:  # Mostrar apenas primeiros 5
                logger.warning(f"    {invalid}")
            if len(invalid_cnpjs) > 5:
                logger.warning(f"    ... e mais {len(invalid_cnpjs) - 5} inválidos")

        return unique_cnpjs

    def validate_file(self, file_path: Path, column_name: str = "cnpj") -> dict:
        """
        Valida arquivo XLSX sem carregar todos os dados

        Args:
            file_path: Caminho para arquivo XLSX
            column_name: Nome da coluna com CNPJs

        Returns:
            Dict com informações de validação
        """
        result = {
            "valid": False,
            "exists": False,
            "readable": False,
            "has_column": False,
            "columns": [],
            "row_count": 0,
            "error": None
        }

        try:
            # Verificar se arquivo existe
            result["exists"] = file_path.exists()
            if not result["exists"]:
                result["error"] = f"Arquivo não encontrado: {file_path}"
                return result

            # Tentar ler apenas as primeiras linhas
            df = pd.read_excel(file_path, nrows=1)
            result["readable"] = True
            result["columns"] = df.columns.tolist()

            # Verificar se coluna existe
            result["has_column"] = column_name in df.columns
            if not result["has_column"]:
                result["error"] = f"Coluna '{column_name}' não encontrada"
                return result

            # Contar linhas totais
            df_full = pd.read_excel(file_path, usecols=[column_name])
            result["row_count"] = len(df_full.dropna())

            result["valid"] = True

        except Exception as e:
            result["error"] = str(e)

        return result


def validate_cnpj_file(file_path: str, column_name: str = "cnpj") -> bool:
    """
    Função utilitária para validar arquivo de CNPJs

    Args:
        file_path: Caminho para arquivo XLSX
        column_name: Nome da coluna com CNPJs

    Returns:
        True se arquivo é válido
    """
    loader = CNPJLoader()

    try:
        path = Path(file_path)
        validation = loader.validate_file(path, column_name)

        if validation["valid"]:
            logger.info(f"Arquivo válido: {validation['row_count']} CNPJs encontrados")
            return True
        else:
            logger.error(f"Arquivo inválido: {validation['error']}")
            return False

    except Exception as e:
        logger.error(f"Erro ao validar arquivo: {e}")
        return False


def load_cnpjs(file_path: str, column_name: str = "cnpj") -> List[str]:
    """
    Função utilitária para carregar CNPJs de arquivo

    Args:
        file_path: Caminho para arquivo XLSX
        column_name: Nome da coluna com CNPJs

    Returns:
        Lista de CNPJs válidos
    """
    loader = CNPJLoader()
    path = Path(file_path)
    return loader.load_cnpjs_from_xlsx(path, column_name)


# Exemplo de uso
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python cnpj_loader.py <arquivo.xlsx> [coluna]")
        sys.exit(1)

    file_path = sys.argv[1]
    column_name = sys.argv[2] if len(sys.argv) > 2 else "cnpj"

    # Validar arquivo
    if validate_cnpj_file(file_path, column_name):
        # Carregar CNPJs
        cnpjs = load_cnpjs(file_path, column_name)
        print(f"\nCNPJs carregados ({len(cnpjs)}):")
        for cnpj in cnpjs[:10]:  # Mostrar apenas primeiros 10
            print(f"  {cnpj}")
        if len(cnpjs) > 10:
            print(f"  ... e mais {len(cnpjs) - 10} CNPJs")
    else:
        print("❌ Arquivo inválido")
        sys.exit(1)