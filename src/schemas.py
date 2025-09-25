"""
Pydantic schemas for CADRI data extraction using LLM structured outputs
"""

from typing import List, Optional, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime


class EntidadeGeradora(BaseModel):
    """Dados da entidade geradora de resíduos"""
    nome: Optional[str] = Field(None, description="Razão social da entidade geradora")
    cadastro_cetesb: Optional[str] = Field(None, description="Número de cadastro CETESB")
    logradouro: Optional[str] = Field(None, description="Logradouro do endereço")
    numero: Optional[str] = Field(None, description="Número do endereço")
    complemento: Optional[str] = Field(None, description="Complemento do endereço")
    bairro: Optional[str] = Field(None, description="Bairro")
    cep: Optional[str] = Field(None, description="CEP")
    municipio: Optional[str] = Field(None, description="Município")
    uf: Optional[str] = Field(None, description="Unidade Federativa")
    atividade: Optional[str] = Field(None, description="Descrição da atividade")
    bacia_hidrografica: Optional[str] = Field(None, description="Bacia hidrográfica")
    funcionarios: Optional[str] = Field(None, description="Número de funcionários")


class EntidadeDestinacao(BaseModel):
    """Dados da entidade de destinação de resíduos"""
    nome: Optional[str] = Field(None, description="Razão social da entidade destinatária")
    cadastro_cetesb: Optional[str] = Field(None, description="Número de cadastro CETESB")
    logradouro: Optional[str] = Field(None, description="Logradouro do endereço")
    numero: Optional[str] = Field(None, description="Número do endereço")
    complemento: Optional[str] = Field(None, description="Complemento do endereço")
    bairro: Optional[str] = Field(None, description="Bairro")
    cep: Optional[str] = Field(None, description="CEP")
    municipio: Optional[str] = Field(None, description="Município")
    uf: Optional[str] = Field(None, description="Unidade Federativa")
    atividade: Optional[str] = Field(None, description="Atividade do destinatário")
    bacia_hidrografica: Optional[str] = Field(None, description="Bacia hidrográfica")
    licenca: Optional[str] = Field(None, description="Número da licença ambiental")
    data_licenca: Optional[str] = Field(None, description="Data de emissão da licença")


class DadosDocumento(BaseModel):
    """Metadados do documento CADRI"""
    numero_processo: Optional[str] = Field(None, description="Número do processo CETESB")
    numero_certificado: Optional[str] = Field(None, description="Número do certificado")
    versao_documento: Optional[str] = Field(None, description="Versão do documento")
    data_documento: Optional[str] = Field(None, description="Data de emissão do documento")
    data_validade: Optional[str] = Field(None, description="Data de validade")
    tipo_documento: Optional[str] = Field(None, description="Tipo do documento (CADRI ou Certificado)")


class ItemResiduoCADRI(BaseModel):
    """Dados estruturados de um item de resíduo extraído de PDF CADRI"""

    # Identificação básica
    numero_documento: str = Field(..., description="Número do documento CADRI")
    item_numero: Optional[str] = Field(None, description="Número sequencial do item (01, 02, etc.)")

    # Identificação do resíduo
    numero_residuo: Optional[str] = Field(None, description="Código do resíduo (D099, F001, K001, etc.)")
    descricao_residuo: Optional[str] = Field(None, description="Descrição completa do resíduo")
    origem_residuo: Optional[str] = Field(None, description="Origem do resíduo")
    classe_residuo: Optional[str] = Field(None, description="Classe do resíduo (I, IIA, IIB, etc.)")
    estado_fisico: Optional[str] = Field(None, description="Estado físico (LÍQUIDO, SÓLIDO, GASOSO)")
    oii: Optional[str] = Field(None, description="Campo OII (Orgânico/Inorgânico)")

    # Características técnicas
    quantidade: Optional[str] = Field(None, description="Quantidade numérica")
    unidade: Optional[str] = Field(None, description="Unidade de medida (t/ano, kg, L, m³, etc.)")
    composicao_aproximada: Optional[str] = Field(None, description="Composição aproximada do resíduo")
    metodo_utilizado: Optional[str] = Field(None, description="Método utilizado para caracterização")
    cor_cheiro_aspecto: Optional[str] = Field(None, description="Características físicas (cor, cheiro, aspecto)")

    # Logística
    acondicionamento_codigos: Optional[str] = Field(None, description="Códigos de acondicionamento (E01,E04,E05)")
    acondicionamento_descricoes: Optional[str] = Field(None, description="Descrições do acondicionamento (Tambor, Tanque, etc.)")
    destino_codigo: Optional[str] = Field(None, description="Código de destino (T34, etc.)")
    destino_descricao: Optional[str] = Field(None, description="Descrição do destino")

    # Metadados de extração
    pagina_origem: Optional[str] = Field(None, description="Página do PDF onde foi encontrado")
    raw_fragment: Optional[str] = Field(None, description="Fragmento de texto original extraído")

    # Dados das entidades (nested objects)
    entidade_geradora: Optional[EntidadeGeradora] = Field(None, description="Dados da entidade geradora")
    entidade_destinacao: Optional[EntidadeDestinacao] = Field(None, description="Dados da entidade de destinação")

    # Dados do documento
    dados_documento: Optional[DadosDocumento] = Field(None, description="Metadados do documento")


class CADRIExtractionResult(BaseModel):
    """Resultado completo da extração de um PDF CADRI"""

    numero_documento: str = Field(..., description="Número do documento CADRI processado")
    total_items: int = Field(..., description="Total de itens de resíduo encontrados")
    items: List[ItemResiduoCADRI] = Field(..., description="Lista de itens extraídos")
    extraction_method: str = Field(default="llm", description="Método de extração usado")
    processed_at: Optional[datetime] = Field(default_factory=datetime.now, description="Timestamp do processamento")

    @validator('processed_at', pre=True, always=True)
    def validate_processed_at(cls, v):
        """Validator for processed_at with fallback to current datetime"""
        if v is None:
            return datetime.now()
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            # Try to fix common datetime format issues
            import re
            # Fix double colons: T12::00 -> T12:00:00
            v = re.sub(r'T(\d{1,2})::(\d{2})', r'T\1:\2:00', v)
            # Add seconds if missing: T12:00 -> T12:00:00
            v = re.sub(r'T(\d{1,2}):(\d{2})$', r'T\1:\2:00', v)
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                # If parsing fails, return current datetime
                return datetime.now()
        return datetime.now()

    class Config:
        """Configuração do modelo Pydantic"""
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


def flatten_item_to_dict(item: ItemResiduoCADRI) -> dict:
    """
    Converte um ItemResiduoCADRI para dicionário plano compatível com CSV
    seguindo o schema CSVSchemas.CADRI_ITEMS_COLS
    """
    result = {
        # Campos básicos
        'numero_documento': item.numero_documento,
        'item_numero': item.item_numero,
        'numero_residuo': item.numero_residuo,
        'descricao_residuo': item.descricao_residuo,
        'origem_residuo': item.origem_residuo,
        'classe_residuo': item.classe_residuo,
        'estado_fisico': item.estado_fisico,
        'oii': item.oii,
        'quantidade': item.quantidade,
        'unidade': item.unidade,
        'composicao_aproximada': item.composicao_aproximada,
        'metodo_utilizado': item.metodo_utilizado,
        'cor_cheiro_aspecto': item.cor_cheiro_aspecto,
        'acondicionamento_codigos': item.acondicionamento_codigos,
        'acondicionamento_descricoes': item.acondicionamento_descricoes,
        'destino_codigo': item.destino_codigo,
        'destino_descricao': item.destino_descricao,
        'pagina_origem': item.pagina_origem,
        'raw_fragment': item.raw_fragment,

        # Dados da entidade geradora
        'geradora_nome': item.entidade_geradora.nome if item.entidade_geradora else None,
        'geradora_cadastro_cetesb': item.entidade_geradora.cadastro_cetesb if item.entidade_geradora else None,
        'geradora_logradouro': item.entidade_geradora.logradouro if item.entidade_geradora else None,
        'geradora_numero': item.entidade_geradora.numero if item.entidade_geradora else None,
        'geradora_complemento': item.entidade_geradora.complemento if item.entidade_geradora else None,
        'geradora_bairro': item.entidade_geradora.bairro if item.entidade_geradora else None,
        'geradora_cep': item.entidade_geradora.cep if item.entidade_geradora else None,
        'geradora_municipio': item.entidade_geradora.municipio if item.entidade_geradora else None,
        'geradora_uf': item.entidade_geradora.uf if item.entidade_geradora else None,
        'geradora_atividade': item.entidade_geradora.atividade if item.entidade_geradora else None,
        'geradora_bacia_hidrografica': item.entidade_geradora.bacia_hidrografica if item.entidade_geradora else None,
        'geradora_funcionarios': item.entidade_geradora.funcionarios if item.entidade_geradora else None,

        # Dados da entidade de destinação
        'destino_entidade_nome': item.entidade_destinacao.nome if item.entidade_destinacao else None,
        'destino_entidade_cadastro_cetesb': item.entidade_destinacao.cadastro_cetesb if item.entidade_destinacao else None,
        'destino_entidade_logradouro': item.entidade_destinacao.logradouro if item.entidade_destinacao else None,
        'destino_entidade_numero': item.entidade_destinacao.numero if item.entidade_destinacao else None,
        'destino_entidade_complemento': item.entidade_destinacao.complemento if item.entidade_destinacao else None,
        'destino_entidade_bairro': item.entidade_destinacao.bairro if item.entidade_destinacao else None,
        'destino_entidade_cep': item.entidade_destinacao.cep if item.entidade_destinacao else None,
        'destino_entidade_municipio': item.entidade_destinacao.municipio if item.entidade_destinacao else None,
        'destino_entidade_uf': item.entidade_destinacao.uf if item.entidade_destinacao else None,
        'destino_entidade_atividade': item.entidade_destinacao.atividade if item.entidade_destinacao else None,
        'destino_entidade_bacia_hidrografica': item.entidade_destinacao.bacia_hidrografica if item.entidade_destinacao else None,
        'destino_entidade_licenca': item.entidade_destinacao.licenca if item.entidade_destinacao else None,
        'destino_entidade_data_licenca': item.entidade_destinacao.data_licenca if item.entidade_destinacao else None,

        # Dados do documento
        'numero_processo': item.dados_documento.numero_processo if item.dados_documento else None,
        'numero_certificado': item.dados_documento.numero_certificado if item.dados_documento else None,
        'versao_documento': item.dados_documento.versao_documento if item.dados_documento else None,
        'data_documento': item.dados_documento.data_documento if item.dados_documento else None,
        'data_validade': item.dados_documento.data_validade if item.dados_documento else None,
        'tipo_documento': item.dados_documento.tipo_documento if item.dados_documento else None,

        # Timestamp
        'updated_at': datetime.now().isoformat()
    }

    return result