# CETESB CADRI Data Extractor

Sistema automatizado para extrair dados cadastrais de empresas e informações técnicas detalhadas de documentos CADRI (Certificados de Movimentação de Resíduos de Interesse Ambiental) do site público da CETESB.

## 📈 Evolução do Projeto

### Versão 3.0 (Setembro 2025) - Expansão Completa
- **✨ Extração expandida**: De 15 para 47 campos estruturados por item
- **🏢 Dados das entidades**: Captura completa de informações de geradores e destinatários
- **📄 Metadados do documento**: Processo, certificado, versão e datas
- **🔧 Novos utilitários**: `cadri_utils.py`, `monitor_progress.py`
- **⚡ Parser otimizado**: Melhor reconhecimento de padrões em PDFs

### Versão 2.0 (Setembro 2025) - Pipeline Robusto
- **🔄 Pipeline completo**: 4 etapas automatizadas (list → detail → download → parse)
- **📥 Download direto**: Descoberta de padrões de URL para PDFs
- **🎯 Parser standalone**: Extração inicial de 15 campos técnicos
- **💾 CSV estruturado**: Persistência com deduplicação

### Versão 1.0 (Setembro 2025) - MVP
- **🌐 Web scraping**: Coleta de empresas e documentos CADRI
- **📊 Estrutura básica**: Dados cadastrais e técnicos essenciais

## Objetivo

Gerar **duas tabelas de dados estruturadas**:

1. **📋 Dados Cadastrais** (`empresas.csv`) - Informações de todas as empresas listadas no site CETESB
2. **🗂️ Dados Técnicos** (`cadri_itens.csv`) - Informações detalhadas extraídas dos documentos CADRI com 15 campos estruturados

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd cetesb-cadri-csv

# Instale dependências
pip install -r requirements.txt

# Instale Playwright browsers
playwright install chromium

# Configure ambiente
cp .env.example .env
```

## Uso Simplificado

### 1. Pipeline Completo (Coleta + Download + Parsing)

```bash
# Executar todas as etapas automaticamente
python -m src.pipeline --stage all
```

### 2. Etapas Individuais

```bash
# Etapa 1: Coletar empresas e documentos
python -m src.pipeline --stage list --seeds CEM,AGR,BIO
python -m src.pipeline --stage detail

# Etapa 2: Baixar PDFs
python cert_mov_direct_downloader.py

# Etapa 3: Extrair dados dos PDFs
python pdf_parser_standalone.py
```

### 3. Busca por CNPJs (Novo!)

```bash
# Pipeline completo com lista de CNPJs
python -m src.pipeline --stage all --cnpj-file empresas.xlsx

# Apenas busca por CNPJs específicos
python -m src.pipeline --stage list --cnpj-file lista_cnpjs.xlsx

# Combinado com outras etapas
python -m src.pipeline --stage list --cnpj-file empresas.xlsx
python -m src.pipeline --stage detail
```

## Arquitetura

```
Empresas → Documentos → PDFs → Dados Extraídos
   ↓           ↓         ↓         ↓
List    →   Detail  →  Download → Parse
```

### Componentes Principais

#### 🔧 Core Pipeline
- **`src/pipeline.py`**: Orquestrador principal com 4 etapas (list, detail, download, parse)
- **`src/scrape_list.py`**: Coleta empresas via Playwright (seeds + CNPJs)
- **`src/scrape_detail.py`**: Extrai documentos CADRI via httpx
- **`src/store_csv.py`**: Persistência CSV com schema de 47 campos
- **`src/cnpj_loader.py`**: Carregamento e validação de CNPJs via XLSX

#### 📥 Módulos de Download
- **`cert_mov_direct_downloader.py`**: Download direto otimizado com descoberta de URL patterns
- **`interactive_pdf_downloader.py`**: Fallback interativo com Playwright para casos especiais
- **`src/pdf_url_builder.py`**: Construção inteligente de URLs de PDFs

#### 📊 Parser e Análise
- **`pdf_parser_standalone.py`**: Extrator avançado com 47 campos estruturados
  - Extração de dados das entidades (geradora/destinação)
  - Parsing de características técnicas dos resíduos
  - Captura de metadados do documento

#### 🛠️ Utilitários
- **`monitor_progress.py`**: Dashboard de progresso em tempo real
  - Status por etapa do pipeline
  - Estatísticas de extração
  - Identificação de pendências
- **`cadri_utils.py`**: Ferramentas de gerenciamento
  - Listagem de tipos de documentos
  - Validação de dados
  - Contagem e estatísticas

## Dados Extraídos

### empresas.csv (Dados Cadastrais)
- `cnpj` (chave primária)
- `razao_social`
- `logradouro`, `municipio`, `uf`, `cep`
- `numero_cadastro_cetesb`
- `descricao_atividade`

### cadri_itens.csv (Dados Técnicos Expandidos - 47 Campos)

**📋 Identificação do Resíduo (5 campos):**
- `numero_residuo` (D099, F001, K001, etc.)
- `descricao_residuo`
- `classe_residuo` (I, IIA, IIB)
- `estado_fisico` (LIQUIDO, SOLIDO, GASOSO)
- `item_numero` (ordem do item no documento)

**🔬 Características Técnicas (7 campos):**
- `quantidade`, `unidade` (t, kg, L, m³)
- `oii` (Orgânico/Inorgânico)
- `composicao_aproximada`
- `metodo_utilizado`
- `cor_cheiro_aspecto`
- `raw_fragment` (texto original extraído)

**📦 Logística (4 campos):**
- `acondicionamento_codigos` (E01,E04,E05)
- `acondicionamento_descricoes` (Tambor, Tanque, Bombonas)
- `destino_codigo` (T34, etc.)
- `destino_descricao`

**🏢 Entidade Geradora (13 campos):**
- `geradora_nome` - Razão social
- `geradora_cadastro_cetesb` - Número de cadastro
- `geradora_logradouro`, `geradora_numero`, `geradora_complemento`
- `geradora_bairro`, `geradora_cep`, `geradora_municipio`, `geradora_uf`
- `geradora_atividade` - Descrição da atividade
- `geradora_bacia_hidrografica` - Bacia hidrográfica
- `geradora_funcionarios` - Número de funcionários

**🚛 Entidade de Destinação (14 campos):**
- `destino_entidade_nome` - Razão social do destinatário
- `destino_entidade_cadastro_cetesb` - Número de cadastro
- `destino_entidade_logradouro`, `destino_entidade_numero`, `destino_entidade_complemento`
- `destino_entidade_bairro`, `destino_entidade_cep`, `destino_entidade_municipio`, `destino_entidade_uf`
- `destino_entidade_atividade` - Atividade do destinatário
- `destino_entidade_bacia_hidrografica` - Bacia hidrográfica
- `destino_entidade_licenca` - Número da licença ambiental
- `destino_entidade_data_licenca` - Data de emissão da licença

**📄 Dados do Documento (8 campos):**
- `numero_documento` - Número do CADRI
- `tipo_documento` - CADRI ou Certificado
- `numero_processo` - Número do processo CETESB
- `numero_certificado` - Número do certificado
- `versao_documento` - Versão do documento
- `data_documento` - Data de emissão
- `data_validade` - Data de validade
- `updated_at` - Timestamp da última atualização

## Monitoramento

```bash
# Dashboard de progresso completo
python monitor_progress.py

# Estatísticas detalhadas
python cadri_utils.py count         # Contagem por tipo de documento
python cadri_utils.py validate      # Validação de dados extraídos
python cadri_utils.py list-types    # Tipos de documentos disponíveis

# Verificar PDFs específicos
python pdf_parser_standalone.py --document 16000520
python pdf_parser_standalone.py --force-reparse  # Reprocessar todos
```

## Formato do Arquivo XLSX de CNPJs

Para usar a funcionalidade de busca por CNPJs, crie um arquivo Excel (.xlsx) com a seguinte estrutura:

| cnpj |
|------|
| 11222333000181 |
| 44555666000199 |
| 77888999000155 |

**Requisitos:**
- Coluna deve se chamar exatamente "cnpj" (minúsculo)
- CNPJs podem ter ou não formatação (pontos/barras são removidos automaticamente)
- CNPJs inválidos são ignorados com aviso no log
- Duplicatas são automaticamente removidas

**Exemplo de uso:**
```bash
# Salvar lista de CNPJs em empresas.xlsx
python -m src.pipeline --stage all --cnpj-file empresas.xlsx
```

## Configuração (.env)

```env
# Rate limiting (segundos)
RATE_MIN=0.6
RATE_MAX=1.4

# Browser settings
HEADLESS=true
BROWSER_TIMEOUT=30000

# Limites
MAX_PAGES=10
MAX_RETRIES=3
```

## Resultados Esperados

Com a configuração padrão, o sistema extrai:
- **~5.000+ empresas** cadastradas na CETESB
- **~15.000+ documentos** CADRI disponíveis
- **~50.000+ itens de resíduos** com informações técnicas detalhadas
- **47 campos estruturados** por item, incluindo:
  - Dados completos da entidade geradora
  - Informações detalhadas do destinatário
  - Metadados do processo e certificados
  - Características técnicas expandidas

## Características Técnicas

- **🔄 Operações idempotentes**: Pode ser interrompido e retomado
- **📊 Extração estruturada**: 47 campos técnicos expandidos por item
- **⚡ Download otimizado**: URL pattern discovery para downloads diretos
- **🛡️ Rate limiting**: Respeita limites do servidor
- **💾 Persistência CSV**: Dados estruturados prontos para análise
- **🎯 Parser inteligente**: Reconhecimento de padrões complexos em PDFs
- **📈 Monitoramento real-time**: Dashboard de progresso detalhado

## Limitações

1. **Sem API pública**: Sistema baseado em web scraping
2. **Dependente de layout**: PDFs devem seguir padrão estruturado
3. **Rate limiting obrigatório**: Necessário respeitar limites do servidor
4. **Stopwords corporativas**: Termos como "LTDA", "ME" não retornam resultados

## Suporte

Para dúvidas ou problemas, consulte os logs em `data/scraper.log` ou abra uma issue no repositório.

## Licença

MIT License - Veja arquivo LICENSE para detalhes.