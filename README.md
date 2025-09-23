# CETESB CADRI Data Extractor

Sistema automatizado para extrair dados cadastrais de empresas e informações técnicas detalhadas de documentos CADRI (Certificados de Movimentação de Resíduos de Interesse Ambiental) do site público da CETESB.

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

## Arquitetura

```
Empresas → Documentos → PDFs → Dados Extraídos
   ↓           ↓         ↓         ↓
List    →   Detail  →  Download → Parse
```

### Componentes Principais

- **Pipeline (`src/`)**: Orquestração das etapas de coleta
- **cert_mov_direct_downloader.py**: Download direto de PDFs
- **interactive_pdf_downloader.py**: Download interativo (fallback)
- **pdf_parser_standalone.py**: Extração estruturada de dados dos PDFs
- **monitor_progress.py**: Monitoramento do progresso
- **cadri_utils.py**: Utilitários de gerenciamento

## Dados Extraídos

### empresas.csv (Dados Cadastrais)
- `cnpj` (chave primária)
- `razao_social`
- `logradouro`, `municipio`, `uf`, `cep`
- `numero_cadastro_cetesb`
- `descricao_atividade`

### cadri_itens.csv (Dados Técnicos - 15 Campos)
**Identificação do Resíduo:**
- `numero_residuo` (D099, F001, K001, etc.)
- `descricao_residuo`
- `classe_residuo` (I, IIA, IIB)
- `estado_fisico` (LIQUIDO, SOLIDO, GASOSO)

**Características Técnicas:**
- `quantidade`, `unidade` (t, kg, L, m³)
- `oii` (Orgânico/Inorgânico)
- `composicao_aproximada`
- `metodo_utilizado`
- `cor_cheiro_aspecto`

**Logística:**
- `acondicionamento_codigos` (E01,E04,E05)
- `acondicionamento_descricoes` (Tambor, Tanque, Bombonas)
- `destino_codigo` (T34, etc.)
- `destino_descricao`

**Metadados:**
- `numero_documento`, `item_numero`, `tipo_documento`

## Monitoramento

```bash
# Verificar progresso geral
python monitor_progress.py

# Status específico de documentos
python cadri_utils.py count

# Listar tipos de documentos disponíveis
python cadri_utils.py list-types
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

## Características Técnicas

- **🔄 Operações idempotentes**: Pode ser interrompido e retomado
- **📊 Extração estruturada**: 15 campos técnicos por item de resíduo
- **⚡ Download otimizado**: URL pattern discovery para downloads diretos
- **🛡️ Rate limiting**: Respeita limites do servidor
- **💾 Persistência CSV**: Dados estruturados prontos para análise

## Limitações

1. **Sem API pública**: Sistema baseado em web scraping
2. **Dependente de layout**: PDFs devem seguir padrão estruturado
3. **Rate limiting obrigatório**: Necessário respeitar limites do servidor
4. **Stopwords corporativas**: Termos como "LTDA", "ME" não retornam resultados

## Suporte

Para dúvidas ou problemas, consulte os logs em `data/scraper.log` ou abra uma issue no repositório.

## Licença

MIT License - Veja arquivo LICENSE para detalhes.