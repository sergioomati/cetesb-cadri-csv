# CETESB CADRI Scraper - CSV Version

Sistema robusto de scraping e ETL para extrair dados de Certificados de Movimentação de Resíduos de Interesse Ambiental (CADRI) do site público da CETESB.

## Características

- **Busca adaptativa** por razão social com refinamento automático de seeds (3→4 caracteres)
- **Persistência idempotente** em CSV com deduplicação via pandas
- **Download e parsing de PDFs** com extração estruturada de resíduos
- **Pipeline resiliente** com checkpoint e capacidade de resumir
- **Rate limiting** configurável com jitter para respeitar o servidor
- **Logging estruturado** com métricas de desempenho

## Arquitetura

```
1. Seeds → 2. Lista → 3. Detalhes → 4. PDFs → 5. Parse → 6. CSV
         ↑                                              ↓
         └─────────── Bootstrapping ←──────────────────┘
```

### Componentes Principais

- **seeds.py**: Geração inteligente de seeds com bloqueio de stopwords corporativas
- **scrape_list.py**: Busca empresas via Playwright (headless)
- **scrape_detail.py**: Extrai documentos CADRI via httpx/BeautifulSoup
- **download_pdf.py**: Download assíncrono de PDFs do portal de autenticidade
- **parse_pdf.py**: Extração de resíduos com pymupdf (sem OCR)
- **store_csv.py**: Upsert idempotente com pandas

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd cetesb_cadri_csv

# Crie ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instale dependências
pip install -r requirements.txt

# Instale Playwright browsers
playwright install chromium

# Configure ambiente
cp .env.example .env
# Edite .env conforme necessário
```

## Uso

### Pipeline de Coleta de Dados

```bash
# Executar coleta completa (list + detail)
python -m src.pipeline --stage all

# Com seeds específicos
python -m src.pipeline --stage all --seeds CEM,ACE,AGR
```

### Etapas Individuais

```bash
# 1. Buscar empresas
python -m src.pipeline --stage list --seeds CEM

# 2. Extrair detalhes e URLs de documentos
python -m src.pipeline --stage detail
```

### Download e Parsing de PDFs (Módulos Separados)

```bash
# 3. Baixar PDFs (módulo interativo com Playwright)
python interactive_pdf_downloader.py

# 4. Parsear PDFs baixados
python pdf_parser_standalone.py
```

### Opções Avançadas

```bash
# Resetar estado dos seeds
python -m src.pipeline --reset-seeds

# Começar do zero (sem resumir)
python -m src.pipeline --no-resume

# Ajustar log level
python -m src.pipeline --log-level DEBUG

# Limitar iterações
python -m src.pipeline --max-iterations 3
```

## Estrutura dos CSVs

### empresas.csv
- `cnpj` (chave primária)
- `razao_social`
- `municipio`
- `uf`
- `updated_at`

### cadri_documentos.csv
- `numero_documento` (chave primária)
- `tipo_documento`
- `cnpj`
- `razao_social`
- `data_emissao`
- `url_detalhe`
- `url_pdf`
- `status_pdf`
- `pdf_hash` (SHA256)
- `updated_at`

### cadri_itens.csv
- `numero_documento`
- `residuo`
- `classe` (I, IIA, IIB)
- `estado_fisico` (sólido, líquido, pastoso, gasoso)
- `quantidade`
- `unidade` (kg, t, L, m³)
- `pagina_origem`
- `raw_fragment`
- `updated_at`

## Configuração (.env)

```env
# Rate limiting (segundos)
RATE_MIN=0.6
RATE_MAX=1.4

# Browser
HEADLESS=true
BROWSER_TIMEOUT=30000

# Limites
MAX_PAGES=10
MAX_RETRIES=3

# User Agent (incluir contato!)
USER_AGENT="Mozilla/5.0 (compatible; SeuProjeto/1.0; +seu@email.com)"
```

## Limitações Conhecidas

1. **Sem API pública**: Sistema baseado em scraping web
2. **Buscas por CNPJ**: Não aceitam substring, apenas CNPJ completo
3. **Stopwords corporativas**: "LTDA", "ME", "EPP" não retornam resultados
4. **PDFs antigos**: Alguns podem exigir OCR (não implementado)
5. **Rate limiting**: Necessário respeitar limites para evitar bloqueios
6. **Variações de layout**: PDFs podem ter formatos diferentes

## Testes

```bash
# Executar todos os testes
pytest

# Com coverage
pytest --cov=src --cov-report=html

# Teste específico
pytest tests/test_seeds.py -v
```

## Monitoramento

O sistema registra métricas em tempo real:
- Taxa de extração (detalhes/hora)
- PDFs baixados e parseados
- Erros encontrados
- ETA estimado

Logs salvos em `data/scraper.log`

## Boas Práticas

1. **Execute em horários de baixo tráfego** (noites e fins de semana)
2. **Monitore os logs** para detectar mudanças no site
3. **Faça backups dos CSVs** regularmente
4. **Respeite robots.txt** e termos de uso
5. **Use User-Agent descritivo** com informações de contato

## Troubleshooting

### "No results found"
- Verifique se o seed tem pelo menos 3 caracteres
- Evite stopwords (LTDA, ME, EPP, S/A)
- Tente seeds mais genéricos (CEM, AGR, BIO)

### PDFs não baixando
- Verifique conectividade
- Confirme que o número do documento está correto
- Alguns PDFs podem não existir no portal

### Parsing incorreto
- PDFs com layout diferente podem necessitar ajustes nos regex
- Verifique `raw_fragment` no CSV para debug
- PDFs escaneados precisariam de OCR

## Licença

MIT - Veja arquivo LICENSE

## Contribuições

PRs são bem-vindos! Por favor:
1. Fork o projeto
2. Crie feature branch
3. Adicione testes
4. Faça commit das mudanças
5. Push e abra PR

## Contato

Para dúvidas ou sugestões, abra uma issue no GitHub.