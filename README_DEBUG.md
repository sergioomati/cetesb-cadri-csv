# Guia de Debug para Detecção de Documentos CADRI

Este guia explica como usar as ferramentas de debug criadas para investigar por que não estão sendo detectados documentos CADRI.

## Problema Identificado

Baseado na análise do CSV `cadri_documentos.csv` (vazio) e do log, o sistema está:
- ✅ Encontrando empresas (386, 1056, 1134 resultados por seed)
- ❌ **NÃO encontrando documentos CADRI** nas páginas de detalhes
- ❌ **NÃO extraindo informações das empresas** (razão social, município vazios)

## Ferramentas Criadas

### 1. `debug_single_company.py`
Script principal para debug de uma empresa específica.

**Uso:**
```bash
# Testar CNPJ específico
python debug_single_company.py --cnpj 12345678000100

# Buscar empresa por nome
python debug_single_company.py --search "PETROBRAS"

# Testar URL direta
python debug_single_company.py --url "https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc=12345678000100"

# Debug detalhado
python debug_single_company.py --search "CEMIG" --log-level DEBUG
```

**O que faz:**
- Busca empresa (se necessário)
- Acessa página de detalhes
- **Salva HTML da página** para análise manual
- Tenta extrair informações da empresa
- Tenta extrair documentos CADRI
- **Salva todos os dados em JSON** para debug
- **Analisa estrutura HTML** automaticamente

### 2. `src/html_analyzer.py`
Ferramenta para analisar páginas HTML salvas.

**Uso:**
```bash
# Analisar arquivo específico
python -m src.html_analyzer --file data/debug/session_XXXX/detail_page.html

# Analisar todos os HTMLs de uma sessão
python -m src.html_analyzer --dir data/debug/session_XXXX
```

**O que faz:**
- Analisa estrutura HTML (tabelas, formulários, links)
- Conta menções de palavras-chave ('CADRI', 'resíduo', etc.)
- Identifica padrões de documentos
- **Sugere correções** baseadas na análise

### 3. `src/improved_patterns.py`
Versão melhorada dos patterns de detecção.

**Melhorias:**
- **Múltiplas variações** do tipo de documento
- **Fallbacks** para diferentes estruturas HTML
- **Extração flexível** sem depender de headers específicos
- **Deduplicação** de documentos
- **Logging detalhado** de métodos de extração

### 4. `test_debug_example.py`
Exemplos de como usar as ferramentas.

**Uso:**
```bash
# Ver exemplos de uso
python test_debug_example.py --show-usage

# Executar testes automáticos
python test_debug_example.py --run-tests
```

## Fluxo de Debug Recomendado

### Passo 1: Teste com Empresa Conhecida
```bash
# Use uma empresa que você sabe que deveria ter documentos CADRI
python debug_single_company.py --search "PETROBRAS" --log-level DEBUG
```

### Passo 2: Analise o HTML Salvo
1. Vá para o diretório da sessão: `data/debug/session_YYYYMMDD_HHMMSS/`
2. Abra o arquivo `detail_page_XXXX.html` em um navegador
3. Procure manualmente por:
   - Tabelas com documentos
   - Menções de "CADRI", "Certificado", "Resíduo"
   - Estrutura diferente do esperado

### Passo 3: Analise os Dados Extraídos
Verifique os arquivos JSON salvos:
- `company_info.json`: Dados da empresa extraídos
- `documents.json`: Documentos encontrados
- `html_analysis.json`: Análise da estrutura HTML
- `extraction_patterns.json`: Padrões tentados

### Passo 4: Use o Analisador HTML
```bash
python -m src.html_analyzer --dir data/debug/session_YYYYMMDD_HHMMSS
```

Este comando vai gerar relatório detalhado com sugestões.

### Passo 5: Ajuste os Patterns
Com base na análise, você pode:
1. Modificar `src/scrape_detail.py` para usar os patterns melhorados
2. Ajustar seletores CSS específicos
3. Adicionar novos patterns para estruturas encontradas

## Possíveis Causas e Soluções

### 1. Filtro de Tipo de Documento Muito Restritivo
**Problema:** `TARGET_DOC_TYPE = "CERT MOV RESIDUOS INT AMB"` muito específico

**Solução:** Use os patterns melhorados que incluem variações:
```python
DOCUMENT_TYPE_VARIATIONS = [
    "CERT MOV RESIDUOS INT AMB",
    "CERTIFICADO MOVIMENTACAO RESIDUOS",
    "CADRI",
    # etc...
]
```

### 2. Estrutura HTML Diferente do Esperado
**Problema:** Páginas não usam tabelas padrão

**Solução:** Patterns melhorados incluem extração de:
- Divs com documentos
- Parágrafos com informações
- Listas de documentos
- Estruturas aninhadas

### 3. Páginas Vazias ou com Erro
**Problema:** CNPJs inválidos ou páginas com erro

**Solução:** Debug mostra conteúdo real da página e detecta mensagens de erro

### 4. Rate Limiting ou Bloqueio
**Problema:** Servidor bloqueia requests

**Solução:** Debug mostra status HTTP e conteúdo retornado

## Exemplo de Uso Completo

```bash
# 1. Debug uma empresa específica
python debug_single_company.py --search "BRASKEM" --log-level DEBUG

# 2. Analise o resultado
# Verifique os logs para ver o que foi encontrado

# 3. Analise HTML salvo
python -m src.html_analyzer --dir data/debug/session_YYYYMMDD_HHMMSS

# 4. Se necessário, teste patterns melhorados
# Modifique scrape_detail.py para usar improved_patterns.py

# 5. Teste novamente
python debug_single_company.py --cnpj CNPJ_ENCONTRADO --log-level DEBUG
```

## Integrando Patterns Melhorados

Para usar os patterns melhorados no sistema principal:

1. **Modifique `DetailScraper`** em `src/scrape_detail.py`:
```python
from improved_patterns import extract_cadri_documents_improved

# Na função extract_cadri_documents, substitua por:
def extract_cadri_documents(self, soup: BeautifulSoup, company_info: Dict) -> List[Dict]:
    return extract_cadri_documents_improved(soup, company_info)
```

2. **Teste o sistema completo**:
```bash
python -m src.pipeline --stage detail --log-level DEBUG
```

## Arquivos de Debug

Após cada execução, os seguintes arquivos são salvos:

```
data/debug/session_YYYYMMDD_HHMMSS/
├── detail_page_HASH_TIMESTAMP.html     # HTML da página
├── company_info_TIMESTAMP.json         # Dados da empresa
├── documents_TIMESTAMP.json            # Documentos encontrados
├── html_analysis_TIMESTAMP.json        # Análise da estrutura
├── extraction_patterns_TIMESTAMP.json  # Padrões tentados
└── page_analysis_TIMESTAMP.json        # Análise detalhada
```

## Próximos Passos

1. **Execute o debug** com algumas empresas conhecidas
2. **Analise os HTMLs** salvos para entender a estrutura real
3. **Ajuste os patterns** baseado nos achados
4. **Teste iterativamente** até encontrar documentos
5. **Integre a solução** no sistema principal

Com essas ferramentas, você deve conseguir identificar exatamente onde está o problema na extração de documentos CADRI.