#!/usr/bin/env python3
"""
# DEBUG_CNPJ: Script debug standalone para investigar formulário CNPJ
TEMPORÁRIO - Para ser removido após corrigir problema
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from browser import BrowserManager
from config import BASE_URL_LICENCIAMENTO


class CNPJFormDebugger:
    """Debug do formulário CNPJ"""

    def __init__(self):
        self.search_url = f"{BASE_URL_LICENCIAMENTO}/processo_consulta.asp"
        self.test_cnpj = "60409075002953"

    async def debug_form_structure(self):
        """# DEBUG_CNPJ: Investigar estrutura completa do formulário"""
        print(f"# DEBUG_CNPJ: Iniciando debug do formulário CNPJ")
        print(f"# DEBUG_CNPJ: URL: {self.search_url}")
        print(f"# DEBUG_CNPJ: CNPJ teste: {self.test_cnpj}")

        async with BrowserManager() as browser:
            page = await browser.new_page()

            # Navigate to form
            print(f"# DEBUG_CNPJ: Navegando para formulário...")
            await page.goto(self.search_url, wait_until='networkidle')

            # DEBUG_CNPJ: Screenshot inicial
            debug_dir = Path("data/debug")
            debug_dir.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(debug_dir / "DEBUG_CNPJ_01_form_loaded.png"))

            # DEBUG_CNPJ: Verificar todos os inputs do formulário
            print(f"# DEBUG_CNPJ: Analisando todos os campos do formulário...")

            form_inputs = await page.evaluate("""
                () => {
                    const inputs = Array.from(document.querySelectorAll('input'));
                    return inputs.map(input => ({
                        tagName: input.tagName,
                        type: input.type,
                        name: input.name,
                        id: input.id,
                        value: input.value,
                        placeholder: input.placeholder,
                        visible: !input.hidden && input.offsetParent !== null,
                        disabled: input.disabled,
                        readonly: input.readOnly
                    }));
                }
            """)

            print(f"# DEBUG_CNPJ: Encontrados {len(form_inputs)} campos input:")
            for i, inp in enumerate(form_inputs):
                print(f"# DEBUG_CNPJ: Input {i+1}: {inp}")

            # DEBUG_CNPJ: Verificar especificamente campo CNPJ
            cnpj_selectors = [
                'input[name="cnpj"]',
                'input[id="cnpj"]',
                '#cnpj',
                'input[placeholder*="cnpj" i]',
                'input[placeholder*="CNPJ"]'
            ]

            print(f"# DEBUG_CNPJ: Testando seletores de CNPJ...")
            for selector in cnpj_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        is_visible = await element.is_visible()
                        is_enabled = await element.is_enabled()
                        attrs = await element.evaluate('el => ({name: el.name, id: el.id, type: el.type, placeholder: el.placeholder})')
                        print(f"# DEBUG_CNPJ: ✅ Seletor '{selector}' encontrado - Visível: {is_visible}, Habilitado: {is_enabled}, Attrs: {attrs}")
                    else:
                        print(f"# DEBUG_CNPJ: ❌ Seletor '{selector}' NÃO encontrado")
                except Exception as e:
                    print(f"# DEBUG_CNPJ: ❌ Erro testando seletor '{selector}': {e}")

            # DEBUG_CNPJ: Verificar estrutura HTML do formulário
            form_html = await page.evaluate("""
                () => {
                    const forms = Array.from(document.querySelectorAll('form'));
                    return forms.map(form => form.outerHTML);
                }
            """)

            print(f"# DEBUG_CNPJ: HTML dos formulários encontrados:")
            for i, html in enumerate(form_html):
                print(f"# DEBUG_CNPJ: Formulário {i+1}:")
                print(f"# DEBUG_CNPJ: {html[:500]}...")  # Primeiros 500 chars

    async def test_cnpj_input_methods(self):
        """# DEBUG_CNPJ: Testar diferentes métodos de input de CNPJ"""
        print(f"# DEBUG_CNPJ: Testando métodos de input de CNPJ...")

        async with BrowserManager() as browser:
            page = await browser.new_page()
            await page.goto(self.search_url, wait_until='networkidle')

            debug_dir = Path("data/debug")

            # Teste 1: CNPJ sem formatação
            print(f"# DEBUG_CNPJ: Teste 1 - CNPJ sem formatação: {self.test_cnpj}")
            await self.test_cnpj_format(page, self.test_cnpj, "sem_formatacao", debug_dir)

            # Teste 2: CNPJ com formatação
            formatted_cnpj = f"{self.test_cnpj[:2]}.{self.test_cnpj[2:5]}.{self.test_cnpj[5:8]}/{self.test_cnpj[8:12]}-{self.test_cnpj[12:]}"
            print(f"# DEBUG_CNPJ: Teste 2 - CNPJ com formatação: {formatted_cnpj}")
            await self.test_cnpj_format(page, formatted_cnpj, "com_formatacao", debug_dir)

    async def test_cnpj_format(self, page, cnpj_value, test_name, debug_dir):
        """# DEBUG_CNPJ: Testar um formato específico de CNPJ"""
        try:
            # Recarregar página para novo teste
            await page.goto(self.search_url, wait_until='networkidle')

            # Encontrar campo CNPJ
            cnpj_field = await page.query_selector('input[name="cnpj"]')
            if not cnpj_field:
                print(f"# DEBUG_CNPJ: ❌ Campo CNPJ não encontrado para teste {test_name}")
                return

            # Screenshot antes
            await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_02_{test_name}_before.png"))

            # Preencher campo
            print(f"# DEBUG_CNPJ: Preenchendo campo com valor: {cnpj_value}")
            await cnpj_field.click()
            await cnpj_field.fill("")  # Limpar primeiro
            await cnpj_field.type(cnpj_value)

            # Verificar se valor foi aceito
            filled_value = await cnpj_field.input_value()
            print(f"# DEBUG_CNPJ: Valor preenchido: '{filled_value}'")

            # Screenshot após preenchimento
            await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_03_{test_name}_filled.png"))

            # Tentar submeter
            submit_btn = await page.query_selector('input[type="submit"]')
            if submit_btn:
                print(f"# DEBUG_CNPJ: Tentando submeter formulário...")
                await submit_btn.click()

                # Aguardar resposta com timeout menor
                try:
                    await page.wait_for_selector('table, .erro, .error', timeout=5000)
                    print(f"# DEBUG_CNPJ: ✅ Formulário submetido com sucesso para {test_name}")

                    # Screenshot do resultado
                    await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_04_{test_name}_result.png"))

                    # Verificar se há resultados ou erros
                    page_content = await page.content()
                    if "nenhum resultado" in page_content.lower():
                        print(f"# DEBUG_CNPJ: ⚠️ Nenhum resultado encontrado para {test_name}")
                    elif "erro" in page_content.lower():
                        print(f"# DEBUG_CNPJ: ❌ Erro na busca para {test_name}")
                    else:
                        print(f"# DEBUG_CNPJ: ✅ Resultados encontrados para {test_name}")

                except Exception as e:
                    print(f"# DEBUG_CNPJ: ❌ Timeout na submissão para {test_name}: {e}")
                    await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_05_{test_name}_timeout.png"))

        except Exception as e:
            print(f"# DEBUG_CNPJ: ❌ Erro no teste {test_name}: {e}")

    async def test_javascript_events(self):
        """# DEBUG_CNPJ: Testar eventos JavaScript no formulário"""
        print(f"# DEBUG_CNPJ: Testando eventos JavaScript...")

        async with BrowserManager() as browser:
            page = await browser.new_page()
            await page.goto(self.search_url, wait_until='networkidle')

            debug_dir = Path("data/debug")

            # Verificar eventos JavaScript anexados aos campos
            js_events = await page.evaluate("""
                () => {
                    const cnpjField = document.querySelector('input[name="cnpj"]');
                    if (!cnpjField) return "Campo CNPJ não encontrado";

                    // Verificar listeners
                    const events = ['focus', 'blur', 'input', 'change', 'keyup', 'keydown'];
                    const hasEvents = {};

                    events.forEach(eventType => {
                        // Simular evento para ver se há listeners
                        const event = new Event(eventType, { bubbles: true });
                        hasEvents[eventType] = cnpjField.dispatchEvent(event);
                    });

                    return {
                        hasEvents,
                        fieldInfo: {
                            name: cnpjField.name,
                            id: cnpjField.id,
                            type: cnpjField.type,
                            required: cnpjField.required,
                            pattern: cnpjField.pattern,
                            maxLength: cnpjField.maxLength
                        }
                    };
                }
            """)

            print(f"# DEBUG_CNPJ: Eventos JavaScript detectados: {js_events}")

            # Testar preenchimento com eventos
            print(f"# DEBUG_CNPJ: Testando preenchimento com eventos JavaScript...")
            try:
                await page.evaluate(f"""
                    () => {{
                        const cnpjField = document.querySelector('input[name="cnpj"]');
                        if (cnpjField) {{
                            // Simular interação humana completa
                            cnpjField.focus();
                            cnpjField.value = '{self.test_cnpj}';

                            // Disparar eventos
                            ['input', 'change', 'blur'].forEach(eventType => {{
                                const event = new Event(eventType, {{ bubbles: true }});
                                cnpjField.dispatchEvent(event);
                            }});
                        }}
                    }}
                """)

                # Screenshot após JS
                await page.screenshot(path=str(debug_dir / "DEBUG_CNPJ_06_js_filled.png"))

                # Verificar valor
                js_value = await page.evaluate("document.querySelector('input[name=\"cnpj\"]')?.value")
                print(f"# DEBUG_CNPJ: Valor após JavaScript: '{js_value}'")

            except Exception as e:
                print(f"# DEBUG_CNPJ: ❌ Erro testando eventos JavaScript: {e}")

    async def run_all_tests(self):
        """# DEBUG_CNPJ: Executar todos os testes de debug"""
        print(f"# DEBUG_CNPJ: === INICIANDO DEBUG COMPLETO DO FORMULÁRIO CNPJ ===")

        try:
            await self.debug_form_structure()
            print(f"# DEBUG_CNPJ: ---")
            await self.test_cnpj_input_methods()
            print(f"# DEBUG_CNPJ: ---")
            await self.test_javascript_events()
        except Exception as e:
            print(f"# DEBUG_CNPJ: ❌ Erro geral no debug: {e}")

        print(f"# DEBUG_CNPJ: === DEBUG COMPLETO FINALIZADO ===")
        print(f"# DEBUG_CNPJ: Screenshots salvos em: data/debug/DEBUG_CNPJ_*.png")


async def main():
    """# DEBUG_CNPJ: Função principal de debug"""
    debugger = CNPJFormDebugger()
    await debugger.run_all_tests()


if __name__ == "__main__":
    print("# DEBUG_CNPJ: Executando debug standalone do formulário CNPJ...")
    asyncio.run(main())