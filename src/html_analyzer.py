#!/usr/bin/env python3
"""
HTML Analysis Utilities for CETESB Debug

Ferramentas para analisar páginas HTML salvas durante o debug,
identificar padrões e melhorar a extração de dados.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup
import json
from datetime import datetime

from utils_text import normalize_text, extract_document_number, parse_date_br
from config import TARGET_DOC_TYPE
from logging_conf import logger


class HTMLAnalyzer:
    """Analyze HTML pages for debugging CADRI extraction"""

    def __init__(self, debug_dir: Path = None):
        self.debug_dir = debug_dir

    def analyze_file(self, html_file: Path) -> Dict:
        """Analyze a single HTML file"""
        logger.info(f"Analyzing HTML file: {html_file}")

        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract URL from HTML comment if present
        url_match = re.search(r'<!-- URL: (.*?) -->', content)
        url = url_match.group(1) if url_match else "Unknown"

        soup = BeautifulSoup(content, 'html.parser')

        analysis = {
            'file': str(html_file),
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'basic_info': self._analyze_basic_structure(soup),
            'text_analysis': self._analyze_text_content(soup),
            'table_analysis': self._analyze_tables(soup),
            'form_analysis': self._analyze_forms(soup),
            'link_analysis': self._analyze_links(soup),
            'document_patterns': self._find_document_patterns(soup),
            'potential_fixes': self._suggest_fixes(soup)
        }

        return analysis

    def _analyze_basic_structure(self, soup: BeautifulSoup) -> Dict:
        """Analyze basic HTML structure"""
        return {
            'title': soup.title.string if soup.title else None,
            'total_elements': len(soup.find_all()),
            'div_count': len(soup.find_all('div')),
            'table_count': len(soup.find_all('table')),
            'form_count': len(soup.find_all('form')),
            'input_count': len(soup.find_all('input')),
            'link_count': len(soup.find_all('a')),
            'script_count': len(soup.find_all('script')),
            'has_doctype': '<!DOCTYPE' in str(soup)[:200],
            'encoding_meta': bool(soup.find('meta', attrs={'charset': True}) or
                                soup.find('meta', attrs={'http-equiv': 'Content-Type'}))
        }

    def _analyze_text_content(self, soup: BeautifulSoup) -> Dict:
        """Analyze text content for keywords"""
        text_content = soup.get_text()
        text_lower = text_content.lower()

        # Key terms for CADRI documents
        keywords = {
            'cadri': text_lower.count('cadri'),
            'residuo': text_lower.count('resíduo') + text_lower.count('residuo'),
            'certificado': text_lower.count('certificado'),
            'cert': text_lower.count('cert'),
            'documento': text_lower.count('documento'),
            'movimentacao': text_lower.count('movimentação') + text_lower.count('movimentacao'),
            'interesse_ambiental': text_lower.count('interesse ambiental'),
            'licenciamento': text_lower.count('licenciamento'),
            'cetesb': text_lower.count('cetesb')
        }

        # Search for target document type
        target_matches = []
        if TARGET_DOC_TYPE.lower() in text_lower:
            # Find contexts where target type appears
            target_pattern = re.compile(rf'.{{0,100}}{re.escape(TARGET_DOC_TYPE.lower())}.{{0,100}}', re.IGNORECASE)
            matches = target_pattern.findall(text_content)
            target_matches = [match.strip() for match in matches[:5]]

        # Look for document numbers
        doc_numbers = []
        for line in text_content.split('\n'):
            line = line.strip()
            if line and len(line) > 3:
                doc_num = extract_document_number(line)
                if doc_num and len(doc_num) >= 4:  # Reasonable document number length
                    doc_numbers.append({
                        'number': doc_num,
                        'context': line[:100]
                    })

        return {
            'total_length': len(text_content),
            'line_count': len(text_content.split('\n')),
            'keyword_counts': keywords,
            'target_type_matches': target_matches,
            'potential_doc_numbers': doc_numbers[:10],  # First 10
            'has_error_messages': any(term in text_lower for term in
                                    ['erro', 'error', 'não encontrado', 'not found', 'nenhum resultado'])
        }

    def _analyze_tables(self, soup: BeautifulSoup) -> List[Dict]:
        """Analyze all tables for document data"""
        tables = soup.find_all('table')
        table_analyses = []

        for i, table in enumerate(tables):
            # Get table structure
            headers = table.find_all('th')
            header_texts = [h.get_text().strip() for h in headers]

            rows = table.find_all('tr')
            data_rows = rows[1:] if headers else rows  # Skip header row if present

            # Analyze content
            table_text = table.get_text().lower()
            has_documents = any(term in table_text for term in
                              ['documento', 'cadri', 'certificado', 'resíduo', 'residuo'])

            # Sample data from first few rows
            sample_data = []
            for row in data_rows[:3]:
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text().strip() for cell in cells]
                if any(cell for cell in cell_texts):  # Non-empty row
                    sample_data.append(cell_texts)

            # Check for document patterns in table
            potential_docs = []
            for row in data_rows[:10]:  # Check first 10 rows
                row_text = row.get_text()
                if TARGET_DOC_TYPE.lower() in row_text.lower():
                    cells = [cell.get_text().strip() for cell in row.find_all(['td', 'th'])]
                    doc_num = extract_document_number(row_text)
                    if doc_num:
                        potential_docs.append({
                            'document_number': doc_num,
                            'cells': cells,
                            'raw_text': row_text.strip()
                        })

            table_analysis = {
                'index': i,
                'header_count': len(headers),
                'headers': header_texts,
                'row_count': len(data_rows),
                'column_count': max(len(row.find_all(['td', 'th'])) for row in rows) if rows else 0,
                'has_document_content': has_documents,
                'sample_data': sample_data,
                'potential_documents': potential_docs,
                'table_classes': table.get('class', []),
                'table_id': table.get('id', ''),
                'nested_tables': len(table.find_all('table'))
            }

            table_analyses.append(table_analysis)

        return table_analyses

    def _analyze_forms(self, soup: BeautifulSoup) -> List[Dict]:
        """Analyze forms on the page"""
        forms = soup.find_all('form')
        form_analyses = []

        for i, form in enumerate(forms):
            inputs = form.find_all('input')
            selects = form.find_all('select')
            textareas = form.find_all('textarea')

            form_analysis = {
                'index': i,
                'action': form.get('action', ''),
                'method': form.get('method', 'GET'),
                'input_count': len(inputs),
                'select_count': len(selects),
                'textarea_count': len(textareas),
                'input_types': [inp.get('type', 'text') for inp in inputs],
                'input_names': [inp.get('name', '') for inp in inputs if inp.get('name')],
                'has_search_elements': any(term in str(form).lower() for term in
                                         ['search', 'busca', 'consulta', 'pesquisa'])
            }

            form_analyses.append(form_analysis)

        return form_analyses

    def _analyze_links(self, soup: BeautifulSoup) -> Dict:
        """Analyze links for PDF and detail page patterns"""
        links = soup.find_all('a')

        # Categorize links
        pdf_links = []
        detail_links = []
        auth_links = []
        other_links = []

        for link in links:
            href = link.get('href', '')
            text = link.get_text().strip()

            if 'pdf' in href.lower():
                pdf_links.append({'href': href, 'text': text})
            elif 'autenticidade' in href.lower():
                auth_links.append({'href': href, 'text': text})
            elif 'processo_resultado' in href.lower():
                detail_links.append({'href': href, 'text': text})
            else:
                other_links.append({'href': href, 'text': text})

        return {
            'total_links': len(links),
            'pdf_links': pdf_links[:5],  # First 5
            'authenticity_links': auth_links[:5],
            'detail_links': detail_links[:5],
            'other_links': len(other_links),
            'external_links': len([l for l in links if l.get('href', '').startswith('http')])
        }

    def _find_document_patterns(self, soup: BeautifulSoup) -> Dict:
        """Find patterns that could indicate document data"""
        patterns_found = {}

        # Pattern 1: Elements containing target document type
        target_elements = soup.find_all(string=lambda x: x and TARGET_DOC_TYPE.lower() in x.lower())
        if target_elements:
            patterns_found['target_type_elements'] = []
            for elem in target_elements[:5]:
                try:
                    parent = elem.parent
                    context = {
                        'text': elem.strip()[:100],
                        'parent_tag': parent.name if parent else None,
                        'parent_class': parent.get('class', []) if parent else [],
                        'siblings': len(parent.find_all()) if parent else 0
                    }
                    patterns_found['target_type_elements'].append(context)
                except:
                    pass

        # Pattern 2: Numeric patterns that could be document numbers
        text_content = soup.get_text()
        number_patterns = re.findall(r'\b\d{4,10}\b', text_content)
        if number_patterns:
            patterns_found['potential_doc_numbers'] = list(set(number_patterns))[:10]

        # Pattern 3: Date patterns
        date_patterns = re.findall(r'\b\d{1,2}/\d{1,2}/\d{4}\b', text_content)
        if date_patterns:
            patterns_found['date_patterns'] = list(set(date_patterns))[:5]

        # Pattern 4: Elements with specific classes or IDs that might contain documents
        doc_containers = soup.find_all(['div', 'span', 'td'],
                                     class_=re.compile(r'doc|cert|result', re.I))
        if doc_containers:
            patterns_found['potential_containers'] = []
            for container in doc_containers[:3]:
                patterns_found['potential_containers'].append({
                    'tag': container.name,
                    'class': container.get('class', []),
                    'id': container.get('id', ''),
                    'text_sample': container.get_text().strip()[:100]
                })

        return patterns_found

    def _suggest_fixes(self, soup: BeautifulSoup) -> List[str]:
        """Suggest potential fixes based on analysis"""
        suggestions = []

        # Check if page has content at all
        text_content = soup.get_text().strip()
        if len(text_content) < 100:
            suggestions.append("Page has very little content - check if page loaded correctly")

        # Check for error messages
        if any(term in text_content.lower() for term in ['erro', 'error', 'não encontrado']):
            suggestions.append("Page contains error messages - check URL parameters or search terms")

        # Check for CADRI mentions
        cadri_count = text_content.lower().count('cadri')
        if cadri_count == 0:
            suggestions.append("No CADRI mentions found - this might not be a document page")
        elif cadri_count > 0:
            suggestions.append(f"Found {cadri_count} CADRI mentions - page likely contains relevant data")

        # Check table structure
        tables = soup.find_all('table')
        if not tables:
            suggestions.append("No tables found - documents might be in different HTML structure")
        else:
            for i, table in enumerate(tables):
                headers = table.find_all('th')
                if not headers:
                    suggestions.append(f"Table {i+1} has no headers - might need different parsing approach")

        # Check for target document type
        if TARGET_DOC_TYPE.lower() not in text_content.lower():
            suggestions.append(f"Target document type '{TARGET_DOC_TYPE}' not found - check filter criteria")

        # Check for potential document numbers
        doc_numbers = re.findall(r'\b\d{4,}\b', text_content)
        if not doc_numbers:
            suggestions.append("No potential document numbers found")
        elif len(doc_numbers) > 10:
            suggestions.append(f"Many number patterns found ({len(doc_numbers)}) - refine extraction logic")

        return suggestions

    def generate_report(self, html_file: Path, output_file: Path = None) -> Path:
        """Generate comprehensive analysis report"""
        analysis = self.analyze_file(html_file)

        if not output_file:
            output_file = html_file.parent / f"analysis_{html_file.stem}.json"

        # Save detailed analysis
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Analysis report saved: {output_file}")

        # Print summary
        self._print_summary(analysis)

        return output_file

    def _print_summary(self, analysis: Dict):
        """Print analysis summary to console"""
        print(f"\n{'='*60}")
        print(f"HTML ANALYSIS SUMMARY")
        print(f"{'='*60}")
        print(f"File: {analysis['file']}")
        print(f"URL: {analysis['url']}")

        basic = analysis['basic_info']
        print(f"\nBasic Structure:")
        print(f"  Title: {basic['title']}")
        print(f"  Elements: {basic['total_elements']}")
        print(f"  Tables: {basic['table_count']}")
        print(f"  Forms: {basic['form_count']}")
        print(f"  Links: {basic['link_count']}")

        text = analysis['text_analysis']
        print(f"\nText Analysis:")
        print(f"  Length: {text['total_length']} characters")
        print(f"  CADRI mentions: {text['keyword_counts']['cadri']}")
        print(f"  Resíduo mentions: {text['keyword_counts']['residuo']}")
        print(f"  Document mentions: {text['keyword_counts']['documento']}")
        print(f"  Potential doc numbers: {len(text['potential_doc_numbers'])}")

        tables = analysis['table_analysis']
        if tables:
            print(f"\nTable Analysis:")
            for table in tables:
                print(f"  Table {table['index'] + 1}: {table['row_count']} rows, {table['column_count']} columns")
                if table['potential_documents']:
                    print(f"    Potential documents: {len(table['potential_documents'])}")

        suggestions = analysis['potential_fixes']
        if suggestions:
            print(f"\nSuggestions:")
            for suggestion in suggestions:
                print(f"  - {suggestion}")

    def batch_analyze(self, html_dir: Path) -> Dict:
        """Analyze all HTML files in a directory"""
        html_files = list(html_dir.glob("*.html"))

        if not html_files:
            logger.warning(f"No HTML files found in {html_dir}")
            return {}

        logger.info(f"Analyzing {len(html_files)} HTML files in {html_dir}")

        batch_results = {
            'summary': {
                'total_files': len(html_files),
                'analyzed': 0,
                'with_cadri_content': 0,
                'with_documents': 0,
                'with_errors': 0
            },
            'files': []
        }

        for html_file in html_files:
            try:
                analysis = self.analyze_file(html_file)
                batch_results['files'].append(analysis)
                batch_results['summary']['analyzed'] += 1

                # Update counters
                if analysis['text_analysis']['keyword_counts']['cadri'] > 0:
                    batch_results['summary']['with_cadri_content'] += 1

                total_docs = sum(len(table['potential_documents']) for table in analysis['table_analysis'])
                if total_docs > 0:
                    batch_results['summary']['with_documents'] += 1

                if analysis['text_analysis']['has_error_messages']:
                    batch_results['summary']['with_errors'] += 1

            except Exception as e:
                logger.error(f"Error analyzing {html_file}: {e}")
                continue

        # Save batch report
        batch_report_file = html_dir / "batch_analysis_report.json"
        with open(batch_report_file, 'w', encoding='utf-8') as f:
            json.dump(batch_results, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Batch analysis complete. Report saved: {batch_report_file}")
        return batch_results


def main():
    """Test HTML analyzer"""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze HTML files for CADRI extraction debugging")
    parser.add_argument('--file', help='Single HTML file to analyze')
    parser.add_argument('--dir', help='Directory containing HTML files to analyze')

    args = parser.parse_args()

    analyzer = HTMLAnalyzer()

    if args.file:
        html_file = Path(args.file)
        if html_file.exists():
            analyzer.generate_report(html_file)
        else:
            print(f"File not found: {html_file}")

    elif args.dir:
        html_dir = Path(args.dir)
        if html_dir.exists():
            analyzer.batch_analyze(html_dir)
        else:
            print(f"Directory not found: {html_dir}")

    else:
        print("Please specify --file or --dir")


if __name__ == "__main__":
    main()