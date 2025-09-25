import pymupdf
import re

def parse_residue_data(text):
    residue_pattern = r'(\d{2}\s+Resíduo\s*:.*?)(?=(?:\d{2}\s+Resíduo\s*:|$))'
    residue_sections = re.findall(residue_pattern, text, re.DOTALL)
    
    result = []
    
    for section in residue_sections:
        if not section.strip():
            continue
            
        residue_dict = {}
        lines = [line.strip() for line in section.split('\n') if line.strip()]
        
        current_field = None
        current_value = []
        acondicionamento_list = []
        
        # Define valid residue fields to avoid picking up irrelevant data
        valid_fields = [
            'Origem', 'Classe', 'Estado Físico', 'O/I', 'Qtde', 
            'Composição Aproximada', 'Método Utilizado', 
            'Cor, Cheiro, Aspecto', 'Destino', 'Acondicionamento'
        ]
        page_breaking = False
        for line in lines:
            if line == "GOVERNO DO ESTADO DE SÃO PAULO":
                page_breaking = True
                continue
            if re.match(r'^Pag\.\d+/\d+$', line):
                page_breaking = False
                continue
            if page_breaking:
                continue
            if re.match(r'^\d{2}\s+', line):
                match = re.match(r'^(\d{2})\s+Resíduo\s*:\s*(.+)', line)
                if match:
                    residue_dict['numero'] = match.group(1)
                    residue_dict['residuo'] = match.group(2).strip()
                continue
            
            # Check if it's a valid field header
            field_match = re.match(r'^([^:]+?)\s*:\s*(.*)$', line)
            field_name = field_match.group(1).strip() if field_match else None
            field_value = field_match.group(2).strip() if field_match else None
            if field_name and any(valid_field in field_name for valid_field in valid_fields):
                # Save previous field if exists
                if current_field and current_value:
                    field_key = current_field.lower().replace(' ', '_').replace(',', '')
                    if current_field == 'Acondicionamento':
                        acondicionamento_list.append(' '.join(current_value))
                    else:
                        residue_dict[field_key] = ' '.join(current_value)
                    
                # Start new field
                current_field = field_name
                current_value = [field_value] if field_value else []
                
                # Handle combined line (Classe : I  Estado Físico : SOLIDO  O/I : O     Qtde : 1  t / ano)
                if 'Classe' in current_field and 'Estado Físico' in line:
                    # Parse the combined line
                    classe_match = re.search(r'Classe\s*:\s*(\w+)', line)
                    estado_match = re.search(r'Estado Físico\s*:\s*(\w+)', line)
                    oi_match = re.search(r'O/I\s*:\s*([I/O]+)', line)
                    qtde_match = re.search(r'Qtde\s*:\s*(\d+(?:[.,]\d+)?)\s*(?:t|kg)(?:\s*/\s*(?:ano|mês|dia|semana|hora))?', line)
                    
                    if classe_match:
                        residue_dict['classe'] = classe_match.group(1)
                    if estado_match:
                        residue_dict['estado_fisico'] = estado_match.group(1)
                    if oi_match:
                        residue_dict['o_i'] = oi_match.group(1).strip()
                    if qtde_match:
                        # Extract unit from the line - now handles optional time units and comma decimals
                        unit_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(t|kg)(?:\s*/\s*(ano|mês|dia|semana|hora))?', line)
                        if unit_match:
                            amount = unit_match.group(1)
                            unit = unit_match.group(2)
                            time_unit = unit_match.group(3) if unit_match.group(3) else None
                            if time_unit:
                                residue_dict['quantidade'] = f"{amount} {unit}/{time_unit}"
                            else:
                                residue_dict['quantidade'] = f"{amount} {unit}"
                    
                    current_field = None
                    current_value = []
                
            elif current_field:
                # This is a continuation line (ignore lines that are all caps - likely headers)
                # Also ignore numeric-only lines that might be page numbers or process numbers
                if not re.match(r'^\d+$', line):
                    current_value.append(line)
        
        # Save the last field
        if current_field and current_value:
            field_key = current_field.lower().replace(' ', '_').replace(',', '')
            if current_field == 'Acondicionamento':
                acondicionamento_list.append(' '.join(current_value))
            else:
                residue_dict[field_key] = ' '.join(current_value)
        
        # Add acondicionamento list if exists
        if acondicionamento_list:
            residue_dict['acondicionamento'] = acondicionamento_list
            
        if residue_dict and 'numero' in residue_dict:  # Only add if it has a residue number
            result.append(residue_dict)
    
    return result

def parse_entity_data_final(text):
    """
    Final version of entity data parser.
    Extracts data between specific markers and maps fields correctly.
    Handles tab separators as null field indicators.
    """
    # Define the start and end markers
    start_marker = "CETESB, na Internet, no endereço: autenticidade.cetesb.sp.gov.br"
    end_marker = "Este certificado, composto de"
    
    # Find the section between markers
    start_idx = text.find(start_marker)
    end_idx = text.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        return {}
    
    # Extract the section and get lines
    section = text[start_idx + len(start_marker):end_idx].strip()
    lines = [line.strip() for line in section.split('\n') if line.strip()]
    
    # Process lines to handle tab characters
    processed_lines = []
    for line in lines:
        if '\t' in line:
            # Split by tab and add parts, with empty strings for tab positions
            parts = line.split('\t')
            for i, part in enumerate(parts):
                if i > 0:  # Add empty field for tab separator
                    processed_lines.append("")
                if part.strip():
                    processed_lines.append(part.strip())
        else:
            processed_lines.append(line)
    
    # Define field names in the exact order they appear
    field_names = [
        'GERADORA_Nome',
        'GERADORA_Cadastro na CETESB', 
        'GERADORA_Logradouro',
        'GERADORA_Número',
        'GERADORA_Complemento',
        'GERADORA_Bairro',  # Actually CEP in the data
        'GERADORA_CEP',     # Actually Município in the data
        'GERADORA_Município',
        'GERADORA_Descrição da Atividade',
        'GERADORA_Bacia Hidrográfica',
        'GERADORA_N° de Funcionários',
        'DESTINAÇÃO_Nome',
        'DESTINAÇÃO_Cadastro na CETESB',
        'DESTINAÇÃO_Logradouro', 
        'DESTINAÇÃO_Número',
        'DESTINAÇÃO_Complemento',
        'DESTINAÇÃO_Bairro',
        'DESTINAÇÃO_CEP',
        'DESTINAÇÃO_Município',
        'DESTINAÇÃO_Descrição da Atividade',
        'DESTINAÇÃO_Bacia Hidrográfica',
        'DESTINAÇÃO_N°LIC./CERT.FUNCION.',
        'DESTINAÇÃO_Data LIC./CERTIFIC.'
    ]
    
    # Create result dictionary
    entity_data = {}
    for i, field_name in enumerate(field_names):
        if i < len(processed_lines):
            entity_data[field_name] = processed_lines[i]
        else:
            entity_data[field_name] = ""
    
    return entity_data

def process_pdf(path):
  result_list = []
  doc = pymupdf.open(path) # open a document  
  pdf_text = ""
  initial_data = {}
  i = 0
  for page in doc: # iterate the document pages
    pdf_text += page.get_text() # get plain text encoded as UTF-8
    if i == 0:
        initial_data = parse_entity_data_final(pdf_text)
    i += 1

  for residuo in parse_residue_data(pdf_text):
    result_list.append({'processo': re.sub(r"[^\d]","", path), **residuo, **initial_data})
    
  return result_list

if __name__ == "__main__":
  import pandas as pd
  result = process_pdf("./7002144.pdf")
  pd.DataFrame(result).to_csv("output.csv", sep=";", encoding='utf-8-sig')
