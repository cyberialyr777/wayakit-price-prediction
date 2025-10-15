import re

def parse_volume_string(text_string):
    if not text_string:
        return None
    
    match = re.search(r'(\d+\.?\d*)\s*(ltr|ml|l|g|kg|liter|litre|liters|milliliters|grams|kilograms|oz|ounce|fl\s?oz|fluid\sounces?)\b', text_string, re.I)
    if not match:
        return None
        
    quantity = float(match.group(1))
    unit = match.group(2).lower().strip()
    normalized_value = quantity
    
    if 'milliliter' in unit or unit == 'ml':
        unit = 'ml'
    elif 'liter' in unit or unit == 'l' or unit == 'ltr':
        normalized_value = quantity * 1000
        unit = 'L'
    elif 'gram' in unit or unit == 'g':
        unit = 'g'
    elif 'kilogram' in unit or unit == 'kg':
        normalized_value = quantity * 1000
        unit = 'kg'
    elif 'oz' in unit or 'ounce' in unit:
        normalized_value = quantity * 29.5735
        unit = 'fl oz'

    return {'quantity': quantity, 'unit': unit, 'normalized': normalized_value}

def parse_count_string(text_string):
    if not text_string:
        return None

    patterns = [
        r'(\d+)\s*/\s*(box|pack|count)\b',
        r'(\d+)\s*(?:wet\s*)?(wipes|count|sheets|sachets|pack|pcs|pieces|pc)\b',
        r'\b(pack|box)\s*of\s*(\d+)',
        r'^(\d+)\s*(?:sanitizing\s*)?(wipes|count|sheets|sachets|pack|pcs|pieces|pc)\b'
    ]

    for pattern in patterns:
        match = re.search(pattern, text_string, re.I)
        if match:
            # Different patterns might have the number in group 1 or 2
            quantity_str = match.group(1) if match.group(1) and match.group(1).isdigit() else (match.group(2) if len(match.groups()) > 1 and match.group(2) and match.group(2).isdigit() else None)
            if quantity_str:
                quantity = int(quantity_str)
                return {'quantity': quantity, 'unit': 'units', 'normalized': quantity}
    
    return None

def parse_saco_count_string(text_string):
    if not text_string:
        return None
    
    match = re.search(r'(\d+)\s*-\s*(piece|wipes|rags)\b', text_string, re.I)
    if not match:
        return None
        
    quantity = int(match.group(1))
    return {'quantity': quantity, 'unit': 'units', 'normalized': quantity}

def parse_volume_with_multiplier(text_string):
    if not text_string:
        return None
    office_supply_match = re.search(
        r'\((\d+\.?\d*)\s+.*?\s*[xX]\s*(\d+\.?\d*)\s*(ltr|ml|l|liter|litre|liters|milliliters)\b.*\)',
        text_string, re.I
    )
    if office_supply_match:
        multiplier = float(office_supply_match.group(1))
        base_quantity = float(office_supply_match.group(2))
        unit_text = office_supply_match.group(3)
        total_quantity = multiplier * base_quantity
        
        final_data = parse_volume_string(f"{total_quantity} {unit_text}")
        if final_data:
            final_data['quantity'] = total_quantity
            return final_data
    gogreen_match = re.search(r'(\d+)\s*Pcs\s*[xX]\s*(\d+\.?\d*)\s*(ltr|ml|l|liter|litre|liters|milliliters)\b', text_string, re.I)
    if gogreen_match:
        multiplier = float(gogreen_match.group(1))
        base_quantity = float(gogreen_match.group(2))
        unit_text = gogreen_match.group(3)
        total_quantity = multiplier * base_quantity
        
        final_data = parse_volume_string(f"{total_quantity} {unit_text}")
        if final_data:
            final_data['quantity'] = total_quantity
            return final_data
    base_volume_data = parse_volume_string(text_string)
    if not base_volume_data:
        return None 

    multiplier = 1
    simple_multiplier_match = re.search(r'(\d+)\s*[xX]', text_string, re.I)
    if simple_multiplier_match:
        multiplier = int(simple_multiplier_match.group(1))

    total_quantity = base_volume_data['quantity'] * multiplier
    
    base_volume_data['quantity'] = total_quantity
    base_volume_data['normalized'] = base_volume_data['normalized'] * multiplier
    
    return base_volume_data

def extract_aerosense_units(text):
    match_pcs = re.search(r'\((\d+)\s*pcs\)', text)
    if match_pcs:
        return int(match_pcs.group(1))
    match_pack = re.search(r'(\d+)-pack\s*x\s*(\d+)', text)
    if match_pack:
        return int(match_pack.group(1)) * int(match_pack.group(2))

    return None