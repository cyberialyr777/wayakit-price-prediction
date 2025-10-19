import os
import json
import requests
import time
import boto3
import base64
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name="me-south-1"):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error al obtener el secreto '{secret_name}': {e}")
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)

class RelevanceAgent:
    def __init__(self):
        SECRET_NAME = "wayakit/test/credentials" 
        AWS_REGION = "me-south-1" 

        try:
            secrets = get_secret(SECRET_NAME, AWS_REGION)
            self.api_key = secrets.get('GEMINI_API_KEY')

            if not self.api_key:
                print("ERROR: GEMINI_API_KEY no encontrado en AWS Secrets Manager.")
            else:
                print("GEMINI_API_KEY cargada exitosamente desde AWS Secrets Manager.")

        except Exception as e:
            print(f"ERROR CRÍTICO: No se pudo cargar GEMINI_API_KEY desde Secrets Manager. {e}")
            self.api_key = None

        if not self.api_key:
            print("API KEY not found")
        self.relevance_api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key={self.api_key}"
        self.extraction_api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key={self.api_key}"
        
        self.headers = {'Content-Type': 'application/json'}

    def _get_prompt(self, product_name, search_query):
        return f"""
        You are a highly precise expert shopping assistant. Your task is to determine if a product title is a relevant and specific match for a user's search query. Your decisions must be strict.

        --- RULES ---

        ## General Rules:
        1.  **Tools vs. Cleaners:** If the query is for a liquid/spray cleaner (e.g., "glass cleaner"), you MUST REJECT cleaning tools (cloths, wipes, brushes). Only accept tools if the query explicitly asks for one (e.g., "disinfectant wipes").
        2.  **Context of Use:** If the query specifies an application (e.g., "for furniture", "marble", " hardwood"), you MUST REJECT products for a different application (e.g., "laundry," "dishes").
        3.  **Bundles and Promotions:** If the product title indicates a bundle, promo, or combo of DIFFERENT product types (e.g., "Glass Cleaner + Surface Disinfectant"), you MUST REJECT it. The product must be only what the user searched for.

        ## Specificity Rules:
        4.  **Specialized Surfaces:** If the query asks for a cleaner for a specific surface (e.g., "hardwood floor cleaner"), you MUST REJECT general-purpose or multi-surface cleaners. The product must be explicitly for that surface.
        5.  **Specialized Products:** If the query is for a specialized product (e.g., "wax and floor polish," "waterless car wash"), you MUST REJECT general cleaners. The product title must clearly indicate it performs that specific function.
        6.  **Automotive Focus:** If the query is for a car cleaning product (e.g., "microfiber for vehicle," "car disinfectant rags"), you MUST REJECT general-purpose products. The product must be explicitly marketed for automotive use.

        ## Final Instruction:
        Respond with only "Yes" or "No".

        --- EXAMPLES ---

        # Example (Tools vs. Cleaner)
        User Search Query: "glass cleaner"
        Product Title: "Microfiber cloth for glass"
        Is the product a relevant match for the query?
        No

        # Example (Bundles and Promotions)
        User Search Query: "glass cleaner"
        Product Title: "Go Green Promo Surface Cleaner 750 ML + Glass Cleaner 650 ML"
        Is the product a relevant match for the query?
        No

        # Example (Context of Use)
        User Search Query: "fabric freshener for furnitures"
        Product Title: "Loyal Fabric Softener & Freshener for Laundry"
        Is the product a relevant match for the query?
        No

        # Example (Specialized Surface)
        User Search Query: "hardwood floor cleaner"
        Product Title: "Mr. Clean Multi-Purpose Floor Cleaner"
        Is the product a relevant match for the query?
        No
        
        # Example (Specialized Product)
        User Search Query: "wax and floor polish"
        Product Title: "Pledge Floor Gloss, Polish and Wax"
        Is the product a relevant match for the query?
        Yes

        # Example (Automotive Focus)
        User Search Query: "car surface disinfectant wet rags"
        Product Title: "Lysol Disinfecting Wipes, Multi-Surface Lemon Scent"
        Is the product a relevant match for the query?
        No

        # Example (Automotive Focus)
        User Search Query: "microfiber for vehicle cleaning"
        Product Title: "Armor All Car Cleaning Microfiber Towel"
        Is the product a relevant match for the query?
        Yes
        
        # Example (Waterless Product)
        User Search Query: "waterless car wash"
        Product Title: "Meguiar's Gold Class Car Wash Shampoo & Conditioner"
        Is the product a relevant match for the query?
        No

        --- END EXAMPLES ---

        --- CURRENT TASK ---
        User Search Query: "{search_query}"
        Product Title: "{product_name}"

        Is the product a relevant match for the query?
        """

    def is_relevant(self, product_name, search_query):
        if not self.api_key:
            return False

        prompt = self._get_prompt(product_name, search_query)
        chat_history = [{"role": "user", "parts": [{"text": prompt}]}]
        payload = {"contents": chat_history}
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # CAMBIO 2: Se usa la URL específica para la relevancia (flash-lite).
                response = requests.post(self.relevance_api_url, headers=self.headers, data=json.dumps(payload))
                
                if response.status_code == 429:
                    print("      -> Rate limit hit. Waiting for 60 seconds to reset...")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                result = response.json()

                if result.get('candidates'):
                    decision = result['candidates'][0]['content']['parts'][0]['text'].strip().lower()
                    print(f"      -> IA decision: {decision}")
                    return "yes" in decision
                else:
                    print("      -> No candidates found in AI response.")
                    return False

            except requests.exceptions.RequestException as e:
                print(f"      -> Network error contacting AI agent: {e}")
                if attempt < max_retries - 1:
                    print(f"      -> Retrying in 10 seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(10)
                else:
                    return False
            except Exception as e:
                print(f"      -> Unexpected error processing AI response: {e}")
                return False
        
        print("      -> Failed to get a valid response from AI after multiple retries.")
        return False
    
    def extract_wipes_units(self, product_title: str) -> int:
        if not self.api_key:
            return 0

        # CAMBIO 1: El nuevo prompt que definimos arriba.
        prompt = f"""
You are an expert data extractor. Your goal is to calculate the TOTAL number of wipes from a product title. You must identify the base count per pack and any multipliers (like "Pack of 2", "3 Pack", "4x", etc.) and multiply them together.

First, provide a brief, one-sentence reasoning of your calculation. Then, provide the final integer.
Your final output MUST be a valid JSON object with two keys: "reasoning" and "total_units".

--- EXAMPLES ---

# Example 1: Standard multiplication
Title: "Clorox Disinfecting Bleach Free Cleaning Wipes, 75 Wipes, Pack Of 3"
{{
  "reasoning": "The title indicates 3 packs of 75 wipes each, so the total is 3 * 75.",
  "total_units": 225
}}

# Example 2: Multiplication with 'x'
Title: "Armor All Car Disinfectant Wipes, 30 Wipes Each, 3 Pack"
{{
  "reasoning": "The title specifies 3 packs containing 30 wipes each, so the total is 3 * 30.",
  "total_units": 90
}}

# Example 3: No multiplication needed
Title: "Antibacterial Wet Rags, 20 Sheets"
{{
  "reasoning": "The title mentions a single pack of 20 sheets with no multipliers.",
  "total_units": 20
}}

# Example 4: Complex multiplication
Title: "Family Pack 2 x (3 x 80 wipes)"
{{
  "reasoning": "The title shows a nested structure of 2 packs, each containing 3 packs of 80 wipes, so the total is 2 * 3 * 80.",
  "total_units": 480
}}

# Example 5: Product is not wipes
Title: "Microfiber Cloths 6 Pack"
{{
  "reasoning": "The product is 'Microfiber Cloths', which are not wipes. The count is irrelevant.",
  "total_units": 0
}}

--- CURRENT TASK ---

Title: "{product_title}"
"""

        chat_history = [{"role": "user", "parts": [{"text": prompt}]}]
        payload = {"contents": chat_history}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Se usa la URL de extracción (gemini-2.5-pro) como ya lo tenías
                response = requests.post(self.extraction_api_url, headers=self.headers, data=json.dumps(payload))

                if response.status_code == 429:
                    print("      -> Rate limit hit (units). Waiting for 60 seconds to reset...")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                result = response.json()

                if result.get('candidates'):
                    text_response = result['candidates'][0]['content']['parts'][0]['text'].strip()
                    
                    # CAMBIO 2: Parsear la respuesta como JSON en lugar de usar regex.
                    try:
                        # Limpiar la respuesta por si viene con formato de bloque de código markdown
                        if text_response.startswith("```json"):
                            text_response = text_response.strip("```json\n").strip("`")
                        
                        data = json.loads(text_response)
                        reasoning = data.get("reasoning", "No reasoning provided.")
                        total_units = int(data.get("total_units", 0))

                        print(f"      -> IA reasoning: {reasoning}")
                        print(f"      -> IA wipes units: {total_units}")
                        return total_units
                        
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        print(f"      -> Failed to parse JSON response: {e}")
                        print(f"      -> Raw response: '{text_response}'")
                        return 0
                else:
                    print("      -> No candidates found in AI response (units).")
                    return 0

            except requests.exceptions.RequestException as e:
                print(f"      -> Network error contacting AI agent (units): {e}")
                if attempt < max_retries - 1:
                    print(f"      -> Retrying in 10 seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(10)
                else:
                    return 0
            except Exception as e:
                print(f"      -> Unexpected error processing AI response (units): {e}")
                return 0

        print("      -> Failed to get a valid units response from AI after multiple retries.")
        return 0