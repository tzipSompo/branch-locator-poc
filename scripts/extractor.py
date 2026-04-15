import os
import ssl
import json
import asyncio
import vertexai
import streamlit as st
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.oauth2 import service_account
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

# --- הגדרות סכימה (Pydantic) ---
class Branch(BaseModel):
    company: str = Field(..., description="Company name")
    branch_name: str = Field("", description="Specific branch name, or empty string if not found")
    address: str = Field(..., description="Full street address")
    city: str = Field(..., description="City")
    source: str = Field(..., description="Source URL or google maps")

class ExtractionResponse(BaseModel):
    branches: List[Branch]

class BranchExtractor:
    def __init__(self):
        import warnings
        warnings.filterwarnings("ignore", message=".*deprecated.*") # משתיק את אזהרות יוני 2026
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(os.path.dirname(current_dir), "gcp-key.json")
        is_local = os.path.exists(key_path)

        if is_local:
            # --- מצב משרד (חייבים REST ודילוג SSL) ---
            proxy_url = "http://192.168.174.80:8080"
            os.environ.update({'HTTP_PROXY': proxy_url, 'HTTPS_PROXY': proxy_url, 'PYTHONHTTPSVERIFY': '0', 'GOOGLE_API_USE_REST': 'true'})
            ssl._create_default_https_context = ssl._create_unverified_context
            
            credentials = service_account.Credentials.from_service_account_file(key_path)
            project_id = os.getenv("GCP_PROJECT_ID")
            location = os.getenv("GCP_LOCATION", "us-central1")
            
            # כאן החזרתי את ה-api_transport שחיוני לפרוקסי שלך
            vertexai.init(project=project_id, location=location, credentials=credentials, api_transport="rest")
        else:
            # --- מצב ענן ---
            creds_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
            creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
            credentials = service_account.Credentials.from_info(creds_info)
            project_id = st.secrets["GCP_PROJECT_ID"]
            location = st.secrets.get("GCP_LOCATION", "us-central1")
            vertexai.init(project=project_id, location=location, credentials=credentials)

        self.model_id = "gemini-2.5-flash" 
        self.model = GenerativeModel(self.model_id)
    async def _process_chunk(self, company_name: str, chunk: List[str], i: int, max_retries: int) -> List[Branch]:
        """מעבד צ'אנק בודד עם מנגנון Retry פנימי"""
        chunk_context = "\n".join(chunk)
        prompt = f"""
        Task: Extract EVERY SINGLE branch of "{company_name}" found in the data below.
        Rules:
        1. If it looks like a branch, include it.
        2. Keep names and addresses in ENGLISH exactly as they appear.
        3. Do not summarize. Include mall names (BIG, Azrieli, etc.) in the address.
        Data:
        {chunk_context}
        """

        for attempt in range(max_retries):
            try:
                response = self.model.generate_content(
                    prompt,
                    generation_config=GenerationConfig(
                        response_mime_type='application/json',
                        response_schema={
                            "type": "object",
                            "properties": {
                                "branches": {
                                    "type": "array",
                                    "items": Branch.model_json_schema()
                                }
                            }
                        }
                    )
                )
                if response.text:
                    raw_data = json.loads(response.text)
                    return ExtractionResponse(**raw_data).branches
                return []

            except Exception as e:
                if any(err in str(e) for err in ["503", "429"]) and attempt < max_retries - 1:
                    wait_time = (2 ** (attempt + 1))
                    await asyncio.sleep(wait_time)
                    continue
                print(f"❌ Error in chunk {i+1}: {e}")
                return []
        return []

    async def extract_branches(self, company_name: str, search_data: dict, max_retries: int = 3) -> List[Branch]:
        # 1. איסוף פריטים
        raw_items = []
        for place in search_data.get('maps', []):
            raw_items.append(f"Map: {place.get('title')} at {place.get('address')}")
        for result in search_data.get('organic', []):
            raw_items.append(f"Web: {result.get('snippet')} (Source: {result.get('link')})")

        if not raw_items:
            return []

        # 2. חלוקה לצ'אנקים
        chunk_size = 30
        chunks = [raw_items[i:i + chunk_size] for i in range(0, len(raw_items), chunk_size)]
        
        print(f"📦 Extraction: Processing {len(raw_items)} items in {len(chunks)} parallel tasks...")

        # 3. הרצה מקבילית של כל הצ'אנקים! (שיפור מהירות משמעותי)
        tasks = [self._process_chunk(company_name, chunk, i, max_retries) for i, chunk in enumerate(chunks)]
        results = await asyncio.gather(*tasks)

        # 4. איחוד תוצאות ומניעת כפילויות בסיסית (לפי כתובת)
        all_extracted = []
        unique_addresses = set()

        for batch in results:
            for branch in batch:
                addr_key = f"{branch.address.strip().lower()}_{branch.city.strip().lower()}"
                if addr_key not in unique_addresses:
                    unique_addresses.add(addr_key)
                    all_extracted.append(branch)

        return all_extracted