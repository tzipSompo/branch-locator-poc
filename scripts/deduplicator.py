import json
import asyncio
import re
import vertexai
import os
import ssl
import streamlit as st
from rapidfuzz import fuzz
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.oauth2 import service_account

class BranchDeduplicator:
    def __init__(self, extractor, threshold=82):
        self.extractor = extractor
        self.threshold = threshold
        
        
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
        self.llm = GenerativeModel(self.model_id)

    def _clean(self, text):
        """ניקוי תווים לפני שליחה ל-LLM למניעת בעיות Matching"""
        if not text: return ""
        # הסרת גרשיים מכל הסוגים ומקפים
        return re.sub(r'[\"\'\-]', '', str(text)).strip() 
    
    async def _get_canonical_map(self, items: list, item_type: str):
        if not items: return {}
        unique_items = list(set(self._clean(i) for i in items if i))
    
        prompt = (
            f"You are a master data cleaner for Israeli retail chains.\n"
            f"Task: Map all variations of {item_type} to one standard version.\n"
            f"Rules:\n"
            f"1. For COMPANIES: Always map to the standard **ENGLISH** name (e.g., 'Aroma', 'Fox Home', 'Super-Pharm').\n"
            f"2. For CITIES: Always map to the standard **ENGLISH** name (e.g., 'Tel Aviv', 'Jerusalem', 'Haifa').\n"
            f"3. Map ALL variations (English, typos, partial names) to these EXACT standards.\n"
            f"4. Do not include branch names or addresses in this mapping.\n"
            f"\nList to process: {unique_items}\n"
            f"Return ONLY a flat JSON object where the key is the input and the value is the standard name."
        )
    
        try:
            response = await asyncio.to_thread(
                self.llm.generate_content,
                prompt,
                generation_config=GenerationConfig(response_mime_type="application/json")
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"⚠️ Normalization failed: {e}")
            return {}

    async def _judge_pair(self, b1, b2):
        prompt = (
            "Decide if these two are the EXACT same physical store branch.\n"
            f"Rules:\n"
            f"1. If one address contains the other (e.g., 'Weizman 14' and 'Weizman 14, Mall'), they are the SAME.\n"
            f"2. Hebrew and English versions of the same street are the SAME.\n"
            f"3. Ignore 'Paz', 'Gas station', 'Mall' - focus only on street name and number.\n"
            f"\nBranch A: {b1.branch_name} at {b1.address}, {b1.city}\n"
            f"Branch B: {b2.branch_name} at {b2.address}, {b2.city}\n"
            f'Respond ONLY with JSON: {{"is_same": true/false, "merged_address": "Standard Hebrew Address", "merged_name": "Clean Branch Name"}}'
        )
    
        try:
            response = await asyncio.to_thread(
                self.llm.generate_content,
                prompt,
                generation_config=GenerationConfig(response_mime_type="application/json")
            )
            return json.loads(response.text)
        except:
            return {"is_same": False}

    async def _process_group(self, group):
        """מנקה כפילויות בתוך קבוצה (חברה + עיר)"""
        unique_in_group = []
        for candidate in group:
            found_match = False
            for existing in unique_in_group:
                # ניקוי לצורך השוואה בלבד
                score = fuzz.token_set_ratio(self._clean(candidate.address), self._clean(existing.address))
                
                if score > 94:
                    found_match = True
                    break
                # אם הציון נמוך (נניח בגלל אנגלית/עברית), אנחנו עדיין שואלים את ה-AI 
                # אם הסף (threshold) שלך גבוה מדי, הוא ידלג על הכרעת ה-AI
                elif score > 60: 
                    decision = await self._judge_pair(candidate, existing)
                    if decision.get("is_same"):
                        existing.address = decision.get('merged_address', existing.address)
                        found_match = True
                        break
            
            if not found_match:
                unique_in_group.append(candidate)
        return unique_in_group

    async def deduplicate(self, branches: list) -> list:
        if not branches: return []

        # 1. ניקוי ונירמול
        for b in branches:
            b.company = self._clean(b.company)
            b.city = self._clean(b.city)
            b.address = self._clean(b.address)

        print("🔧 Normalizing Brands and Cities...")
        comp_task = self._get_canonical_map([b.company for b in branches], "company")
        city_task = self._get_canonical_map([b.city for b in branches if b.city], "city")
        comp_map, city_map = await asyncio.gather(comp_task, city_task)
        
        for b in branches:
            b.company = comp_map.get(b.company, b.company)
            if b.city:
                b.city = city_map.get(b.city, b.city)

        # 2. קיבוץ (Grouping) - התיקון הקריטי כאן!
        groups = {}
        for b in branches:
            # אנחנו יוצרים מפתח חיפוש חכם:
            # אם כתוב 'Tel Aviv' או 'תל אביב', אנחנו רוצים שהם יהיו באותו Key.
            # ניקח רק את המילה הראשונה של העיר (למשל 'Tel' או 'תל') כדי לאחד 'Tel Aviv Yafo' ו-'Tel Aviv'
            city_part = str(b.city).split()[0].upper() if b.city else "UNKNOWN"
            key = (str(b.company).upper(), city_part)
            
            if key not in groups:
                groups[key] = []
            groups[key].append(b)

        # 3. ניקוי מקבילי
        print(f"Sweep: Processing {len(groups)} groups...")
        tasks = [self._process_group(group) for group in groups.values()]
        results = await asyncio.gather(*tasks)
        
        return [branch for city_list in results for branch in city_list]
