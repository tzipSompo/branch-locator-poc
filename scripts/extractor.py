import os
import ssl
import asyncio
from typing import List, Optional
from pydantic import BaseModel, Field
from google import genai
from dotenv import load_dotenv

load_dotenv()

# פתרון SSL לסביבה משרדית
os.environ['PYTHONHTTPSVERIFY'] = '0'
ssl._create_default_https_context = ssl._create_unverified_context

class Branch(BaseModel):
    company: str = Field(..., description="Company name")
    branch_name: Optional[str] = Field(None, description="Specific branch name")
    address: str = Field(..., description="Full street address")
    city: str = Field(..., description="City")
    source: str = Field(..., description="Source URL or google maps")

class ExtractionResponse(BaseModel):
    branches: List[Branch]

class BranchExtractor:
    def __init__(self):
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is missing")
        
        # חשוב: הורדנו את ה-http_options! 
        # ה-SDK החדש יבחר לבד את הגרסה הנכונה (v1beta) כשהוא יראה שיש Schema
        self.client = genai.Client(api_key=api_key)

    async def extract_branches(self, company_name: str, search_data: dict, max_retries: int = 3) -> List[Branch]:
        """
        מחלץ סניפים עם מנגנון Retry אוטומטי במקרה של עומס על השרת (503).
        """
        # בניית ה-Context
        context = f"Company: {company_name}\n\n"
        for place in search_data.get('maps', []):
            context += f"Map: {place.get('title')} at {place.get('address')}\n"
        for result in search_data.get('organic', []):
            context += f"Web: {result.get('snippet')} (Source: {result.get('link')})\n"

        prompt = f"Return a JSON list of all retail branches for {company_name} based on this data:\n{context}"

        # לוגיקת ה-Retry
        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model='gemini-flash-latest', #השתמשתי במודל זה כי במודלים אחרים מהר מאד הגעתי למכסה - ככה הוא מוצא לי את המודל הזמין . כמובן שבמצב אמיתי הייתי משתמשת במודל יציב ובתשלום
                    contents=prompt,
                    config={
                        'response_mime_type': 'application/json',
                        'response_schema': ExtractionResponse,
                    }
                )
                
                if response.parsed:
                    return response.parsed.branches
                return []

            except Exception as e:
                error_msg = str(e)
                
                # אם השגיאה היא עומס (503) או מכסה (429), נבצע המתנה וניסיון חוזר
                if ("503" in error_msg or "429" in error_msg) and attempt < max_retries - 1:
                    # חישוב זמן המתנה: 2 שניות, אז 4, אז 8...
                    wait_time = (2 ** (attempt + 1)) 
                    print(f"⏳ Server busy for {company_name}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                
                # אם זו שגיאה אחרת או שנגמרו הניסיונות
                print(f"❌ Extraction error for {company_name}: {e}")
                break 
                
        return []    



    