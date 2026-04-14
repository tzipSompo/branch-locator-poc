import requests
import os
import json
from dotenv import load_dotenv
import urllib3

# השורה הזו משתיקה את האזהרות הספציפיות על ה-SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

class CompanySearcher:
    def __init__(self):
        self.api_key = os.getenv("SERPER_API_KEY")
        self.proxy = os.getenv("HTTP_PROXY")  # ה-Proxy המשרדי שלך
        
        if not self.api_key:
            raise ValueError("SERPER_API_KEY is missing from .env")

    def search_company_branches(self, company_name, cities=None):
        """
        מחפש סניפים. אם רשימת ערים מסופקת, הוא יבצע חיפוש ממוקד לכל עיר.
        """
        # אם לא סיפקנו ערים, נבצע חיפוש כללי אחד
        search_queries = [f"{company_name} סניפים בישראל כתובות"]
        
        # אם סיפקנו ערים, נוסיף שאילתה לכל עיר
        if cities:
            search_queries = [f"{company_name} סניפים ב{city} כתובת מלאה" for city in cities]

        all_results = {
            "organic": [],
            "maps": []
        }

        # הגדרות ה-Proxy ל-Requests
        proxies = {
            "http": self.proxy,
            "https": self.proxy
        } if self.proxy else None

        for query in search_queries:
            
            payload = json.dumps({
                "q": query,
                "gl": "il",  # התמקדויות בישראל
                "hl": "iw",  # עברית כשפה מועדפת לתוצאות
                "num": 20    # בקשת מקסימום תוצאות מ-Serper
            })
            
            headers = {
                'X-API-KEY': self.api_key,
                'Content-Type': 'application/json'
            }

            try:
                # ביצוע החיפוש (עם verify=False בגלל ה-Proxy המשרדי)
                response = requests.post(
                    "https://google.serper.dev/search", 
                    headers=headers, 
                    data=payload,
                    proxies=proxies,
                    verify=False
                )
                
                results = response.json()
                
                # איסוף תוצאות "אורגניות" (Snippets מאתרים)
                if 'organic' in results:
                    all_results["organic"].extend(results['organic'])
                
                # איסוף תוצאות "מפות" (עסקים מקומיים)
                if 'maps' in results:
                    all_results["maps"].extend(results['maps'])

            except Exception as e:
                print(f"❌ Search error for query '{query}': {e}")

        # ניקוי כפילויות בסיסי (לפי לינק או כתובת במפות) כדי לא להעמיס על Gemini
        all_results["organic"] = self._deduplicate(all_results["organic"], "link")
        all_results["maps"] = self._deduplicate(all_results["maps"], "address")

        return all_results

    def _deduplicate(self, items, key):
        """פונקציית עזר למניעת כפילויות בתוצאות הגולמיות"""
        seen = set()
        unique_items = []
        for item in items:
            val = item.get(key)
            if val not in seen:
                seen.add(val)
                unique_items.append(item)
        return unique_items