import requests
import os
import json
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

class CompanySearcher:
    def __init__(self):
        self.api_key = os.getenv("SERPER_API_KEY")
        self.proxy = os.getenv("HTTP_PROXY")
        
        if not self.api_key:
            raise ValueError("SERPER_API_KEY is missing from .env")

    def search_company_branches(self, company_name, cities=None):
        """
        מחפש סניפים באמצעות שאילתות באנגלית בלבד להגדלת כמות התוצאות.
        """
        search_queries = []
        
        if cities:
            for city in cities:
                # וריאציות באנגלית לכל עיר כדי "לתפוס" מקורות מידע שונים
                search_queries.append(f"{company_name} branches in {city} Israel full address")
                search_queries.append(f"{company_name} store locator {city} address")
        else:
            # חישוב כללי אם לא נבחרו ערים
            search_queries = [
                f"{company_name} branches in Israel list with addresses",
                f"{company_name} locations Israel official list"
            ]

        all_results = {
            "organic": [],
            "maps": []
        }

        # הגדרות ה-Proxy (נשאר ללא שינוי עבור המשרד)
        proxies = {
            "http": self.proxy,
            "https": self.proxy
        } if self.proxy else None

        for query in search_queries:
            
            payload = json.dumps({
                "q": query,
                "gl": "il",  # נשארים ממוקדים בישראל
                "hl": "en",  #    שפת ממשק החיפוש לאנגלית
                "num": 100   
            })
            
            headers = {
                'X-API-KEY': self.api_key,
                'Content-Type': 'application/json'
            }

            try:
                response = requests.post(
                    "https://google.serper.dev/search", 
                    headers=headers, 
                    data=payload,
                    proxies=proxies,
                    verify=False,
                    timeout=20
                )
                
                results = response.json()
                
                # איסוף תוצאות "אורגניות"
                if 'organic' in results:
                    all_results["organic"].extend(results['organic'])
                
                # איסוף תוצאות "מפות" (Maps)
                if 'maps' in results:
                    all_results["maps"].extend(results['maps'])

            except Exception as e:
                print(f"❌ Search error for query '{query}': {e}")

        # ניקוי כפילויות (לפי לינק לאורגני וכתובת למפות)
        all_results["organic"] = self._deduplicate(all_results["organic"], "link")
        all_results["maps"] = self._deduplicate(all_results["maps"], "address")

        print(f"✅ Total raw results found: {len(all_results['organic']) + len(all_results['maps'])}")
        return all_results

    def _deduplicate(self, items, key):
        """פונקציית עזר למניעת כפילויות בתוצאות הגולמיות"""
        seen = set()
        unique_items = []
        for item in items:
            val = item.get(key)
            if val and val not in seen:
                seen.add(val)
                unique_items.append(item)
        return unique_items