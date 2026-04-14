import asyncio
import pandas as pd
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from typing import List

# הגדרת כמות חברות לעיבוד במקביל (מומלץ להשאיר 1 כדי לא לחרוג ממכסות ה-LLM והחיפוש)
MAX_CONCURRENT_COMPANIES = 1

# רשימת ערים לחיפוש ממוקד - תוכלי להוסיף או להסיר ערים לפי הצורך
CITIES_TO_SEARCH = [
    "תל אביב", "ירושלים", "חיפה", "ראשון לציון", 
    "פתח תקווה", "אשדוד", "נתניה", "באר שבע", "חולון"
]

async def process_company(company_name: str, searcher: CompanySearcher, extractor: BranchExtractor, semaphore: asyncio.Semaphore):
    """
    תהליך עיבוד לחברה בודדת: חיפוש מקיף לפי ערים -> חילוץ -> החזרת תוצאות
    """
    async with semaphore:
        print(f"🔍 Starting deep search for: {company_name} in {len(CITIES_TO_SEARCH)} cities...")
        
        # שלב 1: חיפוש (מעבירים את רשימת הערים ל-Searcher)
        # השתמשנו ב-asyncio.to_thread כי Requests היא ספריה סינכרונית
        search_results = await asyncio.to_thread(
            searcher.search_company_branches, 
            company_name, 
            cities=CITIES_TO_SEARCH
        )
        
        if not search_results or (not search_results.get('organic') and not search_results.get('maps')):
            print(f"⚠️ No results found for {company_name}")
            return []

        # שלב 2: חילוץ עם Gemini
        # כמות הנתונים עכשיו תהיה גדולה יותר בגלל החיפוש המורחב
        print(f"🤖 Extracting branches from deep search data for: {company_name}...")
        branches = await extractor.extract_branches(company_name, search_results)
        
        print(f"✅ Found {len(branches)} branches for {company_name}")
        return branches

async def main():
    # אתחול הכלים
    searcher = CompanySearcher()
    extractor = BranchExtractor()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COMPANIES)

    # רשימת החברות
    companies = ["Aroma", "Fox Home", "Super-Pharm"]
    
    # יצירת משימות
    tasks = [process_company(name, searcher, extractor, semaphore) for name in companies]
    
    # הרצה והמתנה לכל התוצאות
    all_results_nested = await asyncio.gather(*tasks)
    
    # שיטוח הרשימה
    all_branches = [branch for company_list in all_results_nested for branch in company_list]

    # שלב 3: ניקוי כפילויות לוגי ושמירה
    if all_branches:
        df = pd.DataFrame([b.model_dump() for b in all_branches])
        
        # ניקוי כפילויות שמבוסס על שילוב של חברה, עיר וכתובת (למקרה שאותו סניף עלה בכמה ערים)
        initial_count = len(df)
        df['address_clean'] = df['address'].str.lower().str.strip()
        df = df.drop_duplicates(subset=['company', 'city', 'address_clean'])
        df = df.drop(columns=['address_clean'])
        
        print(f"🧹 Deduplication: Removed {initial_count - len(df)} duplicate entries.")
        
        # שמירה לקובץ
        output_file = "found_branches_deep.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n🎊 Success! Saved {len(df)} unique branches to {output_file}")
    else:
        print("❌ No branches were found in the process.")

if __name__ == "__main__":
    asyncio.run(main())
    
