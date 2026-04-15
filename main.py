import asyncio
import pandas as pd
import sys
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from typing import List
from scripts.deduplicator import BranchDeduplicator

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
MAX_CONCURRENT_COMPANIES = 4

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
        print(f"🔍 Starting deep search for: {company_name}")
        
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


def print_final_report(branches):
    if not branches:
        print("⚠️ No branches found.")
        return

    # הפיכה לדיקשנרי ומיון
    data = [b.model_dump() for b in branches]
    
    # מיון לפי חברה ואז עיר
    data.sort(key=lambda x: (x['company'], x['city'] or ""))

    current_company = None
    
    print("\n" + "="*60)
    print("🏆  FINAL CONSOLIDATED BRANCH LIST")
    print("="*60)

    for b in data:
        if b['company'] != current_company:
            current_company = b['company']
            print(f"\n📦 Company: {current_company}")
            print("-" * 30)
            count = 1
        
        name = b['branch_name'] or "סניף"
        address = b['address'] or "כתובת לא ידועה"
        city = b['city'] or ""
        
        print(f"{count:2}. {name} | {address}, {city}")
        count += 1
    
    print("\n" + "="*60)

async def main():
    # אתחול הכלים
    searcher = CompanySearcher()
    extractor = BranchExtractor()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COMPANIES)

    # רשימת החברות
    companies = ["Super-Pharm","Castro,","Fox","Aroma"]
    
    # יצירת משימות
    tasks = [process_company(name, searcher, extractor, semaphore) for name in companies]
    
    # הרצה והמתנה לכל התוצאות
    all_results_nested = await asyncio.gather(*tasks)
    
    # שיטוח הרשימה
    all_branches = [branch for company_list in all_results_nested for branch in company_list]

    # שלב 3: ניקוי כפילויות לוגי ושמירה
    if all_branches:
        print(f"\n🧹 Starting Deduplication on {len(all_branches)} items...")
        
        # 1. יצירת האובייקט (מעבירים לו את ה-extractor כדי שיוכל להשתמש ב-Gemini לשפיטה)
        deduplicator = BranchDeduplicator(extractor)
    
        # 2. הרצת הניקוי (שימי לב ל-await כי זה פונה ל-LLM במקרים גבוליים)
        clean_branches = await deduplicator.deduplicate(all_branches)
    
        print(f"✅ Deduplication complete! Remaining: {len(clean_branches)} branches.")
    
        #שלב 4 הדפסת רשימה מאוחדת
        print_final_report(clean_branches)

        df = pd.DataFrame([b.model_dump() for b in clean_branches])
        
        # שמירה לקובץ
        output_file = "found_branches_deep.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"🎊 Success! Saved {len(df)} unique branches to {output_file}")
    else:
        print("❌ No branches were found during the process.")

if __name__ == "__main__":
    asyncio.run(main())
    
