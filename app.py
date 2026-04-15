import streamlit as st
import asyncio
import pandas as pd
import os
from google.oauth2 import service_account
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from scripts.deduplicator import BranchDeduplicator

# הגדרת דף ועיצוב
st.set_page_config(page_title="AI Branch Locator", page_icon="📍", layout="wide")

st.title("📍 AI Branch Locator & Deduplicator")
st.markdown("""
מערכת חכמה לאיתור סניפים, חילוץ נתונים וניקוי כפילויות. 
הערה: המערכת משתמשת ב-Gemini 2.5 Flash ובתהליכי עיבוד מקביליים לביצועים מיטביים.
""")

# --- תפריט צד (Sidebar) להגדרות ---
st.sidebar.header("⚙️ הגדרות")
companies_input = st.sidebar.text_input("חברות לחיפוש (מופרדות בפסיק)", "ארומה, סופר-פארם, פוקס הום")
cities_input = st.sidebar.text_area("ערים לחיפוש", "תל אביב, ירושלים, חיפה, ראשון לציון, באר שבע")
threshold = st.sidebar.slider("רגישות ניקוי (Threshold)", 70, 95, 82)

# --- פונקציית עזר לניהול הרשאות (Cloud vs Local) ---
def get_gcp_credentials():
    # בדיקה אם אנחנו בענן (Streamlit Secrets)
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        creds_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        # תיקון קטן למחרוזת ה-private key בגלל פורמט TOML
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_info(creds_info)
    
    # אם אנחנו מקומית - חיפוש הקובץ הקיים
    key_path = "gcp-key.json"
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path)
    
    return None

# --- לוגיקת ההרצה הראשית ---
async def run_branch_pipeline(companies, cities):
    # וידוא הרשאות
    credentials = get_gcp_credentials()
    if not credentials:
        st.error("❌ לא נמצאו הרשאות GCP (gcp-key.json או Secrets)")
        return None

    # אתחול הרכיבים
    searcher = CompanySearcher()
    extractor = BranchExtractor() # המודל יאותחל בתוך ה-Extractor
    deduplicator = BranchDeduplicator(extractor=extractor, threshold=threshold)
    
    all_extracted = []
    
    status_container = st.container()
    progress_bar = st.progress(0)
    
    for i, company in enumerate(companies):
        with status_container:
            st.info(f"🔍 מחפש סניפים עבור: **{company}**...")
            
            # שלב 1: חיפוש (Serper API)
            loop = asyncio.get_event_loop()
            search_results = await loop.run_in_executor(None, searcher.search_company_branches, company, cities)
            
            # שלב 2: חילוץ נתונים (Extractor)
            st.info(f"🧠 מחלץ נתונים עבור: **{company}**...")
            branches = await extractor.extract_branches(company, search_results)
            all_extracted.extend(branches)
            
            progress_bar.progress((i + 1) / len(companies))
    
    # שלב 3: ניקוי כפילויות (Deduplicator - Parallel)
    with status_container:
        st.info(f"🧹 מנקה כפילויות עבור {len(all_extracted)} סניפים שנמצאו...")
        clean_branches = await deduplicator.deduplicate(all_extracted)
        st.success(f"✅ נמצאו {len(clean_branches)} סניפים ייחודיים!")
        
    return clean_branches

# --- ממשק המשתמש להפעלה ---
if st.button("🚀 התחל תהליך איתור ומיפוי"):
    if not companies_input or not cities_input:
        st.warning("נא להזין חברות וערים לחיפוש.")
    else:
        companies = [c.strip() for c in companies_input.split(",")]
        cities = [city.strip() for city in cities_input.split(",")]
        
        # הרצת ה-Pipeline
        results = asyncio.run(run_branch_pipeline(companies, cities))
        
        if results:
            # הפיכה ל-DataFrame להצגה נוחה
            df = pd.DataFrame([b.model_dump() for b in results])
            
            # סידור עמודות
            cols = ['company', 'branch_name', 'city', 'address']
            df = df[cols] if all(c in df.columns for c in cols) else df
            
            st.subheader("📊 דוח סניפים סופי")
            st.dataframe(df, use_container_width=True)
            
            # אפשרות להורדה
            csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                label="📥 הורד תוצאות כ-CSV",
                data=csv,
                file_name="branches_report.csv",
                mime="text/csv",
            )