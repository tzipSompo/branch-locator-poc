import streamlit as st
import asyncio
import pandas as pd
import os
import re
import json
from google.oauth2 import service_account
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from scripts.deduplicator import BranchDeduplicator

# הגדרת דף ועיצוב
st.set_page_config(page_title="AI Branch Locator", page_icon="📍", layout="wide")

st.title("📍 AI Branch Locator & Deduplicator")
st.markdown("מערכת חכמה לאיתור סניפים, חילוץ נתונים וניקוי כפילויות.")

# --- תפריט צד (Sidebar) ---
st.sidebar.header("⚙️ הגדרות")
companies_input = st.sidebar.text_input("חברות לחיפוש (מופרדות בפסיק)", "ארומה, סופר-פארם")
cities_input = st.sidebar.text_area("ערים לחיפוש", "תל אביב, ירושלים, חיפה")
threshold = st.sidebar.slider("רגישות ניקוי (Threshold)", 70, 95, 82)



def get_gcp_credentials():
    if "GCP_SERVICE_ACCOUNT" not in st.secrets:
        st.error("❌ לא נמצאו Secrets!")
        st.stop()
    
    try:
        # טעינה למילון
        creds_dict = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        
        # תיקון קריטי: החלפת מחרוזת ה-n\ בתו ירידת שורה אמיתי
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
        return service_account.Credentials.from_service_account_info(creds_dict)
            
    except Exception as e:
        st.error(f"⚠️ שגיאה בטעינת הרשאות: {e}")
        st.stop()

async def run_branch_pipeline(companies, cities, status_placeholder, progress_bar):
    """לוגיקת ההרצה - ללא קריאות ישירות ל-st.container/st.info"""
    credentials = get_gcp_credentials()
    
    # אתחול הרכיבים
    searcher = CompanySearcher()
    extractor = BranchExtractor()
    deduplicator = BranchDeduplicator(extractor=extractor, threshold=threshold)
    
    all_extracted = []
    
    for i, company in enumerate(companies):
        # עדכון UI דרך ה-Placeholder שהוכן מראש
        status_placeholder.info(f"🔍 מחפש סניפים עבור: **{company}**...")
        
        # שלב 1: חיפוש (Thread-safe)
        search_results = await asyncio.to_thread(searcher.search_company_branches, company, cities)
        
        # שלב 2: חילוץ נתונים
        status_placeholder.info(f"🧠 מחלץ נתונים עבור: **{company}**...")
        branches = await extractor.extract_branches(company, search_results)
        all_extracted.extend(branches)
        
        progress_bar.progress((i + 1) / len(companies))
    
    # שלב 3: ניקוי כפילויות
    status_placeholder.info(f"🧹 מנקה כפילויות עבור {len(all_extracted)} סניפים...")
    clean_branches = await deduplicator.deduplicate(all_extracted)
    
    return clean_branches

# --- ממשק המשתמש להפעלה ---
if st.button("🚀 התחל תהליך איתור ומיפוי"):
    if not companies_input or not cities_input:
        st.warning("נא להזין חברות וערים לחיפוש.")
    else:
        companies = [c.strip() for c in companies_input.split(",")]
        cities = [city.strip() for city in cities_input.split(",")]
        
        # יצירת אלמנטים של UI ב-Thread הראשי
        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        
        try:
            # הרצה בסביבה אסינכרונית מבוקרת
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                run_branch_pipeline(companies, cities, status_placeholder, progress_bar)
            )
            
            if results:
                status_placeholder.success(f"✅ נמצאו {len(results)} סניפים ייחודיים!")
                
                # הצגת התוצאות
                df = pd.DataFrame([b.model_dump() for b in results])
                st.subheader("📊 דוח סניפים סופי")
                st.dataframe(df, use_container_width=True)
                
                # כפתור הורדה
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label="📥 הורד תוצאות כ-CSV",
                    data=csv,
                    file_name="branches_report.csv",
                    mime="text/csv",
                )
        except Exception as e:
            st.error(f"❌ שגיאה בהרצת התהליך: {e}")
