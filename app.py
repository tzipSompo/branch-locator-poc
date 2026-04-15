import streamlit as st
import asyncio
import pandas as pd
import os
import re
from google.oauth2 import service_account
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from scripts.deduplicator import BranchDeduplicator

# הגדרת דף ועיצוב
st.set_page_config(page_title="AI Branch Locator", page_icon="📍", layout="wide")

st.title("📍 AI Branch Locator & Deduplicator")

# --- תפריט צד (Sidebar) ---
st.sidebar.header("⚙️ הגדרות")
companies_input = st.sidebar.text_input("חברות לחיפוש", "ארומה, סופר-פארם")
cities_input = st.sidebar.text_area("ערים לחיפוש", "תל אביב, ירושלים")
threshold = st.sidebar.slider("רגישות ניקוי", 70, 95, 82)

def get_gcp_credentials():
    if "GCP_SERVICE_ACCOUNT" not in st.secrets:
        st.error("שגיאה: לא נמצא GCP_SERVICE_ACCOUNT ב-Secrets!")
        st.stop()
    
    try:
        # המרה בטוחה למילון
        creds_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        
        if "private_key" in creds_info:
            # תיקון Private Key - ה-replace הכפול קריטי
            creds_info["private_key"] = creds_info["private_key"].strip().replace("\\n", "\n")
        
        # שימוש בגישה ישירה למחלקה כדי לעקוף בעיות ב-Python 3.14
        from google.oauth2.service_account import Credentials
        return Credentials.from_info(creds_info)
    except Exception as e:
        st.error(f"שגיאה בטעינת הרשאות: {e}")
        st.stop()

# --- פונקציית הלוגיקה (נקייה מ-Streamlit UI) ---
async def run_logic(companies, cities, threshold, status_placeholder, progress_bar):
    # אתחול
    searcher = CompanySearcher()
    extractor = BranchExtractor()
    deduplicator = BranchDeduplicator(extractor=extractor, threshold=threshold)
    
    all_extracted = []
    
    for i, company in enumerate(companies):
        # עדכון UI דרך ה-Placeholder שהעברנו
        status_placeholder.info(f"🔍 מחפש סניפים עבור: **{company}**...")
        
        # שלב 1: חיפוש
        search_results = await asyncio.to_thread(searcher.search_company_branches, company, cities)
        
        # שלב 2: חילוץ
        status_placeholder.info(f"🧠 מחלץ נתונים עבור: **{company}**...")
        branches = await extractor.extract_branches(company, search_results)
        all_extracted.extend(branches)
        
        progress_bar.progress((i + 1) / len(companies))
    
    status_placeholder.info(f"🧹 מנקה כפילויות עבור {len(all_extracted)} סניפים...")
    clean_branches = await deduplicator.deduplicate(all_extracted)
    
    return clean_branches

# --- ממשק המשתמש להפעלה ---
if st.button("🚀 התחל תהליך איתור ומיפוי"):
    if not companies_input or not cities_input:
        st.warning("נא להזין חברות וערים.")
    else:
        companies = [c.strip() for c in companies_input.split(",")]
        cities = [city.strip() for city in cities_input.split(",")]
        
        # יצירת אלמנטים של UI ב-Thread הראשי
        status_placeholder = st.empty() # שימוש ב-empty במקום container
        progress_bar = st.progress(0)
        
        # הרצת הלוגיקה
        try:
            # ב-Streamlit Cloud משתמשים ב-loop הקיים
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(run_logic(companies, cities, threshold, status_placeholder, progress_bar))
            
            if results:
                st.success(f"✅ נמצאו {len(results)} סניפים ייחודיים!")
                df = pd.DataFrame([b.model_dump() for b in results])
                
                # תצוגה והורדה
                st.subheader("📊 דוח סניפים סופי")
                st.dataframe(df, use_container_width=True)
                
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(label="📥 הורד CSV", data=csv, file_name="report.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ תקלה בהרצה: {e}")
