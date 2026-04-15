import streamlit as st
import asyncio
import pandas as pd
import os
import re
import json

# ייבוא כללי
import google.auth
from scripts.searcher import CompanySearcher
from scripts.extractor import BranchExtractor
from scripts.deduplicator import BranchDeduplicator

# הגדרת דף
st.set_page_config(page_title="AI Branch Locator", page_icon="📍", layout="wide")
st.title("📍 AI Branch Locator & Deduplicator")

def get_gcp_credentials():
    if "GCP_SERVICE_ACCOUNT" not in st.secrets:
        st.error("❌ לא נמצאו GCP_SERVICE_ACCOUNT ב-Secrets!")
        st.stop()
    
    try:
        # 1. המרה למילון נקי
        creds_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        
        # 2. ניקוי ה-Private Key (קריטי)
        if "private_key" in creds_info:
            creds_info["private_key"] = creds_info["private_key"].strip().replace("\\n", "\n")
        
        # 3. התיקון הקריטי: ייבוא ישיר ושימוש במתודה הסטנדרטית ביותר
        from google.oauth2 import service_account
        
        # אנחנו מנסים את שתי המתודות האפשריות כדי לוודא שזה יעבוד בכל גרסה
        if hasattr(service_account.Credentials, 'from_service_account_info'):
            return service_account.Credentials.from_service_account_info(creds_info)
        else:
            return service_account.Credentials.from_info(creds_info)
            
    except Exception as e:
        st.error(f"⚠️ שגיאה טכנית בטעינת ההרשאות: {e}")
        st.stop()

async def run_branch_pipeline(companies, cities, status_placeholder, progress_bar):
    credentials = get_gcp_credentials()
    
    searcher = CompanySearcher()
    extractor = BranchExtractor()
    # כאן אנחנו מעבירים את ה-threshold מה-sidebar
    deduplicator = BranchDeduplicator(extractor=extractor, threshold=st.session_state.get('threshold', 82))
    
    all_extracted = []
    
    for i, company in enumerate(companies):
        status_placeholder.info(f"🔍 מחפש סניפים עבור: **{company}**...")
        # הרצה ב-Thread נפרד כדי לא לחסום את ה-UI
        search_results = await asyncio.to_thread(searcher.search_company_branches, company, cities)
        
        status_placeholder.info(f"🧠 מחלץ נתונים עבור: **{company}**...")
        branches = await extractor.extract_branches(company, search_results)
        all_extracted.extend(branches)
        
        progress_bar.progress((i + 1) / len(companies))
    
    status_placeholder.info(f"🧹 מנקה כפילויות עבור {len(all_extracted)} סניפים...")
    clean_branches = await deduplicator.deduplicate(all_extracted)
    status_placeholder.success(f"✅ נמצאו {len(clean_branches)} סניפים ייחודיים!")
    
    return clean_branches

# --- ממשק המשתמש ---
companies_input = st.sidebar.text_input("חברות לחיפוש", "ארומה, סופר-פארם")
cities_input = st.sidebar.text_area("ערים לחיפוש", "תל אביב, ירושלים")
st.session_state['threshold'] = st.sidebar.slider("רגישות ניקוי", 70, 95, 82)

if st.button("🚀 התחל תהליך"):
    if companies_input and cities_input:
        companies = [c.strip() for c in companies_input.split(",")]
        cities = [city.strip() for city in cities_input.split(",")]
        
        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        
        try:
            # יצירת Loop חדש כדי למנוע התנגשות עם Streamlit
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(run_branch_pipeline(companies, cities, status_placeholder, progress_bar))
            
            if results:
                df = pd.DataFrame([b.model_dump() for b in results])
                st.subheader("📊 דוח סניפים סופי")
                st.dataframe(df, use_container_width=True)
                
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(label="📥 הורד CSV", data=csv, file_name="branches.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ שגיאה בהרצה: {e}")
