import json
import asyncio
import re
from rapidfuzz import fuzz
from langchain_google_genai import ChatGoogleGenerativeAI 
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

class BranchDeduplicator:
    def __init__(self, extractor, threshold=82):
        self.extractor = extractor
        self.threshold = threshold
        
        
        self.llm = ChatGoogleGenerativeAI(
            model=extractor.model_id, 
            temperature=0
        )
        self.parser = JsonOutputParser()

    def _clean(self, text):
        """ניקוי תווים לפני שליחה ל-LLM למניעת בעיות Matching"""
        if not text: return ""
        # הסרת גרשיים מכל הסוגים ומקפים
        return re.sub(r'[\"\'\-]', '', str(text)).strip() 
    
    async def _get_canonical_map(self, items: list, item_type: str):
        """Chain 1: נרמול שמות - הופך חברות לאנגלית וערים לעברית תקנית"""
        if not items: return {}
        
        # ניקוי הפריטים לפני השליחה כדי שהמפתח (Key) יתאים לטקסט המקורי המנוקה
        unique_items = list(set(self._clean(i) for i in items if i)) 
          
        prompt = ChatPromptTemplate.from_template(
            "You are a master data cleaner for Israeli retail chains.\n"
            "Task: Map all variations of {item_type} to one standard version.\n"
            "Rules:\n"
            "1. For COMPANIES: Always map to the standard **ENGLISH** name (e.g., 'Aroma', 'Fox Home', 'Super-Pharm').\n"
            "2. For CITIES: Always map to the standard **ENGLISH** name (e.g., 'תל אביב', 'ירושלים', 'חיפה').\n"
            "3. Map ALL variations (English, typos, partial names) to these EXACT standards.\n"
            "4. IMPORTANT: Do not include branch names or addresses in this mapping.\n"
            "\nList to process: {item_list}\n"
            "Return ONLY a flat JSON object where the key is the input and the value is the standard name."
        )
        
        chain = prompt | self.llm | self.parser
        
        try:
            return await chain.ainvoke({"item_type": item_type, "item_list": unique_items})
        except Exception as e:
            print(f"⚠️ Normalization failed: {e}")
            return {}

    async def _judge_pair(self, b1, b2):
        """Chain 2: שופט AI למקרים גבוליים"""
        prompt = ChatPromptTemplate.from_template(
            "Decide if these two are the EXACT same physical store branch.\n"
            "Rules:\n"
            "1. If one address contains the other (e.g., 'Weizman 14' and 'Weizman 14, Mall'), they are the SAME.\n"
            "2. Hebrew and English versions of the same street are the SAME (e.g. 'Kanfei Nesharim' and 'כנפי נשרים').\n"
            "3. Ignore 'Paz', 'Gas station', 'Mall', 'קניון' - focus only on street name and number.\n"
            "\nBranch A: {n1} at {a1}, {c1}\n"
            "Branch B: {n2} at {a2}, {c2}\n"
            "Respond ONLY with JSON: "
            "{{\"is_same\": true/false, \"merged_address\": \"Standard Hebrew Address\", \"merged_name\": \"Clean Branch Name\"}}"
        )

        chain = prompt | self.llm | self.parser
        try:
            return await chain.ainvoke({
                "n1": b1.branch_name, "a1": b1.address, "c1": b1.city,
                "n2": b2.branch_name, "a2": b2.address, "c2": b2.city
            })
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