הפרויקט נבנה כ-Concept of Proof (PoC) למשימת איתור סניפים ממקורות מגוונים באינטרנט (אתרים רשמיים, מפות, אינדקסים) ואיחודם לכדי רשימה מזוקקת ללא כפילויות.

🛠 טכנולוגיות
Language: Python 3.10+

Data Management: Pandas

AI/LLM: Gemini / OpenAI (Structured Output)

Search API: Serper.dev / Tavily

📂 מבנה הפרויקט
main.py: ה-Pipeline המרכזי.

scripts/searcher.py: מנוע חיפוש ואיתור URLs (שלב 1).

scripts/extractor.py: חילוץ נתונים מובנים מהטקסט (שלב 2).

scripts/deduplicator.py: ניקוי איחוד כפילויות (שלב 3).

⚙️ התקנה והרצה
יצירת סביבה וירטואלית:

Bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
התקנת תלויות:

Bash
pip install -r requirements.txt --proxy http://192.168.174.80:8080/ --trusted-host pypi.org --trusted-host files.pythonhosted.org
הגדרת משתני סביבה:
צרו קובץ .env והוסיפו את המפתחות הבאים:

Plaintext
SERPER_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
