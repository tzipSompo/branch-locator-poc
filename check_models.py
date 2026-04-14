import os
import ssl
from google import genai
from dotenv import load_dotenv

load_dotenv()

# הגדרת SSL לטובת ה-Proxy המשרדי
os.environ['PYTHONHTTPSVERIFY'] = '0'
ssl._create_default_https_context = ssl._create_unverified_context

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

print("--- Available Models ---")
try:
    # כאן השתמשנו ב-supported_actions כפי שהשגיאה הציעה
    for model in client.models.list():
        print(f"Name: {model.name}, Actions: {model.supported_actions}")
except Exception as e:
    print(f"❌ Error listing models: {e}")