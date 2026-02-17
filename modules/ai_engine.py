import os, time
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv("config/.env")

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-1.5-flash")

def summarize(text):
    time.sleep(0.5)
    try:
        res = model.generate_content(text)
        return res.text
    except Exception as e:
        return f"AI Error: {e}"
