# README.md
# fastapi-asr

Minimal FastAPI WebSocket server that streams browser audio to Deepgram sandbox and returns transcripts as JSON.

## Local run

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
set DEEPGRAM_API_KEY=your_deepgram_key
uvicorn main:app --reload