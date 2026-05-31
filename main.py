import os
import json
import threading
from queue import Queue

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from deepgram import DeepgramClient
from deepgram.core.events import EventType

load_dotenv()

app = FastAPI()

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")


@app.get("/")
async def home():
    return FileResponse("index.html")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "FastAPI + Deepgram"
    }


@app.get("/sample")
async def sample():
    """
    Simple Deepgram sample endpoint.

    Demonstrates that the API key works.
    Uses Deepgram-hosted audio.
    """

    try:

        client = DeepgramClient(api_key=DEEPGRAM_API_KEY)

        response = client.listen.v1.media.transcribe_url(
            url="https://static.deepgram.com/examples/Bueller-Life-moves-pretty-fast.wav",
            model="nova-3",
            language="en"
        )

        transcript = (
            response.results
            .channels[0]
            .alternatives[0]
            .transcript
        )

        return {
            "success": True,
            "transcript": transcript
        }

    except Exception as e:

        return {
            "success": False,
            "error": str(e)
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    await websocket.send_text(json.dumps({
        "type": "status",
        "message": "Connected to FastAPI"
    }))

    # Queue used to move audio chunks
    # from FastAPI async world
    # into Deepgram thread.
    audio_queue = Queue()

    try:
        deepgram = DeepgramClient(
            api_key=DEEPGRAM_API_KEY
        )

        with deepgram.listen.v1.connect(
            model="nova-3",
            language="en",
        ) as transcript_connection:
            ready_event = threading.Event()

            # Deepgram Open Event
            def on_open(_):
                ready_event.set()
                print("Deepgram connected")

            # Transcript Event
            def on_message(result):

                try:
                    channel = getattr(
                        result,
                        "channel",
                        None
                    )

                    if not channel:
                        return

                    transcript = (
                        channel
                        .alternatives[0]
                        .transcript
                    )

                    if not transcript:
                        return

                    payload = {
                        "type": "transcript",
                        "transcript": transcript,
                        "is_final": getattr(
                            result,
                            "is_final",
                            False
                        )
                    }

                    # Send transcript back to browser
                    import asyncio
                    asyncio.run(
                        websocket.send_text(
                            json.dumps(payload)
                        )
                    )

                except Exception as e:
                    print("Transcript Error:", e)

            # Register events
            transcript_connection.on(
                EventType.OPEN,
                on_open
            )

            transcript_connection.on(
                EventType.MESSAGE,
                on_message
            )

            # Start Deepgram connection
            transcript_connection.start_listening()

            # Background thread: forward audio chunks to deepgram
            def audio_sender():

                ready_event.wait()

                while True:

                    chunk = audio_queue.get()

                    if chunk is None:
                        break

                    transcript_connection.send_media(
                        chunk
                    )

            sender_thread = threading.Thread(
                target=audio_sender,
                daemon=True
            )

            sender_thread.start()

            await websocket.send_text(json.dumps({
                "type": "status",
                "message": "Deepgram connected"
            }))

            # Receive audio chunks from browser
            while True:
                chunk = await websocket.receive_bytes()
                audio_queue.put(chunk)

    except WebSocketDisconnect:
        print("Browser disconnected")
    except Exception as e:
        print(e)
        await websocket.send_text(
            json.dumps({
                "type": "error",
                "message": str(e)
            })
        )
    finally:
        try:
            audio_queue.put(None)
        except:
            pass
        print("Session ended")
