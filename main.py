import os
import asyncio
import json
import threading
from queue import Queue
from unittest import result

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


@app.get("/sample")
async def sample():
    # simple sample endpoint to demonstrate Deepgram API key works
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


@app.get("/connect-signature")
async def connect_signature():

    import inspect

    deepgram = DeepgramClient(
        api_key=DEEPGRAM_API_KEY
    )

    return {
        "signature": str(
            inspect.signature(
                deepgram.listen.v1.connect
            )
        )
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    loop = asyncio.get_running_loop()

    await websocket.send_text(json.dumps({
        "type": "status",
        "message": "Connecting to Deepgram..."
    }))

    try:
        deepgram = DeepgramClient(
            api_key=DEEPGRAM_API_KEY
        )

        with deepgram.listen.v1.connect(
            model="nova-3",
            language="en",
            smart_format=True,
            interim_results=True,
        ) as dg:

            ready = threading.Event()

            # -----------------------------
            # Deepgram connection opened
            # -----------------------------
            def on_open(_):
                print("Deepgram connected")
                ready.set()

            # -----------------------------
            # Transcript event
            # -----------------------------
            def on_message(result):
                print("EVENT:", getattr(result, "type", None))
                print(result)
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

                    print(payload)

                    # Send transcript back to browser
                    asyncio.run_coroutine_threadsafe(
                        websocket.send_text(
                            json.dumps(payload)
                        ),
                        loop
                    )

                except Exception as e:
                    print("Transcript Error:", e)

            # -----------------------------
            # Error event
            # -----------------------------
            def on_error(error):
                print("Deepgram Error:", error)

            # -----------------------------
            # Register events
            # -----------------------------
            dg.on(
                EventType.OPEN,
                on_open
            )

            dg.on(
                EventType.MESSAGE,
                on_message
            )

            try:
                dg.on(
                    EventType.ERROR,
                    on_error
                )
            except:
                pass

            # -----------------------------
            # Start listener thread
            # -----------------------------
            listener_thread = threading.Thread(
                target=dg.start_listening,
                daemon=True
            )

            listener_thread.start()

            # Wait for Deepgram websocket
            ready.wait(timeout=10)

            if not ready.is_set():

                await websocket.send_text(
                    json.dumps({
                        "type": "error",
                        "message": "Failed to connect to Deepgram"
                    })
                )

                return

            await websocket.send_text(
                json.dumps({
                    "type": "status",
                    "message": "Deepgram connected"
                })
            )

            print("Ready for microphone audio")

            # -----------------------------
            # Keep-alive thread
            # -----------------------------
            def keep_alive():

                while True:

                    try:
                        dg.send_keep_alive()
                    except:
                        break

                    import time
                    time.sleep(5)

            threading.Thread(
                target=keep_alive,
                daemon=True
            ).start()

            # -----------------------------
            # Receive browser audio
            # -----------------------------
            while True:

                chunk = await websocket.receive_bytes()

                print(
                    f"Audio chunk: {len(chunk)} bytes"
                )

                dg.send_media(chunk)

    except WebSocketDisconnect:
        print("Browser disconnected")

    except Exception as e:

        print("WebSocket Error:", e)

        try:
            await websocket.send_text(
                json.dumps({
                    "type": "error",
                    "message": str(e)
                })
            )
        except:
            pass

    finally:
        try:
            dg.send_finalize()
        except:
            pass
