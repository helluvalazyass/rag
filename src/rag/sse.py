from fastapi import FastAPI
import asyncio
from fastapi.responses import StreamingResponse

app = FastAPI()

async def response_gen():
    message = "Hello, I am Manish Yadav"
    message_list = message.split(" ")
    while len(message_list) > 0:
        yield f"data: {message_list.pop(0)}\n\n"
        await asyncio.sleep(1)
    yield f"event: close\ndata: stream_finished\n\n"
    # return

@app.get('/stream')
async def stream_response():
    return StreamingResponse(response_gen(), media_type="text/event-stream")