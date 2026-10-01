import asyncio
import os
from dotenv import load_dotenv

from hydrogram import Client, filters
from hydrogram.types import Message
from aiohttp import web

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8080")

routes = web.RouteTableDef()
app = None

CHUNK_SIZE = 1024 * 1024

@routes.get("/")
async def handle_home(request):
    return web.Response(text="🤖 Bot Streamer Online 24/7", status=200)

# --- REPRODUCTOR HTML A PANTALLA COMPLETA ---
@routes.get("/stream/{chat_id}/{message_id}")
async def handle_stream_player(request):
    chat_id = request.match_info["chat_id"]
    message_id = request.match_info["message_id"]
    video_src = f"/video/{chat_id}/{message_id}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Video Stream</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                background-color: #000;
            }}
            body, html {{
                width: 100%;
                height: 100%;
                overflow: hidden;
                display: flex;
                justify-content: center;
                align-items: center;
            }}
            video {{
                width: 100vw;
                height: 100vh;
                object-fit: contain; /* Mantiene la proporción del video adaptándose al 100% de la ventana */
                outline: none;
            }}
        </style>
    </head>
    <body>
        <video controls autoplay name="media">
            <source src="{video_src}" type="video/mp4">
            Tu navegador no soporta la reproducción de video HTML5.
        </video>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type="text/html")

# --- TRANSMISIÓN DE BYTES DEL VIDEO ---
@routes.get("/video/{chat_id}/{message_id}")
async def handle_video_bytes(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])

        msg = await app.get_messages(chat_id, message_id)
        if not msg or (not msg.video and not msg.document):
            return web.Response(status=404, text="Video no encontrado")

        media = msg.video or msg.document
        file_size = media.file_size
        mime_type = media.mime_type or "video/mp4"

        range_header = request.headers.get("Range")
        from_bytes, to_bytes = 0, file_size - 1

        if range_header:
            bytes_range = range_header.replace("bytes=", "").split("-")
            from_bytes = int(bytes_range[0])
            if bytes_range[1]:
                to_bytes = int(bytes_range[1])

        length = to_bytes - from_bytes + 1

        chunk_offset = from_bytes // CHUNK_SIZE
        offset_difference = from_bytes - (chunk_offset * CHUNK_SIZE)

        response = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type": mime_type,
                "Content-Range": f"bytes {from_bytes}-{to_bytes}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
            },
        )

        await response.prepare(request)

        bytes_written = 0
        async for chunk in app.stream_media(msg, offset=chunk_offset):
            chunk_len = len(chunk)

            if offset_difference > 0:
                if chunk_len <= offset_difference:
                    offset_difference -= chunk_len
                    continue
                else:
                    chunk = chunk[offset_difference:]
                    offset_difference = 0

            to_write = min(len(chunk), length - bytes_written)
            await response.write(chunk[:to_write])
            await response.drain()
            bytes_written += to_write

            if bytes_written >= length:
                break

        return response
    except Exception as e:
        return web.Response(status=500, text=str(e))

async def main():
    global app
    app = Client("streamvideov1", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

    @app.on_message(filters.private)
    async def handle_private_messages(client: Client, message: Message):
        if message.video or message.document:
            chat_id = message.chat.id
            message_id = message.id
            stream_link = f"{SERVER_URL}/stream/{chat_id}/{message_id}"
            
            await message.reply_text(
                f"🎬 **Enlace de reproducción generado:**\n\n`{stream_link}`\n\n"
                f"Abre este enlace en tu navegador para ver el video."
            )
        else:
            await message.reply_text(
                "👋 **¡Hola! Soy tu bot de streaming.**\n\n"
                "Envíame o reenvíame un **video** para generarte el enlace de reproducción."
            )

    await app.start()
    me = await app.get_me()
    print(f"✅ Bot iniciado correctamente como: @{me.username}")

    server_app = web.Application()
    server_app.add_routes(routes)
    runner = web.AppRunner(server_app)
    await runner.setup()
    
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌐 Servidor web escuchando en http://0.0.0.0:{port}")

    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nBot detenido.")