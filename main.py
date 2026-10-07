import asyncio
import os
import hmac
import hashlib
import urllib.parse
from dotenv import load_dotenv

from hydrogram import Client, filters
from hydrogram.types import Message
from aiohttp import web

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8080")

SECRET_KEY = API_HASH.encode()

routes = web.RouteTableDef()
app = None

CHUNK_SIZE = 1024 * 1024

def generate_token(chat_id: int, message_id: int) -> str:
    """Genera una firma HMAC permanente para validar el enlace."""
    data = f"{chat_id}:{message_id}"
    return hmac.new(SECRET_KEY, data.encode(), hashlib.sha256).hexdigest()

def verify_token(chat_id: int, message_id: int, token: str) -> bool:
    """Valida la firma HMAC del enlace."""
    expected_token = generate_token(chat_id, message_id)
    return hmac.compare_digest(expected_token, token)

@routes.get("/")
async def handle_home(request):
    return web.Response(text="200 OK - Bot Streamer & Downloader Active", status=200)

# --- REPRODUCTOR HTML CON INTERFAZ MODERNA PLYR ---
@routes.get("/stream/{chat_id}/{message_id}")
async def handle_stream_player(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])
        token = request.query.get("token")

        if not token or not verify_token(chat_id, message_id, token):
            return web.Response(status=403, text="⛔ Enlace no válido.")

        video_src = f"/video/{chat_id}/{message_id}?token={token}"

        html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stream Player</title>
    <link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css" />
    <style>
        html, body {{
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            background-color: #0d0e12 !important;
            overflow: hidden;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        .player-wrapper {{
            width: 100vw;
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            background: #000;
        }}
        .plyr {{
            width: 100vw !important;
            height: 100vh !important;
            --plyr-color-main: #e50914;
        }}
        .plyr__video-wrapper {{
            height: 100vh !important;
        }}
        video {{
            object-fit: contain !important;
        }}
    </style>
</head>
<body>
    <div class="player-wrapper">
        <video id="player" playsinline autoplay>
            <source src="{video_src}" type="video/mp4" />
        </video>
    </div>

    <script src="https://cdn.plyr.io/3.7.8/plyr.js"></script>
    <script>
        document.addEventListener('DOMContentLoaded', () => {{
            const player = new Plyr('#player', {{
                controls: [
                    'play-large', 'play', 'progress', 'current-time', 
                    'duration', 'mute', 'volume', 'captions', 'settings', 
                    'pip', 'airplay', 'fullscreen'
                ],
                autoplay: true,
                hideControls: true,
                resetOnEnd: true
            }});
        }});
    </script>
</body>
</html>"""

        return web.Response(
            text=html_content, 
            content_type="text/html",
            headers={"Cache-Control": "no-cache"}
        )
    except Exception:
        return web.Response(status=400, text="Petición incorrecta.")

# --- TRANSMISIÓN DE BYTES CON SEGURIDAD ---
@routes.get("/video/{chat_id}/{message_id}")
async def handle_video_bytes(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])
        token = request.query.get("token")

        if not token or not verify_token(chat_id, message_id, token):
            return web.Response(status=403, text="Acceso denegado.")

        msg = await app.get_messages(chat_id, message_id)
        if not msg or (not msg.video and not msg.document and not msg.audio):
            return web.Response(status=404, text="Video no encontrado")

        media = msg.video or msg.document or msg.audio
        file_size = media.file_size
        mime_type = getattr(media, "mime_type", None) or "video/mp4"

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
                "X-Content-Type-Options": "nosniff",
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

# --- ENDPOINT DE DESCARGA DIRECTA DE ARCHIVOS ---
@routes.get("/download/{chat_id}/{message_id}")
async def handle_file_download(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])
        token = request.query.get("token")

        if not token or not verify_token(chat_id, message_id, token):
            return web.Response(status=403, text="Acceso denegado.")

        msg = await app.get_messages(chat_id, message_id)
        if not msg:
            return web.Response(status=404, text="Mensaje no encontrado")

        media = msg.document or msg.video or msg.audio or msg.voice or msg.photo
        if not media:
            return web.Response(status=404, text="No hay archivo adjunto en este mensaje")

        file_size = getattr(media, "file_size", 0)
        mime_type = getattr(media, "mime_type", None) or "application/octet-stream"

        file_name = getattr(media, "file_name", None)
        if not file_name:
            if msg.video:
                file_name = f"video_{message_id}.mp4"
            elif msg.audio:
                file_name = f"audio_{message_id}.mp3"
            elif msg.photo:
                file_name = f"photo_{message_id}.jpg"
            else:
                file_name = f"file_{message_id}.bin"

        safe_filename = urllib.parse.quote(file_name)

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
                "Content-Disposition": f"attachment; filename*=UTF-8''{safe_filename}",
                "Content-Range": f"bytes {from_bytes}-{to_bytes}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-cache",
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
    # in_memory=True evita la creación de archivos .session bloqueantes en contenedores Docker/Fly.io
    app = Client(
        "streamvideov1",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True
    )

    @app.on_message(filters.private)
    async def handle_private_messages(client: Client, message: Message):
        if message.video or message.document or message.audio or message.photo:
            chat_id = message.chat.id
            message_id = message.id
            token = generate_token(chat_id, message_id)
            
            stream_link = f"{SERVER_URL}/stream/{chat_id}/{message_id}?token={token}"
            download_link = f"{SERVER_URL}/download/{chat_id}/{message_id}?token={token}"
            
            response_text = "📁 **Opciones del archivo:**\n\n"
            
            if message.video or message.audio:
                response_text += f"🎬 **Ver en línea:**\n`{stream_link}`\n\n"
                
            response_text += f"📥 **Descarga Directa:**\n`{download_link}`"
            
            await message.reply_text(response_text)
        else:
            await message.reply_text(
                "👋 **¡Hola! Soy tu bot de streaming y descarga.**\n\n"
                "Envíame o reenvíame un **video o archivo** para generarte los enlaces."
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