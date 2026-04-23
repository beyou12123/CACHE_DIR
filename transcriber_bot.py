import os
import asyncio
import uuid
import subprocess
import whisper
import yt_dlp
import traceback
from telegram import Update
from telegram.ext import ContextTypes, ApplicationBuilder, MessageHandler, CommandHandler, filters
from sheets import get_bot_config, add_log_entry

# تحميل النموذج بشكل آمن
model = None

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# --- [ الدوال الأصلية مع تحسينات داخلية فقط ] ---

def get_model():
    global model
    if model is None:
        model = whisper.load_model("base")
    return model

def convert_to_wav(input_file, output_file):
    try:
        result = subprocess.run([
            "ffmpeg", "-y", "-i", input_file,
            "-ar", "16000",
            "-ac", "1",
            output_file
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print("❌ FFmpeg Error:")
            print(result.stderr)
            raise Exception("فشل تحويل الصوت")

    except Exception:
        traceback.print_exc()
        raise

def transcribe_audio(file_path):
    try:
        mdl = get_model()
        result = mdl.transcribe(file_path, language="ar")
        return result["text"]
    except Exception:
        traceback.print_exc()
        raise

def download_from_youtube(url, output):

    # 🔥 إصلاح Shorts
    if "youtube.com/shorts/" in url:
        try:
            video_id = url.split("/shorts/")[1].split("?")[0]
            url = f"https://www.youtube.com/watch?v={video_id}"
        except:
            pass

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': output,
        'quiet': True,

        # 🔥 bypass حديث
        'geo_bypass': True,
        'geo_bypass_country': 'US',

        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web']
            }
        },

        'socket_timeout': 30,
        'noplaylist': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except Exception:
        traceback.print_exc()
        raise

# --- [ جسور الربط المتوافقة مع المصنع ] ---

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المعالج المتوافق مع main.py لأمر /start"""
    bot_token = context.bot.token
    config = get_bot_config(bot_token)

    welcome_text = config.get("الرسالة الترحيبية")
    if not welcome_text or welcome_text == "None":
        welcome_text = "🎤 أهلاً بك في بوت التفريغ الصوتي!\n\nأرسل رسالة صوتية، أو ملفاً صوتياً، أو رابط يوتيوب وسأقوم بتحويله إلى نص فوراً."

    await update.message.reply_text(welcome_text)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المحرك الرئيسي الذي يستدعيه المصنع لكافة الرسائل"""
    if update.message.voice:
        await process_voice_internal(update, context)

    elif update.message.document:
        await process_document_internal(update, context)

    elif update.message.text:
        await process_text_internal(update, context)

# --- [ الدوال التشغيلية ] ---

async def process_voice_internal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ جاري معالجة الرسالة الصوتية...")
    try:
        file = await update.message.voice.get_file()
        input_path = f"{DOWNLOAD_DIR}/{uuid.uuid4()}.ogg"
        wav_path = input_path.replace(".ogg", ".wav")

        await file.download_to_drive(input_path)

        convert_to_wav(input_path, wav_path)

        text = await asyncio.to_thread(transcribe_audio, wav_path)

        await msg.edit_text(f"📝 <b>التفريغ النصي:</b>\n\n{text}", parse_mode="HTML")

        add_log_entry(context.bot.token, "TRANSCRIPTION_SUCCESS", "Voice Note")

        os.remove(input_path)
        os.remove(wav_path)

    except Exception:
        traceback.print_exc()
        await msg.edit_text("❌ حدث خطأ أثناء معالجة الصوت")

async def process_document_internal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ جاري فحص الملف الصوتي...")
    try:
        file = await update.message.document.get_file()
        input_path = f"{DOWNLOAD_DIR}/{uuid.uuid4()}"
        wav_path = input_path + ".wav"

        await file.download_to_drive(input_path)

        convert_to_wav(input_path, wav_path)

        text = await asyncio.to_thread(transcribe_audio, wav_path)

        await msg.edit_text(f"📝 <b>تفريغ الملف:</b>\n\n{text}", parse_mode="HTML")

        add_log_entry(context.bot.token, "TRANSCRIPTION_SUCCESS", "Document/File")

        os.remove(input_path)
        os.remove(wav_path)

    except Exception:
        traceback.print_exc()
        await msg.edit_text("❌ خطأ في معالجة الملف")

async def process_text_internal(update: Update, context: ContextTypes.DEFAULT_TYPE):

    url = update.message.text.strip()

    if "youtube.com" not in url and "youtu.be" not in url:
        return

    msg = await update.message.reply_text("⏳ جاري جلب الصوت من يوتيوب وتفريغه...")

    try:
        file_id = str(uuid.uuid4())
        audio_path = f"{DOWNLOAD_DIR}/{file_id}.mp3"
        wav_path = f"{DOWNLOAD_DIR}/{file_id}.wav"

        await asyncio.to_thread(download_from_youtube, url, audio_path)

        convert_to_wav(audio_path, wav_path)

        text = await asyncio.to_thread(transcribe_audio, wav_path)

        await msg.edit_text(f"📝 <b>تفريغ فيديو يوتيوب:</b>\n\n{text}", parse_mode="HTML")

        add_log_entry(context.bot.token, "TRANSCRIPTION_SUCCESS", "YouTube Link")

        if os.path.exists(audio_path): os.remove(audio_path)
        if os.path.exists(wav_path): os.remove(wav_path)

    except Exception:
        traceback.print_exc()
        await msg.edit_text("❌ فشلت عملية تفريغ الرابط")

# --- [ نظام التشغيل المستقل - محمي من تجميد المصنع ] ---
if __name__ == "__main__":
    # هذا الجزء يعمل فقط إذا قمت بتشغيل الملف مباشرة للتجربة
    # ولكنه لن يعمل عند استيراده داخل المصنع لكي لا يسبب تعارض
    print("🚀 تشغيل البوت في وضع التجربة المستقلة...")
    app = ApplicationBuilder().token("YOUR_TOKEN_HERE").build()
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.VOICE, process_voice_internal))
    app.add_handler(MessageHandler(filters.Document.ALL, process_document_internal))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    app.run_polling()
