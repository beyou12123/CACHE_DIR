import os
import re
import asyncio
import yt_dlp
import uuid
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sheets import add_log_entry, get_bot_config

# مجلد مؤقت للتحميلات (سيتم تنظيفه آلياً)
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# --- [ الدوال المساعدة ] ---

async def get_video_info(url):
    """استخراج معلومات الفيديو بإعدادات متقدمة لتجنب الحظر"""

    # 🔥 إصلاح YouTube Shorts تلقائياً
    if "youtube.com/shorts/" in url:
        try:
            video_id = url.split("/shorts/")[1].split("?")[0]
            url = f"https://www.youtube.com/watch?v={video_id}"
        except:
            pass

    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'format': 'best',

        # --- [ إعدادات التخفي والوصول المتقدم ] ---
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'referer': 'https://www.google.com/',
        'nocheckcertificate': True,

        # 🔥 تجاوز الحظر
        'geo_bypass': True,
        'geo_bypass_country': 'US',

        # 🔥 مهم جداً لليوتيوب 2026
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web']
            }
        },

        'socket_timeout': 30,
        'extract_flat': False,
        'noplaylist': True,

        # 🔥 (اختياري) إذا عندك كوكيز
        # 'cookiefile': 'cookies.txt',
    }

    try:
        def extract():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)

        return await asyncio.to_thread(extract)

    except Exception:
        print("⚠️ خطأ كامل في yt-dlp:")
        traceback.print_exc()
        return None


# --- [ معالجات البوت (Handlers) ] ---

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الترحيب وتعليمات الاستخدام"""
    bot_token = context.bot.token
    config = get_bot_config(bot_token)
    welcome_msg = config.get("الرسالة الترحيبية", "🚀 أهلاً بك في بوت التحميل الشامل!\n\nأرسل رابط الفيديو من (يوتيوب، تيك توك، إنستغرام، فيسبوك) وسأقوم بجلب خيارات التحميل لك فوراً.")
    await update.message.reply_text(welcome_msg)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المحرك الرئيسي للتعامل مع الروابط"""
    url = update.message.text.strip()

    if not re.match(r'http[s]?://', url):
        return

    msg = await update.message.reply_text("🔍 جاري فحص الرابط واستخراج الجودات المتاحة...")

    info = await get_video_info(url)

    if not info:
        await msg.edit_text("❌ عذراً، تعذر الوصول لمحتوى هذا الرابط. تأكد أنه عام وليس خاصاً.")
        return

    title = info.get('title', 'فيديو بدون عنوان')
    duration = info.get('duration_string', 'غير محدد')
    uploader = info.get('uploader', 'غير معروف')

    text = (
        f"✅ <b>تم العثور على الفيديو!</b>\n\n"
        f"📝 <b>العنوان:</b> {title}\n"
        f"👤 <b>الناشر:</b> {uploader}\n"
        f"⏱ <b>المدة:</b> {duration}\n\n"
        f"اختر صيغة التحميل المطلوبة أدناه:"
    )

    keyboard = []

    # تخصيص الأزرار بناءً على المنصة
    if 'tiktok.com' in url:
        keyboard.append([InlineKeyboardButton("🎬 فيديو (بدون علامة مائية)", callback_data=f"dl_tt_no_wm|{url}")])
    else:
        keyboard.append([InlineKeyboardButton("🎬 فيديو (أعلى جودة)", callback_data=f"dl_best_v|{url}")])

    keyboard.append([InlineKeyboardButton("🎵 صوت فقط (MP3)", callback_data=f"dl_audio|{url}")])

    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تنفيذ عملية التحميل والإرسال"""

    query = update.callback_query
    data = query.data
    action, url = data.split('|', 1)

    await query.answer("⏳ بدأت عملية المعالجة.. يرجى الانتظار")
    status_msg = await query.message.reply_text("📥 جاري التحميل من السيرفر الأصلي...")

    # 🔥 إصلاح YouTube Shorts مرة أخرى (احتياط)
    if "youtube.com/shorts/" in url:
        try:
            video_id = url.split("/shorts/")[1].split("?")[0]
            url = f"https://www.youtube.com/watch?v={video_id}"
        except:
            pass

    # توليد اسم ملف فريد
    file_id = str(uuid.uuid4())[:8]
    output_template = os.path.join(DOWNLOAD_DIR, f"{file_id}.%(ext)s")

    ydl_opts = {
        'outtmpl': output_template,
        'quiet': True,

        # 🔥 نفس إعدادات bypass
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

    if action == "dl_audio":
        ydl_opts.update({
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        })

    elif action == "dl_tt_no_wm":
        ydl_opts.update({
            'format': 'bestvideo+bestaudio/best'
        })

    else:
        ydl_opts.update({
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
        })

    try:
        def download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=True)

        info = await asyncio.to_thread(download)

        filename = None

        # 🔥 تحديد الملف الحقيقي بعد الدمج
        for ext in ['mp4', 'mkv', 'webm', 'mp3']:
            test_file = os.path.join(DOWNLOAD_DIR, f"{file_id}.{ext}")
            if os.path.exists(test_file):
                filename = test_file
                break

        if not filename:
            filename = ydl.prepare_filename(info)

        if action == "dl_audio":
            filename = filename.rsplit('.', 1)[0] + ".mp3"

        await status_msg.edit_text("📤 جاري الرفع إلى تليجرام...")

        with open(filename, 'rb') as f:
            if action == "dl_audio":
                await context.bot.send_audio(
                    chat_id=query.message.chat_id,
                    audio=f,
                    title=info.get('title')
                )
            else:
                await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=f,
                    caption=f"✅ {info.get('title')}\n\nتم التحميل بواسطة بوت المصنع."
                )

        await status_msg.delete()
        add_log_entry(context.bot.token, "DOWNLOAD_SUCCESS", f"Type: {action}")

    except Exception:
        traceback.print_exc()
        await status_msg.edit_text("❌ فشلت العملية بسبب خطأ داخلي في التحميل")

    finally:
        if 'filename' in locals() and filename and os.path.exists(filename):
            os.remove(filename)
