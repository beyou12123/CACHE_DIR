from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
# استيراد الوظائف اللازمة من sheets لضمان عمل التحقق من الإعدادات
from sheets import get_bot_config 

# --------------------------------------------------------------------------
# --- [ 1. دالة الترحيب (Start Handler) ] ---
# --------------------------------------------------------------------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    bot_token = context.bot.token
    
    # جلب إعدادات البوت لمعرفة آيدي المالك
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # بناء الأزرار: يظهر "الإعدادات" للمالك فقط
    if user_id == bot_owner_id:
        keyboard = [[
            InlineKeyboardButton("⚙️ الإعدادات", callback_data="lab_settings"),
            InlineKeyboardButton("📺 العرض", callback_data="lab_view")
        ]]
        text = "🧪 <b>مرحباً بك يا سيادة المطور</b>\nتم تفعيل موديول المختبر بنجاح."
    else:
        keyboard = [[InlineKeyboardButton("📺 العرض", callback_data="lab_view")]]
        text = "🧪 <b>مرحباً بك في موديول المختبر</b>\nيمكنك استعراض المحتوى المتاح."

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --------------------------------------------------------------------------
# --- [ 2. معالج ضغطات الأزرار (Callback Query Handler) ] ---
# --------------------------------------------------------------------------
async def contact_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """التحكم في كافة عمليات الضغط على الأزرار الشفافة Inline Buttons"""
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    bot_token = context.bot.token
    
    # جلب الإعدادات ومعرف المسؤول
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    await query.answer()

    # تنفيذ أوامر الأزرار
    if data == "lab_settings":
        await query.edit_message_text("⚙️ واجهة الإعدادات (للمسؤول فقط)")
    elif data == "lab_view":
        await query.edit_message_text("📺 واجهة عرض المحتوى")

# --------------------------------------------------------------------------
# --- [ 3. معالج الرسائل النصية (Message Handler) ] ---
# --------------------------------------------------------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    await update.message.reply_text(f"🔬 مختبر التحليل: استلمت ({text})")

# --------------------------------------------------------------------------
# --- [ 4. ربط المسميات للمحرك الديناميكي ] ---
# --------------------------------------------------------------------------
# هذا السطر هو الأهم لضمان استجابة البوت لأن main.py يبحث عن callback_handler
callback_handler = contact_callback_handler

