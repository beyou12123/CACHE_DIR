import g4f
from telegram import Update
from telegram.ext import ContextTypes
from sheets import get_bot_config

# قاموس لتخزين تاريخ المحادثات (مؤقت في الذاكرة)
# الهيكل: { user_id: [ قائمة الرسائل ] }
user_messages = {}

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # مسح الذاكرة القديمة عند الضغط على start لبدء صفحة جديدة
    user_messages[user_id] = []
    
    await update.message.reply_text(
        "🤖 أهلاً بك في النسخة المطورة! أنا الآن أملك ذاكرة وأستطيع فهم سياق حديثك مثل ChatGPT.\n\n"
        "كيف يمكنني مساعدتك اليوم؟"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_text = update.message.text
    if not user_text: return

    # 1. تهيئة الذاكرة للمستخدم إذا كانت فارغة
    if user_id not in user_messages:
        user_messages[user_id] = []

    # 2. إضافة رسالة المستخدم الجديدة للذاكرة
    user_messages[user_id].append({"role": "user", "content": user_text})

    # 3. إبقاء الذاكرة قصيرة (آخر 6 عناصر فقط: 3 أسئلة و 3 أجوبة) للحفاظ على الأداء
    if len(user_messages[user_id]) > 6:
        user_messages[user_id] = user_messages[user_id][-6:]

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        # 4. إرسال "تاريخ المحادثة" كاملاً وليس فقط الرسالة الأخيرة
        response = await g4f.ChatCompletion.create_async(
            model=g4f.models.default,
            messages=user_messages[user_id], # نرسل القائمة كاملة هنا
        )

        if response:
            # 5. إضافة رد البوت للذاكرة لكي يتذكره في المرة القادمة
            user_messages[user_id].append({"role": "assistant", "content": response})
            await update.message.reply_text(response)
        else:
            await update.message.reply_text("⚠️ المحرك لم يستجب، حاول مجدداً.")

    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في الذاكرة أو المحرك: {str(e)}")

# دالة لتنظيف الذاكرة (اختياري يمكن استدعاؤها عبر زر)
async def clear_memory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_messages[user_id] = []
    await update.message.reply_text("🧹 تم مسح ذاكرة المحادثة بنجاح.")