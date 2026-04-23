import logging
from telegram import ChatMember, ChatMemberUpdated, Bot # تم إضافة Bot لضمان الإرسال المركزي
from telegram.ext import ChatMemberHandler

import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sheets import get_bot_config, add_log_entry, update_content_setting, get_bot_users_count, get_bot_blocks_count

# إعداد التنبيهات لمراقبة الأداء التقني للبوت المصنوع
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --------------------------------------------------------------------------
# [مكان إضافة الدوال والوظائف البرمجية المستقبلية لبوت التواصل]
# مثل: دوال الحظر (Ban)، نظام الإحصائيات الخاص لكل بوت، أو الردود الآلية الذكية.
# --------------------------------------------------------------------------

def escape_markdown(text):
    """دالة تنظيف النصوص لتتوافق مع نظام MarkdownV2 الخاص بتليجرام"""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))

# --------------------------------------------------------------------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_token = context.bot.token
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))
    
    # جلب يوزر البوت الحالي
    bot_info = await context.bot.get_me()
    bot_username = f"@{bot_info.username}"

    # 1. إذا كان الداخل هو صاحب البوت
    if user.id == bot_owner_id:
        keyboard = [[InlineKeyboardButton("⚙️ لوحة تحكم البوت", callback_data="user_admin_panel")]]
        await update.message.reply_text("👋 أهلاً بك يا صاحب البوت في واجهتك الخاصة:", reply_markup=InlineKeyboardMarkup(keyboard))
    
    # 2. إذا كان مستخدماً جديداً
    else:
        # إشعار لصاحب البوت المصنوع
        total_members = get_bot_users_count(bot_token)

        owner_notif = (
            f"<b>تم دخول شخص جديد إلى البوت الخاص بك</b> 👾\n"
            f"-----------------------\n"
            f"• معلومات العضو الجديد .\n\n"
            f"• الاسم : {user.full_name}\n"
            f"• معرف : @{user.username if user.username else 'لا يوجد'}\n"
            f"• الايدي : <code>{user.id}</code>\n"
            f"-----------------------\n"
            f"• عدد الأعضاء الكلي : {total_members}"
        )
        try:
            await context.bot.send_message(chat_id=bot_owner_id, text=owner_notif, parse_mode="HTML")
        except: pass

        # --- [تصحيح: إشعار المطور يتم عبر بوت المصنع الأساسي] ---
        from main import ADMIN_ID, TOKEN
        factory_main_bot = Bot(TOKEN)
        
        factory_admin_notif = (
            f"<b>تم دخول شخص جديد إلى الصانع الخاص بك</b> 👾\n"
            f"-----------------------\n"
            f"• معلومات العضو الجديد .\n"
            f"يوزر البوت : {bot_username}\n"
            f"• الاسم : {user.full_name}\n"
            f"• معرف : @{user.username if user.username else 'لا يوجد'}\n"
            f"• الايدي : <code>{user.id}</code>\n"
            f"-----------------------\n"
            f"• عدد الأعضاء الكلي : {total_members}"
        )
        try:
            # هنا يتم الإرسال إلى حسابك عبر "بوت المصنع" وليس "البوت المصنوع"
            await factory_main_bot.send_message(chat_id=ADMIN_ID, text=factory_admin_notif, parse_mode="HTML")
        except: pass

        # إرسال الترحيب للمستخدم
        welcome_text = config.get("الرسالة الترحيبية", "مرحباً بك في بوت التواصل.")
        await update.message.reply_text(welcome_text)

# --------------------------------------------------------------------------
async def user_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """لوحة التحكم الخاصة بصاحب البوت المصنوع (أزرار شفافة)"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("📝 تعديل الترحيب", callback_data="edit_welcome_msg"),
         InlineKeyboardButton("📜 تعديل القوانين", callback_data="edit_rules_msg")],
        [InlineKeyboardButton("📊 إحصائيات البوت", callback_data="bot_stats"),
         InlineKeyboardButton("📢 إذاعة (Broadcast)", callback_data="bot_broadcast")],
        [InlineKeyboardButton("❌ إغلاق اللوحة", callback_data="close_panel")]
    ]
    
    await query.edit_message_text(
        "🛠 **لوحة تحكم بوت التواصل**\n\nاختر من القائمة أدناه لتعديل إعدادات بوتك مباشرة:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_contact_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """توجيه الرسائل من المستخدمين إلى صاحب البوت وبالعكس"""
    user = update.effective_user
    bot_token = context.bot.token
    
    # محاولة جلب المالك من الذاكرة أو الشيت
    bot_owner_id = context.bot_data.get("owner_id")
    if not bot_owner_id:
        config = get_bot_config(bot_token)
        bot_owner_id = int(config.get("admin_ids", 0))
        context.bot_data["owner_id"] = bot_owner_id

    # --- الحالة الأولى: صاحب البوت يرد على رسالة ---
    if user.id == bot_owner_id:
        # إذا كان المالك يكتب كلمة "التحكم" نظهر له اللوحة مرة أخرى
        if update.message.text == "التحكم":
            keyboard = [[InlineKeyboardButton("⚙️ لوحة تحكم البوت", callback_data="user_admin_panel")]]
            await update.message.reply_text("تفضل برابط اللوحة:", reply_markup=InlineKeyboardMarkup(keyboard))
            return

        if update.message.reply_to_message:
            try:
                original_msg = update.message.reply_to_message.text or update.message.reply_to_message.caption
                match = re.search(r"ID:\s*(\d+)", original_msg)
                
                if match:
                    target_user_id = match.group(1)
                    if update.message.text:
                        await context.bot.send_message(
                            chat_id=target_user_id,
                            text=f"💬 **رد من الإدارة:**\n\n{update.message.text}",
                            parse_mode="Markdown"
                        )
                    elif update.message.photo:
                        await context.bot.send_photo(
                            chat_id=target_user_id,
                            photo=update.message.photo[-1].file_id,
                            caption=f"💬 **رد من الإدارة:**\n\n{update.message.caption or ''}",
                            parse_mode="Markdown"
                        )
                    await update.message.reply_text("✅ تم إرسال ردك.")
                    add_log_entry(bot_token, "OWNER_REPLY", f"To user {target_user_id}")
                else:
                    await update.message.reply_text("❌ لم أجد ID المستخدم في الرسالة.")
            except Exception as e:
                await update.message.reply_text(f"❌ خطأ: {e}")
        else:
            # تنبيه المالك لاستخدام ميزة الرد
            if update.message.text and not update.message.text.startswith('/'):
                await update.message.reply_text("💡 للرد على شخص، قم بعمل (Reply) على رسالته.")

    # --- الحالة الثانية: مستخدم عادي يرسل للمالك ---
    else:
        try:
            info = f"📩 **رسالة جديدة**\n"
            info += f"من: {escape_markdown(user.full_name)}\n"
            info += f"ID: `{user.id}`"
            
            if update.message.text:
                await context.bot.send_message(bot_owner_id, f"{info}\n\n{update.message.text}", parse_mode="MarkdownV2")
            elif update.message.photo:
                await context.bot.send_photo(bot_owner_id, update.message.photo[-1].file_id, caption=info, parse_mode="MarkdownV2")
            
            await update.message.reply_text("✅ تم إرسال رسالتك، سيتم الرد عليك قريباً.")
            add_log_entry(bot_token, "USER_MSG", f"From {user.id}")
        except Exception as e:
            await update.message.reply_text("⚠️ فشل الإرسال حالياً.")

async def contact_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالج الأزرار الشفافة داخل بوت التواصل المصنوع"""
    query = update.callback_query
    data = query.data
    bot_token = context.bot.token

    if data == "user_admin_panel":
        await user_admin_panel(update, context)
    
    elif data == "edit_welcome_msg":
        await query.edit_message_text("📝 أرسل الآن رسالة الترحيب الجديدة التي تريدها:")
        context.user_data["waiting_for"] = "new_welcome"
    
    elif data == "edit_rules_msg":
        await query.edit_message_text("📜 أرسل الآن القوانين الجديدة:")
        context.user_data["waiting_for"] = "new_rules"

    elif data == "close_panel":
        await query.edit_message_text("🔒 تم إغلاق لوحة التحكم. أرسل كلمة 'التحكم' لفتحها مجدداً.")

# --------------------------------------------------------------------------
async def track_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تتبع عمليات الحظر وإعادة الاستخدام وإبلاغ المالك عبر البوت المصنوع"""
    result = update.my_chat_member
    user = result.from_user
    bot_token = context.bot.token
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # جلب عدد المحظورين
    total_blocks = get_bot_blocks_count(bot_token)

    # حالة الحظر (Blocked)
    if result.new_chat_member.status == ChatMember.BANNED:
        block_msg = (
            f"<b>🚫 قام أحد الأعضاء بحظر البوت الخاص بك</b>\n\n"
            f"معلومات العضو:\n"
            f"• الاسم: {user.full_name}\n"
            f"• اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n"
            f"• الآيدي: <code>{user.id}</code>\n\n"
            f"📊 إجمالي عدد المحادثات التي قامت بحظر البوت حتى الآن: {total_blocks}"
        )
        try: 
            await context.bot.send_message(chat_id=bot_owner_id, text=block_msg, parse_mode="HTML")
        except: pass

    # حالة إلغاء الحظر (Unblocked / Member)
    elif result.new_chat_member.status == ChatMember.MEMBER:
        unblock_msg = (
            f"<b>📶 قام مستخدم جديد بإعادة استخدام البوت الخاص بك مرة أخرى.</b>\n\n"
            f"👤 معلومات العضو:\n"
            f"• الاسم: {user.full_name}\n"
            f"• اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n"
            f"• الآيدي: <code>{user.id}</code>\n\n"
            f"📊 إجمالي عدد المحادثات الحالية: {total_blocks}"
        )
        try: 
            await context.bot.send_message(chat_id=bot_owner_id, text=unblock_msg, parse_mode="HTML")
        except: pass


# --------------------------------------------------------------------------
# [مكان إضافة معالجات الردود (Callbacks) المستقبلية]
# مثل: معالجة ضغطة زر الإحصائيات أو الإذاعة.
# --------------------------------------------------------------------------
