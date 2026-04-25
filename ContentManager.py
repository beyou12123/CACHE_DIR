# ContentManager.py
import json
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from sheets import get_bot_config, update_content_setting

# 1. قاموس الربط المركزي (تطابق كامل بين الكيبورد والأعمدة الـ 39)
# تم التأكد من عدم تغيير أي مفتاح أو مسمى عمود لضمان التكامل مع قاعدة البيانات
MAP = {
    "set_org_name": {"col": "اسم_المؤسسة", "label": "🏢 اسم المؤسسة"},
    "set_welcome_msg": {"col": "الرسالة الترحيبية", "label": "💬 الرسالة الترحيبية"},
    "set_rules": {"col": "القوانين", "label": "📜 القوانين"},
    "set_auto_reply": {"col": "auto_reply", "label": "🤖 الرد التلقائي"},
    "toggle_ai": {"col": "ai_enabled", "label": "🧠 تشغيل الذكاء AI"},
    "set_ai_prompt": {"col": "تعليمات_AI", "label": "⚙️ تعليمات AI"},
    "edit_welcome_morning": {"col": "welcome_morning", "label": "🌅 الصباحية"},
    "edit_welcome_noon": {"col": "welcome_noon", "label": "☀️ الظهرية"},
    "edit_welcome_evening": {"col": "welcome_evening", "label": "🌆 المسائية"},
    "edit_welcome_night": {"col": "welcome_night", "label": "🌃 الليلية"},
    "set_welcome_evening": {"col": "welcome_evening", "label": "🌆 ترحيب المساء"},
    "set_welcome_night": {"col": "welcome_night", "label": "🌙 ترحيب الليل"},
    "set_banned_words": {"col": "banned_words", "label": "🚫 الكلمات المحظورة"},
    "manage_admins": {"col": "admin_ids", "label": "👮‍♂️ الأدمن"},
    "set_buttons": {"col": "buttons", "label": "🔘 الأزرار"},
    "set_commands": {"col": "custom_commands", "label": "⌨️ الأوامر المخصصة"},
    "set_language": {"col": "language", "label": "🌐 اللغة"},
    "set_payment": {"col": "إعدادات_الدفع", "label": "💳 معلومات الدفع"},
    "set_delay": {"col": "delay_response", "label": "⏱️ وقت الرد"}
    
}

def get_main_config_keyboard():
    """
    توليد لوحة التحكم الرئيسية بتنسيق (2 في الصف و 3 في الصف للأزرار الصغيرة)
    بدون حذف أي زر أو تغيير التوزيع المعتمد.
    """
    keyboard = [
        [InlineKeyboardButton("🏢 اسم المؤسسة", callback_data="set_org_name"), InlineKeyboardButton("💬 الرسالة الترحيبية", callback_data="set_welcome_msg")],
        [InlineKeyboardButton("📜 القوانين", callback_data="set_rules"), InlineKeyboardButton("🤖 الرد التلقائي", callback_data="set_auto_reply")],
        [InlineKeyboardButton("🧠 تشغيل AI", callback_data="toggle_ai"), InlineKeyboardButton("⚙️ تعليمات AI", callback_data="set_ai_prompt")],
        [InlineKeyboardButton("🌅 الصباحية", callback_data="edit_welcome_morning"), InlineKeyboardButton("☀️ الظهرية", callback_data="edit_welcome_noon"), InlineKeyboardButton("🌆 المسائية", callback_data="edit_welcome_evening")],
        [InlineKeyboardButton("🌃 الليلية", callback_data="edit_welcome_night"), InlineKeyboardButton("🌆 ترحيب المساء", callback_data="set_welcome_evening"), InlineKeyboardButton("🌙 ترحيب الليل", callback_data="set_welcome_night")],
        [InlineKeyboardButton("🚫 المحظورات", callback_data="set_banned_words"), InlineKeyboardButton("👮‍♂️ الأدمن", callback_data="manage_admins")],
        [InlineKeyboardButton("🔘 الأزرار", callback_data="set_buttons"), InlineKeyboardButton("⌨️ الأوامر", callback_data="set_commands")],
        [InlineKeyboardButton("💳 معلومات الدفع", callback_data="set_payment")], 
        [InlineKeyboardButton("🌐 اللغة", callback_data="set_language"), InlineKeyboardButton("⏱️ وقت الرد", callback_data="set_delay")],
        [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==========================================================================
def get_setting_interface(bot_id, callback_data):
    """
    واجهة فحص الحالة (إضافة / تحديث / عودة)
    تطبق شروط التحقق الصارمة من وجود القيمة مسبقاً.
    """
    setting_info = MAP.get(callback_data)
    if not setting_info:
        return "⚠️ خطأ: الإعداد غير موجود في نظام الربط.", None

    # استثناء خاص بالرد التلقائي لفتح اللوحة الإدارية الديناميكية بدلاً من الواجهة الافتراضية
    if callback_data == "set_auto_reply":
        return "🤖 **إدارة الردود التلقائية**\nيمكنك إضافة كلمات مفتاحية جديدة أو مراجعة الكلمات الحالية:", get_auto_reply_manager_keyboard(bot_id)

    column_name = setting_info['col']
    label = setting_info['label']
    
    config = get_bot_config(bot_id)
    current_value = config.get(column_name, "") if config else ""

    exists = False
    if current_value and str(current_value).strip() not in ["", "-", "None", "[]", "{}", "بدون"]:
        exists = True

    if exists:
        text = (
            f"🔍 **مراجعة إعداد:** {label}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📝 **البيانات المسجلة حالياً:**\n"
            f"`{current_value}`\n\n"
            f"⚠️ **تنبيه:** هذا القسم مضاف مسبقاً، هل تريد تحديثه؟"
        )
        buttons = [
            [InlineKeyboardButton(f"🔄 تحديث {label}", callback_data=f"exec_upd_{callback_data}")],
            [InlineKeyboardButton("🔙 عودة", callback_data="back_to_config_main")]
        ]
    else:
        text = (
            f"➖ **إعداد جديد:** {label}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🚫 لا توجد بيانات مسجلة حالياً.\n\n"
            f"✨ يمكنك البدء بالإضافة الآن."
        )
        buttons = [
            [InlineKeyboardButton(f"➕ إضافة {label}", callback_data=f"exec_add_{callback_data}")],
            [InlineKeyboardButton("🔙 عودة", callback_data="back_to_config_main")]
        ]

    return text, InlineKeyboardMarkup(buttons)

# ==========================================================================
async def content_management_handler(update, context):
    """
    المعالج المركزي لجميع عمليات إعدادات المحتوى.
    """
    query = update.callback_query
    data = query.data
    target_bot_id = context.user_data.get('target_bot_id')

    if data == "back_to_config_main":
        await query.message.edit_text(
            "⚙️ **لوحة التحكم بإعدادات المحتوى**\n\nإختر القسم الذي تود إدارته من الأزرار أدناه:",
            reply_markup=get_main_config_keyboard(),
            parse_mode="Markdown"
        )
        return

    if data in MAP:
        text, reply_markup = get_setting_interface(target_bot_id, data)
        await query.message.edit_text(text, reply_markup=reply_markup, parse_mode="Markdown")
        return

    if data.startswith("exec_add_") or data.startswith("exec_upd_"):
        setting_key = data.replace("exec_add_", "").replace("exec_upd_", "")
        setting_info = MAP.get(setting_key)
        
        if setting_info:
            label = setting_info['label']
            col_name = setting_info['col']
            
            context.user_data['waiting_for_config'] = col_name
            context.user_data['config_label'] = label
            
            await query.message.edit_text(
                f"📥 **طلب إدخال بيانات:** {label}\n\n"
                f"الرجاء إرسال النص الجديد الآن (نص، رابط، أو تعليمات).\n"
                f"سيتم حفظ البيانات مباشرة في العمود: `{col_name}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء", callback_data=setting_key)]]),
                parse_mode="Markdown"
            )

# ==========================================================================
async def config_input_receiver(update, context):
    """
    استلام النص من المستخدم وحفظه في الـ 39 عموداً.
    """
    if 'waiting_for_config' in context.user_data:
        col_name = context.user_data['waiting_for_config']
        label = context.user_data['config_label']
        new_value = update.message.text
        target_bot_id = context.user_data.get('target_bot_id')

        # استدعاء دالة التحديث المعتمدة
        success = update_content_setting(target_bot_id, col_name, new_value)

        if success:
            await update.message.reply_text(
                f"✅ تم حفظ وتحديث **{label}** بنجاح!\n"
                f"تمت المزامنة مع قاعدة البيانات السحابية والمحلية."
            )
        else:
            await update.message.reply_text("❌ حدث خطأ أثناء محاولة الحفظ، يرجى المحاولة لاحقاً.")
        
        context.user_data.pop('waiting_for_config', None)
        context.user_data.pop('config_label', None)

# ==========================================================================
async def auto_reply_engine(update, context):
    """
    محرك فحص الرسائل والرد التلقائي المركزي.
    يقرأ من عمود 'auto_reply' بصيغة JSON.
    """
    try:
        if not update.message or not update.message.text:
            return False

        user_text = update.message.text
        target_bot_id = context.bot_data.get('bot_id') or context.user_data.get('target_bot_id')
        
        if not target_bot_id:
            return False

        config = get_bot_config(target_bot_id)
        auto_reply_data = config.get('auto_reply')
        
        if auto_reply_data and str(auto_reply_data).strip() not in ["", "-", "None", "{}"]:
            try:
                replies = json.loads(auto_reply_data)
                for keyword, response in replies.items():
                    if str(keyword).lower() in user_text.lower():
                        await update.message.reply_text(response)
                        return True 
            except Exception as json_e:
                print(f"⚠️ خطأ في تحليل JSON الرد التلقائي: {json_e}")
        return False
    except Exception as e:
        print(f"❌ خطأ في محرك الرد التلقائي: {e}")
        return False

# ==========================================================================
def get_auto_reply_manager_keyboard(bot_id):
    """
    توليد لوحة إدارة الردود التلقائية ديناميكياً (1 أو 2 أو 3 في الصف).
    """
    config = get_bot_config(bot_id)
    auto_reply_data = config.get('auto_reply', "{}")
    
    try:
        replies = json.loads(auto_reply_data) if auto_reply_data not in ["", "-", "None"] else {}
    except:
        replies = {}

    keyboard = []
    keyboard.append([InlineKeyboardButton("➕ إضافة رد تلقائي جديد", callback_data="add_new_auto_reply")])

    current_row = []
    for keyword in replies.keys():
        button = InlineKeyboardButton(f"📝 {keyword}", callback_data=f"view_reply_{keyword}")
        
        if len(keyword) > 12:
            if current_row: keyboard.append(current_row)
            keyboard.append([button])
            current_row = []
        else:
            current_row.append(button)
            if len(current_row) == 3:
                keyboard.append(current_row)
                current_row = []

    if current_row: keyboard.append(current_row)
    keyboard.append([InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="back_to_config_main")])
    
    return InlineKeyboardMarkup(keyboard)
