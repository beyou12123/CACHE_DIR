import os
import json
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from datetime import datetime
# استيراد محرك قاعدة البيانات والكاش
from cache_manager import db_manager, FACTORY_GLOBAL_CACHE, save_cache_to_disk

# --- المتغيرات الثابتة ---
# تم الحفاظ على النص بالكامل مع معالجة فنية لسلاسل النصوص الطويلة لضمان عدم حدوث أخطاء برمجية
AI_GUIDE_FULL_TEXT = """
📘 دليل ضبط الذكاء الاصطناعي للمالك:

`أنت "المستشار الذكي" والموظف الرقمي المعتمد والحصري
🛡️ دستور الموظف الرقمي لمؤسسة (كن أنت للتدريب والتأهيل)
أنت "المستشار الذكي" والموظف الرقمي المعتمد والحصري للمؤسسة. تعمل وفق نظام "البيانات المغلقة"، أي أن معلوماتك تنتهي حيث تنتهي قاعدة البيانات المزودة لك.
🛑 القيود الجبرية (ممنوع التجاوز):
 * قاعدة البيانات فقط: يُمنع منعاً باتاً ذكر أي دورة تدريبية، أو سعر، أو قسم، أو عرض غير موجود في المصفوفة النصية (JSON) المرسلة إليك ضمن سياق النظام. إذا سألك العميل عن دورة "X" وهي غير موجودة في القائمة، يجب أن ترد حصراً بـ: "نعتذر منك، هذه الدورة غير متوفرة حالياً في منصتنا" (هذا الرد ضروري لتفعيل إشعار المدير).
 * تحريم التخمين: لا تخمن أسعاراً أبداً. إذا كانت الدورة موجودة بدون سعر، قل: "يرجى التواصل مع الإدارة لتحديد الرسوم الحالية".
 * العملات الصارمة: السعر المرجعي هو الدولار ($). التزم بمعادلات التحويل التالية فقط:
   * (1$ = 530 ريال يمني).
   * (1$ = 3.75 ريال سعودي).
   مثال إلزامي: "السعر 100$، أي 53,000 يمني أو 375 سعودي".

 * بروتوكول النطاق: أي سؤال خارج (التدريب، المؤسسة، التسجيل) رد بـ: "شكرًا على سؤالك 🌸، لكنني موظف تابع لمؤسسة كن أنت، ولا أملك معلومات خارج إطار برامجنا التدريبية."

💰 استراتيجية الإقناع والتحويل (نموذج ACE):

 * الاستقبال (A - Acknowledge): رحب بالعميل باسمه وبلهجة تناسب بلده (يمني، سعودي، سوداني.. إلخ).
 * القيمة (C - Core Value): ركز على أن التدريب أونلاين عبر Zoom، بشهادات معتمدة، ومحاضرات مسجلة للرجوع إليها. 
 * الإغلاق (E - End with Question): قاعدة ذهبية: لا تنهِ أي رسالة بجملة خبرية. يجب دائماً ختم الرد بسؤال تفاعلي (مثال: "هل نعتمد حجز مقعدك الآن؟"، "هل تود الاطلاع على محاور الدبلوم؟"). 

📋 خطوات التسجيل الرسمية:
 * عند رغبة العميل في الاشتراك، قل له: "ممتاز! لإتمام عملية حجز مقعدك رسمياً، يرجى الضغط على زر (✅ تسجيل في دورة) الموجود في القائمة أسفل الشاشة لملء بياناتك في النظام."

🎨 التنسيق البصري:
 * استخدم النقاط (Bullet Points) والجداول لتنظيم المعلومات. 
 * استخدم الرموز التعبيرية (Emojis) بشكل احترافي وجذاب. 
 * كن مختصراً مقنعاً وودوداً وواثقاً جداً في طرحك.
"""

# قائمة الأعمدة لجدول إعدادات_المحتوى لعملية التصفير الآمن
CONTENT_CONFIG_COLS = [
"bot_id","الرسالة الترحيبية","القوانين","رد التوقف","auto_reply","ai_enabled","welcome_enabled","buttons",
"banned_words","admin_ids","language","theme","delay_response","broadcast_enabled","custom_commands", 
"welcome_morning", "welcome_noon", "welcome_evening", "welcome_night", "اسم_المؤسسة", "تعليمات_AI", "ref_points_join", 
"ref_points_purchase", "min_points_redeem", "currency_unit", "homework_grade", "subscription_price", "ai_provider", 
"maintenance_mode", "max_daily_ai_questions", "backup_channel_id", "bot_status_msg", "trial_end_action", "timezone", "ai_memory_limit", 
"إعدادات_الدفع", "إصدار_التحديث", "حالة_المزامنة", "وقت_التعديل"
]

async def org_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    المعالج النصي لإدخالات المالك (الاسم، التعليمات، الدفع) 
    مع دمج منطق تحديث الرام والقرص والفحص الارتدادي.
    """
    
    user_data = context.user_data
    action = user_data.get('action')
    bot_id = context.bot.id
    text_received = update.message.text.strip()

    # تحديد الحمولة بناءً على الإجراء الحالي
    update_payload = {}
    if action == 'waiting_for_org_name':
        update_payload = {"اسم_المؤسسة": text_received}
    elif action == 'waiting_for_ai_prompt':
        update_payload = {"تعليمات_AI": text_received}
    elif action == 'waiting_for_payment_info':
        update_payload = {"إعدادات_الدفع": text_received}

    if update_payload:
        # 1. تحديث الرام مباشرة
        sheet_name = "إعدادات_المحتوى"
        records = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])

        # البحث عن السجل وتحديثه أو إضافته
        found = False
        for record in records:
            if str(record.get("bot_id")) == str(bot_id):
                record.update(update_payload)
                found = True
                break
        if not found:
            records.append(update_payload)

        # 2. مزامنة التغيير مع القرص الفيزيائي
        save_cache_to_disk()
        # --- [ نهاية منطق التحديث ] ---

        # تصفير الأكشن بعد الحفظ
        context.user_data['action'] = None
        
        # [التحقق الارتدادي الختامي باستخدام الدالة التي طلبت دمجها]
        missing_after = await check_required_configs(bot_id)
        
        if missing_after == "ALL_SET":
            await update.message.reply_text("✅ تم حفظ البيانات واكتملت كافة الإعدادات بنجاح.")
        else:
            await update.message.reply_text(f"✅ تم الحفظ. الحالة الحالية: {missing_after}")

async def check_required_configs(bot_id):
    """
    تقوم بفحص النواقص في الإعدادات الأساسية وترجع أول نقص تجده.
    تعتمد على FACTORY_GLOBAL_CACHE لضمان مطابقة بيانات الرام.
    """
    # الوصول لجدول إعدادات_المحتوى من الكاش
    sheet_name = "إعدادات_المحتوى"
    records = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
    
    # البحث عن السجل الخاص بالبوت باستخدام bot_id
    config = next((r for r in records if str(r.get("bot_id")) == str(bot_id)), {})
    
    # 1. الفحص الارتدادي لاسم المؤسسة
    if not config.get("اسم_المؤسسة") or str(config.get("اسم_المؤسسة")).strip() == "0":
        return "MISSING_ORG_NAME"
    
    # 2. الفحص الارتدادي لتعليمات AI
    if not config.get("تعليمات_AI") or str(config.get("تعليمات_AI")).strip() == "0":
        return "MISSING_AI_PROMPT"
    
    # 3. الفحص الارتدادي لإعدادات الدفع
    if not config.get("إعدادات_الدفع") or str(config.get("إعدادات_الدفع")).strip() == "0":
        return "MISSING_PAYMENT"
    
    # في حال اكتملت كافة البيانات الأساسية
    return "ALL_SET"

# --- 2. دالة عرض لوحة اسم المؤسسة ---
async def show_org_name_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    عرض لوحة إعدادات اسم المؤسسة مع جلب البيانات من الكاش المركزي للمصنع.
    """
    
    query = update.callback_query
    bot_id = context.bot.id
    
    # جلب السجل الخاص بالبوت من الكاش المركزي (إعدادات_المحتوى)
    sheet_name = "إعدادات_المحتوى"
    records = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
    config = next((r for r in records if str(r.get("bot_id")) == str(bot_id)), {})
    
    # جلب قيمة اسم المؤسسة من السجل
    org_name = config.get("اسم_المؤسسة")

    # منطق العرض بناءً على حالة الاسم (فارغ، None، أو "0")
    if not org_name or str(org_name).strip() == "0":
        text = "⚠️ **تنبيه:** لم يتم ضبط اسم المؤسسة بعد.\n\nيرجى إضافة اسم المؤسسة التعليمية أولاً ليتمكن البوت من تعريف نفسه للطلاب."
        keyboard = [
            [InlineKeyboardButton("➕ إضافة اسم المؤسسة", callback_data="trigger_add_org")]
        ]
    else:
        # وضع الاسم بين علامتي ` ` لتمكين النسخ باللمس
        text = f"🏢 **اسم المؤسسة الحالي:**\n\n`{org_name}`\n\nيمكنك التعديل أو العودة للوحة الإعدادات."
        keyboard = [
            [InlineKeyboardButton("🔄 تحديث وتعديل الاسم", callback_data="trigger_add_org")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main_config")]
        ]

    # إضافة زر إغلاق دائم لضمان التحكم
    keyboard.append([InlineKeyboardButton("❌ إغلاق", callback_data="close_panel")])

    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# --- 3. معالج بدء عملية الإضافة ---
async def org_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    مستقبل المدخلات النصية: يقوم بتحديث البيانات في الكاش (RAM) 
    ثم استدعاء الحفظ الفيزيائي (Disk) لضمان المطابقة الكاملة.
    """

    
    user_data = context.user_data
    action = user_data.get('action')
    bot_id = context.bot.id
    text_received = update.message.text.strip()

    # قائمة الأعمدة الـ 39 لضمان التصفير الذكي وعدم فقدان أي مفتاح
    

    # تحديد الحقل المطلوب تحديثه بناءً على الحالة
    update_payload = {}
    if action == 'waiting_for_org_name':
        update_payload = {"اسم_المؤسسة": text_received}
    elif action == 'waiting_for_ai_prompt':
        update_payload = {"تعليمات_AI": text_received}
    elif action == 'waiting_for_payment_info':
        update_payload = {"إعدادات_الدفع": text_received}

    if update_payload:
        sheet_name = "إعدادات_المحتوى"
        records = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
        
        # البحث عن السجل وتحديثه (منطق الرام الذي أكدت عليه)
        target_record = next((r for r in records if str(r.get("bot_id")) == str(bot_id)), None)
        
        if target_record:
            target_record.update(update_payload)
            target_record["وقت_التعديل"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            # إذا لم يوجد سجل، ننشئ واحد جديد ونطبق التصفير الذكي لكافة الأعمدة
            target_record = {"bot_id": str(bot_id)}
            target_record.update(update_payload)
            for col in CONTENT_CONFIG_COLS:
                if col not in target_record:
                    target_record[col] = "0"
            records.append(target_record)

        # تحديث الرام النهائية
        FACTORY_GLOBAL_CACHE["data"][sheet_name] = records
        
        # المزامنة مع القرص (Disk) - الاستدعاء الصحيح بدون بارامترات
        save_cache_to_disk()

        # تنظيف الحالة وإرسال رد النجاح للمستخدم
        user_data['action'] = None
        
        # منطق رسائل النجاح (حسب نوع الحالة)
        if "اسم_المؤسسة" in update_payload:
            msg = f"✅ تم حفظ اسم المؤسسة: `{text_received}`\nهل ننتقل لضبط AI؟"
            kb = [[InlineKeyboardButton("🧠 ضبط AI", callback_data="set_ai_prompt"), InlineKeyboardButton("🔙 عودة", callback_data="set_org_name")]]
        elif "تعليمات_AI" in update_payload:
            msg = "✅ تم حفظ دليل AI بنجاح.\nهل ننتقل لضبط بيانات الدفع؟"
            kb = [[InlineKeyboardButton("💳 ضبط الدفع", callback_data="set_payment"), InlineKeyboardButton("🔙 عودة", callback_data="set_org_name")]]
        else:
            msg = "✅ تم حفظ بيانات الدفع بنجاح."
            kb = [[InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_to_main_config")]]

        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

# --- 5. دالة عرض لوحة تعليمات الذكاء الاصطناعي ---
async def show_ai_prompt_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    bot_id = context.bot.id
    
    # 1. جلب البيانات من الكاش العالمي (الرام) مباشرة
    records = FACTORY_GLOBAL_CACHE["data"].get("إعدادات_المحتوى", [])
    config = next((r for r in records if str(r.get("bot_id")) == str(bot_id)), {})
    
    ai_prompt = config.get("تعليمات_AI")

    # التحقق الارتدادي (يعمل الآن من الرام لحظياً)
    if not config.get("اسم_المؤسسة") or str(config.get("اسم_المؤسسة")) == "0":
        await query.answer("⚠️ يجب ضبط اسم المؤسسة أولاً!", show_alert=True)
        # تأكد من استدعاء دالة عرض لوحة الاسم الصحيحة
        return await set_org_name(update, context) 

    # باقي منطق العرض كما كتبته أنت (فهو ممتاز)
    if not ai_prompt or str(ai_prompt) == "0":
        text = (
            "🧠 **إعدادات الذكاء الاصطناعي:**\n\n"
            "⚠️ **تنبيه:** الدليل الحالي فارغ! البوت يحتاج تعليمات ليفهم دوره."
        )
        btn_text = "➕ إضافة الدليل الآن"
    else:
        preview = ai_prompt[:150] + "..." if len(ai_prompt) > 150 else ai_prompt
        text = (
            "🧠 **تعليمات الذكاء الاصطناعي الحالية:**\n\n"
            f"📝 `{preview}`\n\n"
            "هل تود تحديث الدليل؟"
        )
        btn_text = "🔄 تحديث وتعديل المعلومات"

    keyboard = [
        [InlineKeyboardButton(btn_text, callback_data="trigger_edit_ai")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="set_org_name")]
    ]

    await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# --- 6. معالج بدء تحديث AI وإرسال الدليل الإرشادي ---
async def trigger_edit_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # استخدام النص الكامل المخزن في المتغير الثابت في أعلى الملف
    from set_org import AI_GUIDE_FULL_TEXT 
    
    guide_msg = (
        f"{AI_GUIDE_FULL_TEXT}\n\n"
        "🛑 **الآن:** يرجى إرسال تعليمات الضبط الجديدة (أو نسخ الدليل أعلاه وتعديله) لردها للبوت:"
    )
    
    await query.message.reply_text(
        text=guide_msg,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 إلغاء", callback_data="show_ai_panel")]]),
        parse_mode="Markdown"
    )
    
    context.user_data['action'] = 'waiting_for_ai_prompt'
    await query.answer()


# --- 8. دالة عرض لوحة معلومات الدفع ---
async def show_payment_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    bot_id = context.bot.id
    
    # جلب البيانات من الرام مباشرة
    records = FACTORY_GLOBAL_CACHE["data"].get("إعدادات_المحتوى", [])
    config = next((r for r in records if str(r.get("bot_id")) == str(bot_id)), {})
    
    pay_info = config.get("إعدادات_الدفع")

    # [التحقق الارتدادي الذكي]: التحقق من الرام مباشرة
    org_name = config.get("اسم_المؤسسة")
    ai_prompt = config.get("تعليمات_AI")

    if not org_name or str(org_name) == "0":
        await query.answer("⚠️ يجب ضبط اسم المؤسسة أولاً!", show_alert=True)
        return await show_org_name_panel(update, context) # تأكد من اسم الدالة لديك
        
    if not ai_prompt or str(ai_prompt) == "0":
        await query.answer("⚠️ يجب ضبط دليل الذكاء الاصطناعي أولاً!", show_alert=True)
        return await show_ai_prompt_panel(update, context)

    # منطق العرض
    if not pay_info or str(pay_info) == "0":
        text = "💳 **إعدادات الدفع:**\n\n⚠️ لا توجد معلومات دفع مسجلة. يرجى إضافة أرقام الحسابات وطرق التحويل (مثلاً: بنك كذا، رقم حساب كذا)."
        btn_text = "➕ إضافة بيانات الدفع"
    else:
        text = f"💳 **معلومات الدفع الحالية:**\n\n`{pay_info}`\n\nهل تود تعديلها؟"
        btn_text = "🔄 تحديث وتعديل"
    
    keyboard = [
        [InlineKeyboardButton(btn_text, callback_data="trigger_edit_payment")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="set_org_name")]
    ]

    await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# --- 9. معالج بدء تحديث الدفع ---
async def trigger_edit_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    تفعيل حالة انتظار استقبال معلومات الدفع وإرشاد المالك.
    """
    query = update.callback_query
    
    # تفعيل حالة الانتظار
    context.user_data['action'] = 'waiting_for_payment_info'
    
    await query.message.reply_text(
        text="💰 **يرجى إرسال معلومات الدفع الآن:**\n(مثال: رقم حساب الكريمي، عنوان USDT، أو أي تعليمات دفع أخرى)",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 إلغاء", callback_data="set_payment")]
        ]),
        parse_mode="Markdown"
    )
    await query.answer()

