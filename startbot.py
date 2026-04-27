import os
import re
import asyncio
import logging
import aiohttp
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import (
    Update, 
    ReplyKeyboardMarkup, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    ReplyKeyboardRemove,
    Bot
)

# استيراد أدوات المعالجة والتشغيل من مكتبة telegram.ext
from telegram.ext import (
    Application,
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes, 
    CallbackQueryHandler, 
    ConversationHandler,
    ChatMemberHandler
)


# --- [ إعدادات الهوية والصلاحيات ] ---
TOKEN = os.getenv("BOT_TOKEN")
DEVELOPER_ID = 7607952642 
raw_admins = os.getenv("ADMIN_IDS", "")
BACKUP_CHANNEL_ID = -1003910834893  # المعرف الخاص بالقناة
# تنظيف وقراءة قائمة الإداريين
ADMIN_IDS = [int(i.strip()) for i in raw_admins.replace('[','').replace(']','').split(",") if i.strip().isdigit()]
ALL_ADMINS = set([DEVELOPER_ID] + ADMIN_IDS)

ADMIN_ID = DEVELOPER_ID 
# تعريف مراحل محادثة إنشاء البوت
CHOOSING_TYPE, GETTING_TOKEN, GETTING_NAME = range(3)
WAITING_FOR_MODULE_NAME = 4
# تعريف حالة المحادثة للإذاعة 
WAITING_BROADCAST_CONTENT = 101

RUNNING_BOTS = set()
_running_bot_tokens = set()
RUNNING_LOCK = asyncio.Lock()
ACTIVE_RUNTIME_BOTS = {}
BOT_PROCESS_LOCK_FILE = "/app/cache_data/bot_factory.lock"




def acquire_process_lock():
    """يمنع التشغيل المزدوج مع تنظيف القفل القديم"""
    if os.path.exists(BOT_PROCESS_LOCK_FILE):
        try:
            # قراءة PID العملية القديمة
            with open(BOT_PROCESS_LOCK_FILE, "r") as f:
                old_pid = int(f.read())
            # التأكد هل العملية لا تزال حية فعلاً؟
            os.kill(old_pid, 0) 
            return False # العملية حية، لا تشغل نسخة ثانية
        except (OSError, ValueError):
            # العملية ميتة أو الملف تالف، احذفه
            os.remove(BOT_PROCESS_LOCK_FILE)
            
    with open(BOT_PROCESS_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True



def release_process_lock():
    if os.path.exists(BOT_PROCESS_LOCK_FILE):
        os.remove(BOT_PROCESS_LOCK_FILE)



def is_bot_running(token: str) -> bool:
    return token in ACTIVE_RUNTIME_BOTS


def mark_bot_running(token: str, app):
    ACTIVE_RUNTIME_BOTS[token] = app


def mark_bot_stopped(token: str):
    if token in ACTIVE_RUNTIME_BOTS:
        del ACTIVE_RUNTIME_BOTS[token]

# دالة الترحيب بمستخدم جديد
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """دالة الانطلاق، تسجيل المستخدم، وإشعار المطور بالعضو الجديد"""
    user = update.effective_user
    
    # استيراد الدالة المطلوبة من sheets
    from sheets import save_user, get_total_factory_users


   # تمرير كافة البيانات المطلوبة: ID، Username، الاسم الكامل، وتوكن البوت
    is_new = save_user(user.id, user.username, user.full_name, context.bot.token)
 


    # إذا كان المستخدم جديداً، أرسل إشعاراً للمطور (أنت)
    if is_new:
        total_factory_users = get_total_factory_users()
        factory_notif = (
            f"<b>تم دخول شخص جديد إلى المصنع الخاص بك</b> 👾\n"
            f"            -----------------------\n"
            f"• معلومات العضو الجديد .\n\n"
            f"• الاسم : {user.full_name}\n"
            f"• معرف : @{user.username if user.username else 'لا يوجد'}\n"
            f"• الايدي : <code>{user.id}</code>\n"
            f"            -----------------------\n"
            f"• عدد الأعضاء الكلي للمصنع : {total_factory_users}"
        )
        try:
            # إرسال الإشعار لك عبر بوت المصنع
            await context.bot.send_message(chat_id=DEVELOPER_ID, text=factory_notif, parse_mode="HTML")
        except Exception as e:
            print(f"⚠️ فشل إرسال إشعار العضو الجديد للمطور: {e}")

    # إظهار القائمة الرئيسية للمستخدم
    await update.message.reply_text(
        "✨ أهلاً بك في مصنع البوتات المتطور 🤖\n\n"
        "أنا بوت المصنع، يمكنني مساعدتك في إنشاء وإدارة بوتاتك الخاصة بسهولة وربطها بقاعدة قاعدة البيانات.",parse_mode="HTML", 
        reply_markup=get_main_menu_inline(user.id)
    )
    
    # --- نظام إنشاء البوت (Conversation Flow) ---
async def start_create_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """بدء عملية إنشاء بوت جديد وطلب اختيار النوع (عن طريق الأزرار الشفافة)"""
    query = update.callback_query
    if query:
        await query.answer()
        await query.edit_message_text(
            "🛠 **مرحباً بك في قسم التصنيع**\n\nاختر نوع البوت الذي تريد إنشاءه:",parse_mode="HTML", 
            reply_markup=get_types_menu_inline(query.from_user.id)
        )
    else:
        await update.message.reply_text(
            "🛠 **مرحباً بك في قسم التصنيع**\n\nاختر نوع البوت الذي تريد إنشاءه:",parse_mode="HTML", 
            reply_markup=get_types_menu_inline(update.effective_user.id)
        )
    return CHOOSING_TYPE

async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تخزين النوع المختار وطلب التوكن"""
    query = update.callback_query
    await query.answer()
    
    # استخراج النوع من callback_data
    bot_type = query.data.replace("set_type_", "")
    context.user_data["type"] = bot_type
    
    # استخراج الاسم العربي من الأزرار ديناميكياً لضمان الدقة
    friendly_name = "غير معروف"
    for row in query.message.reply_markup.inline_keyboard:
        for button in row:
            if button.callback_data == query.data:
                friendly_name = button.text
                break
    
    # تخزين الاسم العربي في الذاكرة المؤقتة لاستخدامه في finalize_bot
    context.user_data["bot_friendly_name"] = friendly_name
    
    # إرسال الرسالة المنسقة للمستخدم بالاسم العربي
    await query.edit_message_text(
        text=f"✅ تم اختيار نوع: <b>{friendly_name}</b>\n\n"
             "الآن، من فضلك أرسل <b>API Token</b> الخاص بالبوت.\n"
             "يمكنك الحصول عليه من @BotFather",
        parse_mode="HTML"
    )

    return GETTING_TOKEN

async def receive_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    if not re.match(r'^\d+:[A-Za-z0-9_-]{35,}$', token):
        await update.message.reply_text("❌ التوكن غير صحيح!")
        return GETTING_TOKEN
    
    context.user_data["bot_token"] = token
    bot_type = context.user_data.get("type", "")

    # 1. محاولة جلب الاسم العربي المخزن سابقاً من الذاكرة (الأولوية القصوى)
    friendly_name = context.user_data.get("bot_friendly_name")

    # 2. إذا لم يوجد الاسم في الذاكرة (فقط وحصراً)، نبحث عنه في الميتا
    if not friendly_name:
        try:
            from sheets import meta_sheet
            records = meta_sheet.get_all_records()
            for r in records:
                if str(r.get('key')) == f"desc_{bot_type.strip()}.py":
                    friendly_name = r.get('value')
                    break
        except: 
            pass

    # 3. إذا لم يوجد في الذاكرة ولا في الميتا، نستخدم النوع التقني كخيار أخير
    if not friendly_name:
        friendly_name = bot_type

    # تخزين الاسم النهائي في الذاكرة لضمان وصوله إلى finalize_bot بشكل صحيح
    context.user_data["bot_friendly_name"] = friendly_name
    
    await finalize_bot(update, context)
    return ConversationHandler.END

# --------------------------------------------------------------------------
# --- المحرك الديناميكي الأوتوماتيكي المطور ---
# --------------------------------------------------------------------------
async def run_dynamic_bot(bot_token, bot_type, user_id):
    """الحل الجذري: قتل الجلسات القديمة، ربط الملف البرمجي، وتشغيل المحرك الجديد"""
    try:
        from sheets import meta_sheet
        import importlib
        import importlib.util
        import os
        import sys
        import asyncio
        from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ChatMemberHandler, filters

        # --- [ الخطوة 0: بروتوكول القتل الإجباري للنسخة القديمة ] ---
        # لضمان عدم حدوث Conflict 409 عند إعادة التشغيل اليدوي
        if 'ACTIVE_RUNTIME_BOTS' in globals():
            if bot_token in ACTIVE_RUNTIME_BOTS:
                print(f"⚔️ [SLAUGHTER]: تم رصد جلسة نشطة للتوكن {bot_token[:10]}... جاري التصفية.")
                try:
                    old_app = ACTIVE_RUNTIME_BOTS[bot_token]
                    # إيقاف المحدث والمحرك تماماً وتحرير التوكن من سيرفرات تليجرام
                    if old_app.updater and old_app.updater.running:
                        await old_app.updater.stop()
                    await old_app.stop()
                    await old_app.shutdown()
                    # تنظيف الذاكرة
                    del ACTIVE_RUNTIME_BOTS[bot_token]
                    print(f"✅ [SLAUGHTER]: تم إنهاء الجلسة السابقة بنجاح.")
                    await asyncio.sleep(1.2) # وقت مستقطع لضمان تحرير المنفذ
                except Exception as kill_err:
                    print(f"⚠️ [WARNING]: تنبيه أثناء محاولة التصفية: {kill_err}")

        # 1. تحديد اسم الملف البرمجي الحقيقي (Mapping)
        module_file_name = None
        
        # البحث في قاعدة البيانات (الميتا) عن اسم الملف المرتبط بهذا النوع
        try:
            if meta_sheet:
                records = meta_sheet.get_all_records()
                for r in records:
                    key_val = str(r.get('key', '')).strip()
                    target_key = f"desc_{str(bot_type).strip()}.py"
                    
                    if key_val == target_key:
                        module_file_name = key_val.replace('desc_', '').replace('.py', '')
                        break
        except Exception as e:
            print(f"⚠️ خطأ أثناء فحص الميتا: {e}")

        # الخطة البديلة للمسميات (بدون تغيير مفاتيحك)
        if not module_file_name:
            bot_type_str = str(bot_type)
            if "تواصل" in bot_type_str: module_file_name = "contact_bot"
            elif "حماية" in bot_type_str: module_file_name = "protection_bot"
            elif "تعليمية" in bot_type_str or "education" in bot_type_str: module_file_name = "education_bot"
            elif "متجر" in bot_type_str: module_file_name = "store_bot"
            else: 
                module_file_name = bot_type_str.replace('.py', '').strip()

        # التحقق الفيزيائي من المسار
        file_path = os.path.join(os.getcwd(), f"{module_file_name}.py")
        
        if not os.path.exists(file_path):
            print(f"❌ [خطأ فيزيائي]: الملف {module_file_name}.py غير موجود في المسار: {file_path}")
            possible_files = [f for f in os.listdir('.') if f.endswith('.py')]
            print(f"📂 الملفات المتاحة حالياً في السيرفر: {possible_files}")
            return

        # 2. استيراد الموديول برمجياً وتسجيله في النظام
        print(f"📦 محاولة تحميل الملف: {module_file_name}.py للنوع: {bot_type}")
        
        spec = importlib.util.spec_from_file_location(module_file_name, file_path)
        module = importlib.util.module_from_spec(spec)
        
        # تسجيل الموديول لمنع خطأ module not in sys.modules
        sys.modules[module_file_name] = module 
        
        spec.loader.exec_module(module)
        importlib.reload(module) 

        # 3. بناء تطبيق البوت وتجهيزه
        new_app = ApplicationBuilder().token(bot_token).build()
        new_app.bot_data["owner_id"] = int(user_id)

        # 4. ربط المعالجات (Handlers)
        if hasattr(module, 'start_handler'):
            new_app.add_handler(CommandHandler("start", module.start_handler))
        
        if hasattr(module, 'callback_handler'):
            new_app.add_handler(CallbackQueryHandler(module.callback_handler))
        elif hasattr(module, 'contact_callback_handler'):
            new_app.add_handler(CallbackQueryHandler(module.contact_callback_handler))

        main_filter = filters.ALL & (~filters.COMMAND)
        
        if hasattr(module, 'handle_message'):
            new_app.add_handler(MessageHandler(main_filter, module.handle_message))
        elif hasattr(module, 'handle_contact_message'):
            new_app.add_handler(MessageHandler(main_filter, module.handle_contact_message))

        if hasattr(module, 'track_chats'):
            new_app.add_handler(ChatMemberHandler(module.track_chats, ChatMemberHandler.MY_CHAT_MEMBER))

        # 5. تشغيل البوت وتسجيله
        await new_app.initialize()
        await new_app.start()
        
        if 'mark_bot_running' in globals():
            mark_bot_running(bot_token, new_app)
            
        await new_app.updater.start_polling(drop_pending_updates=True)
        print(f"🚀 [نجاح]: البوت بنوع [{bot_type}] يعمل الآن بنسخة نظيفة عبر ملف [{module_file_name}.py]")

    except Exception as e:
        print(f"⚠️ [خطأ حرج]: في محرك التشغيل للنوع {bot_type}: {e}")

# --------------------------------------------------------------------------
#    """حفظ البيانات، تشغيل المحرك، وإرسال إشعارات النجاح بكافة اللغات والمسميات"""
async def finalize_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """حفظ البيانات، تشغيل المحرك، وإرسال إشعارات النجاح بكافة اللغات والمسميات"""
    
    # 1. جلب البيانات من الذاكرة المؤقتة
    friendly_name = context.user_data.get("bot_friendly_name", "بوت مخصص")

    user = update.effective_user
    user_id = user.id
    bot_type = context.user_data.get("type") 
    bot_token = context.user_data.get("bot_token")

    # تصحيح: التحقق من وجود التوكن قبل البدء لمنع انهيار الدالة
    if not bot_token:
        await update.message.reply_text("❌ حدث خطأ في البيانات، يرجى البدء من جديد.")
        return ConversationHandler.END

    msg = await update.message.reply_text("⏳ جاري تهيئة المحرك ...")

    try:
        from telegram import Bot
        # --- [تصحيح حرج: إدارة جلسة البوت بشكل سليم لمنع أخطاء Sessions في Railway] ---
        async with Bot(bot_token) as temp_bot:
            
            await temp_bot.delete_webhook(drop_pending_updates=True)
            
            bot_info = await temp_bot.get_me()
            bot_username = f"@{bot_info.username}"

            # تصحيح: تفعيل الاستيراد لضمان وصول الدالة للبيانات
            from sheets import save_bot, get_total_bots_count
            success = save_bot(user_id, bot_type, friendly_name, bot_token)

            if success: 
                # تشغيل المحرك الديناميكي الذي أصلحناه سابقاً
                asyncio.create_task(run_dynamic_bot(bot_token, bot_type, user_id))

                # --- [ الرسالة الأولى: في بوت المصنع ] ---
                user_success_text = (
                    f"<b>🎊 تمت العملية بنجاح!</b>\n\n"
                    f"لقد انتهينا من برمجة بوتك الجديد وإطلاقه.\n\n"
                    f"📛 <b>الاسم المخصص:</b> {friendly_name}\n"
                    f"🤖 <b>يوزر البوت:</b> {bot_username}\n\n"
                    f"🚀 البوت الآن جاهز للعمل!"
                )
                
                # تصحيح: التأكد من وجود دالة الكيبورد في النطاق الحالي
                from main import get_main_menu_inline
                await msg.edit_text(text=user_success_text, reply_markup=get_main_menu_inline(user_id), parse_mode="HTML")

                # --- [ الرسالة الثانية: داخل البوت الجديد ] ---
                factory_info = await context.bot.get_me()
                congrats_text = (
                    f"<b>🎈 أهلاً بك في عالمك الخاص!</b>\n\n"
                    f"لقد تم ربط هذا البوت بنجاح بمصنع البوتات وقاعدة البيانات.\n\n"
                    f"📛 <b>الاسم:</b> {friendly_name}\n"
                    f"⚙️ <b>الحالة:</b> مرتبط وجاهز للعمل\n"
                    f"-----------------------\n"
                    f"تم الإنشاء بواسطة: @{factory_info.username}"
                )
                
                try:
                    await temp_bot.send_message(chat_id=user_id, text=congrats_text, parse_mode="HTML")
                    
                    # إرسال الدليل فقط إذا كان النوع منصة تعليمية
                    if bot_type == "education_bot":
                        setup_guide_text = (
                            "🚀 <b>الدليل الشامل لتهيئة منصتك التعليمية</b>\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "مرحباً بك يا دكتور! لضمان عمل المنصة بكفاءة واستقرار، يرجى اتباع الخطوات التالية بالترتيب الموصى به:\n\n"
                            "1️⃣ <b>تنشيط نبض النظام (المزامنة):</b>\n"
                            "بدايةً، قم بالضغط على زر <b>(🛠 الإعدادات العامة وتجهيز النظام)</b>، ثم <b>(🔄 المزامنة)</b>.\n\n"
                            "2️⃣ <b>ضبط الهوية الذكية (AI):</b>\n"
                            "انتقل إلى <b>(🤖 ضبط الـ AI)</b> لتعريف اسم منشأتك ووضع التعليمات.\n\n"
                            "3️⃣ <b>تأسيس الفروع الإدارية:</b>\n"
                            "توجه إلى <b>(إدارة الفروع)</b> وأنشئ فرعك الأول.\n\n"
                            "4️⃣ <b>بناء الكادر التعليمي والإداري:</b>\n"
                            "من قسم <b>(تكويد الكادر)</b>، قم بتوليد روابط انضمام.\n\n"
                            "5️⃣ <b>هيكلة المحتوى التعليمي:</b>\n"
                            "• أضف <b>(📁 الأقسام)</b> أولاً ثم <b>(📚 الدورات)</b>.\n\n"
                            "6️⃣ <b>تفعيل القنوات الرسمية:</b>\n"
                            "من <b>(تجهيز قاعدة البيانات)</b>، قم بربط آيدي القنوات.\n\n"
                            "7️⃣ <b>الضبط المالي ونقاط الإحالة:</b>\n"
                            "قم بضبط <b>(معلومات الدفع)</b>.\n\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "💡 <i>استخدم لوحة التحكم للبدء في التهيئة الآن.</i>"
                        )
                        await temp_bot.send_message(chat_id=user_id, text=setup_guide_text, parse_mode="HTML")
                except Exception as e:
                    print(f"⚠️ فشل إرسال رسائل الترحيب: {e}")

                # --- [ الرسالة الثالثة: إشعار المطور ] ---
                total_bots = get_total_bots_count()
                # تصحيح: استيراد DEVELOPER_ID في حال عدم وجوده عالمياً
                from cache_manager import DEVELOPER_ID
                admin_notification = (
                    f"<b>🔔 إشعار تصنيع جديد</b>\n"
                    f"-----------------------\n"
                    f"👤 <b>المنشئ:</b> {user.full_name}\n"
                    f"🔗 <b>يوزر المالك:</b> @{user.username if user.username else 'لا يوجد'}\n"
                    f"🆔 <b>آيدي المالك:</b> <code>{user_id}</code>\n"
                    f"-----------------------\n"
                    f"🤖 <b>نوع البوت:</b> {friendly_name}\n"
                    f"📛 <b>الاسم:</b> {friendly_name}\n"
                    f"🎈 <b>يوزر البوت:</b> {bot_username}\n"
                    f"-----------------------\n\n"
                    f"📈 <b>إجمالي إنتاج المصنع:</b> {total_bots} بوت"
                )

                await context.bot.send_message(chat_id=DEVELOPER_ID, text=admin_notification, parse_mode="HTML")

            else:
                await msg.edit_text("❌ حدث خطأ أثناء الحفظ.")

    except Exception as e:
        print(f"❌ Error in finalize: {e}")
        # تصحيح: التعامل مع فشل التعديل في حال مسح الرسالة
        try:
            await msg.edit_text(f"⚠️ حدث تداخل بسيط أثناء التهيئة: {e}")
        except:
            pass

    context.user_data.clear()
    return ConversationHandler.END

    
    
    # --- دالة تشغيل البوتات المصنوعة تلقائياً ---
async def start_all_sub_bots():
    import asyncio
    import aiohttp
    import logging
    from cache_manager import FACTORY_GLOBAL_CACHE, db_manager
    from sheets import get_all_active_bots

    logger = logging.getLogger("SUB_BOTS_MANAGER")
    loader_logger = logging.getLogger("SUB_BOTS_LOADER")

    MESSAGE = "🔄 تم تحديث نظام المصنع بنجاح.\nجاري تشغيل بوتك الآن...\n/start"

    if not acquire_process_lock():
        print("⚠️ [SYSTEM]: نسخة أخرى من المصنع تعمل بالفعل. تم إلغاء الإقلاع المزدوج.")
        return

    print("🔄 [SYSTEM]: جاري استهلال محرك التشغيل والإشعارات الشامل...")
    loader_logger.info("Unified start_all_sub_bots process started.")

    try:
        # =========================
        # 🔥 دمج نظام جلب البيانات الكامل
        # =========================
        try:
            print("🔍 [LOG]: محاولة السحب من الكاش...")
            active_bots = FACTORY_GLOBAL_CACHE.get("data", {}).get("البوتات_المصنوعة", [])

            if not active_bots:
                print("⚠️ [LOG]: الكاش فارغ → fallback إلى get_all_active_bots")
                active_bots = get_all_active_bots()

            if not active_bots:
                print("⚠️ [LOG]: fallback إضافي → SQLite مباشر")
                try:
                    db_manager.cursor.execute("SELECT * FROM البوتات_المصنوعة")
                    rows = db_manager.cursor.fetchall()
                    active_bots = [dict(row) for row in rows]
                    print(f"📊 [SUCCESS]: تم جلب {len(active_bots)} من SQLite")
                    loader_logger.info(f"Fetched {len(active_bots)} bots from SQLite.")
                except Exception as sql_e:
                    print(f"❌ [ERROR]: فشل SQLite: {sql_e}")
                    loader_logger.error(f"SQL error: {sql_e}")
                    active_bots = []
            else:
                print(f"✅ [SUCCESS]: تم تحميل {len(active_bots)} من الكاش")
                loader_logger.info(f"Loaded {len(active_bots)} bots from cache")

        except Exception as e:
            print(f"⚠️ [CRITICAL]: فشل جلب البيانات: {e}")
            logger.error(f"Data fetch failure: {e}")
            active_bots = []

        # =========================
        # النتائج (دمج النظامين)
        # =========================
        results = {
            "started": 0,
            "notified": 0,
            "notified_success": 0,
            "notified_failed": 0
        }

        queue = asyncio.Queue()

        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(limit=50)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

            # =========================
            # Retry Engine (مدمج)
            # =========================
            async def send_with_retry(token, owner_id, retries=3):
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                payload = {"chat_id": owner_id, "text": MESSAGE, "parse_mode": "HTML"}

                print(f"📩 [RETRY]: إرسال إلى {owner_id}...")

                for attempt in range(1, retries + 1):
                    try:
                        async with session.post(url, json=payload) as resp:
                            if resp.status == 200:
                                print(f"✅ [SUCCESS]: إشعار {owner_id}")
                                return True

                            if resp.status == 429:
                                retry_after = int(resp.headers.get("Retry-After", 5))
                                print(f"⏳ [RATE LIMIT]: {retry_after}s")
                                await asyncio.sleep(retry_after)
                            else:
                                logger.warning(f"فشل {resp.status} → {owner_id}")

                    except Exception as e:
                        logger.error(f"Retry error {attempt}: {e}")

                    await asyncio.sleep(1)

                return False

            # =========================
            # Worker (مدمج ومحسن)
            # =========================
            async def notification_worker():
                while True:
                    try:
                        token, owner_id = await queue.get()
                    except asyncio.CancelledError:
                        break

                    try:
                        ok = await send_with_retry(token, owner_id)

                        if ok:
                            results["notified"] += 1
                            results["notified_success"] += 1
                        else:
                            results["notified_failed"] += 1

                    finally:
                        queue.task_done()

            # =========================
            # التشغيل الرئيسي
            # =========================
            print("🚀 [LAUNCH]: بدء تشغيل البوتات...")

            for bot_data in active_bots:
                try:
                    token = bot_data.get("التوكن")
                    owner_id = bot_data.get("ID المالك") or bot_data.get("bot_id")
                    bot_type = bot_data.get("نوع البوت")
                    status = bot_data.get("الحالة", "نشط")

                    if not token:
                        print("⏭️ [SKIP]: بدون توكن")
                        continue

                    if status != "نشط":
                        print(f"⏸️ [SKIP]: {token[:10]} غير نشط")
                        continue

                    if is_bot_running(token):
                        print(f"⏭️ [SKIP]: يعمل مسبقاً {token[:10]}")
                        continue

                    target_func = globals().get('run_dynamic_bot')

                    if target_func:
                        async def safe_run():
                            try:
                                await target_func(token, bot_type, owner_id)
                            except Exception as e:
                                logger.error(f"[CRASH] {token[:10]}: {e}")

                        asyncio.create_task(safe_run())

                        results["started"] += 1
                        print(f"✅ [STARTED]: {token[:15]}")

                        if owner_id:
                            await queue.put((token, str(owner_id)))
                            print(f"📥 [QUEUE]: {owner_id}")

                    else:
                        print("❌ run_dynamic_bot غير موجود")

                except Exception as e:
                    print(f"❌ [BOT ERROR]: {e}")
                    logger.error(f"Bot error: {e}")

            # =========================
            # Workers
            # =========================
            workers = []
            if not queue.empty():
                print(f"📢 [NOTIFIER]: إرسال {queue.qsize()} إشعار")
                workers = [asyncio.create_task(notification_worker()) for _ in range(5)]

                await queue.join()

                for w in workers:
                    w.cancel()

                await asyncio.gather(*workers, return_exceptions=True)

        # =========================
        # التقرير النهائي (مدمج)
        # =========================
        print(f"\n{'='*30}")
        print(f"🚀 started: {results['started']}")
        print(f"📩 notified: {results['notified']}")
        print(f"✅ success: {results['notified_success']}")
        print(f"❌ failed: {results['notified_failed']}")
        print(f"{'='*30}")

        logger.info(f"Finished. Started={results['started']} Success={results['notified_success']}")

    finally:
        release_process_lock()

# --------------------------------------------------------------------------
    #~~~~~~~~~~~~~~~~
    
async def boot_all_bots():
    from sheets import get_all_active_bots
    active_bots = get_all_active_bots()
    print(f"🔄 جاري تحضير إقلاع {len(active_bots)} بوت تابعة للمصنع...")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إلغاء عملية إنشاء البوت وتنظيف الذاكرة المؤقتة"""
    query = update.callback_query
    if query:
        await query.answer()
        await query.edit_message_text("❌ تم إلغاء عملية الإنشاء. يمكنك البدء من جديد في أي وقت.")
    else:
        await update.message.reply_text("❌ تم إلغاء العملية والعودة للقائمة الرئيسية.")
    
    context.user_data.clear()
    return ConversationHandler.END

create_bot_conv = ConversationHandler(
    entry_points=[CallbackQueryHandler(start_create_bot, pattern="^start_manufacture$"), MessageHandler(filters.Regex("^➕ إنشاء بوت$"), start_create_bot)],
    states={
        CHOOSING_TYPE: [CallbackQueryHandler(select_type, pattern="^set_type_")],
        GETTING_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_token)],
    },
    fallbacks=[CallbackQueryHandler(cancel, pattern="^cancel_action$"), CommandHandler('cancel', cancel)],
)
