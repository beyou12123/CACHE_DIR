import os
import sys
import re
import asyncio
import time
import importlib # استيراد الموديولات ديناميكياً لتشغيل الملفات المرفوعة
import signal
from datetime import datetime
import json
import SubscriptionManager
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from cache_manager import FACTORY_GLOBAL_CACHE, smart_sync_check, db_manager
from apscheduler.schedulers.background import BackgroundScheduler
from sheets import sync_ad_campaign_results, connect_to_google

# استيراد الأدوات الأساسية من مكتبة تليجرام
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

from startbot import (
    # --- المتغيرات والثوابت والمعرفات ---
    TOKEN,
    DEVELOPER_ID,
    BACKUP_CHANNEL_ID,
    ADMIN_IDS,
    ALL_ADMINS,
    ADMIN_ID,
    CHOOSING_TYPE,
    GETTING_TOKEN,
    GETTING_NAME,
    WAITING_FOR_MODULE_NAME,
    WAITING_BROADCAST_CONTENT,
    RUNNING_BOTS,
    _running_bot_tokens,
    RUNNING_LOCK,
    ACTIVE_RUNTIME_BOTS,
    BOT_PROCESS_LOCK_FILE,
    
    # --- الدوال الأساسية وإدارة النظام ---
    acquire_process_lock,
    release_process_lock,
    is_bot_running,
    mark_bot_running,
    mark_bot_stopped,
    ensure_all_sheets_schema,
    
    # --- دوال التشغيل والمحركات الفرعية ---
    start_all_sub_bots,
    run_dynamic_bot,
    
    # --- معالجات الأوامر والمحادثات (Handlers) ---
    start,
    start_create_bot,
    select_type,
    receive_token,
    cancel
)


# استيراد الدوال من ملف البرمجة الخاص بقاعدة البيانات(sheets.py)
from sheets import (
    save_user, 
    save_bot, 
    update_content_setting, 
    get_bot_config, 
    add_log_entry, 
    get_total_bots_count,
    get_total_factory_users,
    get_all_active_bots,
    setup_bot_factory_database, # أضف هذه أيضاً لأنها المحرك الرئيسي
    ensure_sheet_schema,
    reset_entire_database, 
    ensure_all_sheets_schema
)
try:
    from course_engine import restart_bot_logic
except ImportError:
    restart_bot_logic = None



# --- القوائم الشفافة المحدثة ---
def get_main_menu_inline(user_id):
    u_id = int(user_id)

    keyboard = [
        [InlineKeyboardButton("➕ إنشاء بوت", callback_data="start_manufacture")]
    ]
    
    if u_id in ALL_ADMINS or u_id == DEVELOPER_ID:
        keyboard.append([
            InlineKeyboardButton("🛠 لوحة التحكم (للأدمن)", callback_data="open_admin_dashboard")
        ])
    
    return InlineKeyboardMarkup(keyboard)

# --------------------------------------------------------------------------
def get_types_menu_inline(user_id):
    hidden_dev_files = ['test_lab.py']
 
    keyboard = [
        [InlineKeyboardButton("📩 تواصل", callback_data="set_type_contact_bot"),
         InlineKeyboardButton("🛡 حماية", callback_data="set_type_protection_bot")],
        [InlineKeyboardButton("🎓 منصة تعليمية", callback_data="set_type_education_bot"),
         InlineKeyboardButton("🛒 متجر", callback_data="set_type_store_bot")]
    ]
    
    # استيراد ورقة الميتا لجلب الأوصاف
    from sheets import meta_sheet
    descriptions = {}
    try:
        if meta_sheet:
            records = meta_sheet.get_all_records()
            descriptions = {r['key']: r['value'] for r in records if str(r['key']).startswith('desc_')}
    except: pass

    exclude_files = ['main.py', 'sheets.py','downloader_bot', 'ai_bot', 'transcriber_bot', 'cache_manager.py', 'contact_bot.py', 'education_bot.py', 'protection_bot.py', 'store_bot.py', 'config.py', 'runner.py', 'course_engine.py', 'educational_manager.py', 'ContentManager.py', 'SubscriptionManager.py']
    
    dynamic_buttons = []
    for file in os.listdir('.'):
        if file.endswith('.py') and file not in exclude_files:
            if file in hidden_dev_files and user_id != DEVELOPER_ID:
                continue
        	
            module_name = file[:-3]
            # جلب الاسم الوصفي من الشيت، وإذا لم يوجد نستخدم اسم الملف كبديل
            display_name = descriptions.get(f"desc_{file}", module_name)
            dynamic_buttons.append(InlineKeyboardButton(f"🤖 {display_name}", callback_data=f"set_type_{module_name}"))
    
    for i in range(0, len(dynamic_buttons), 2):
        keyboard.append(dynamic_buttons[i:i + 2])
    
    keyboard.append([InlineKeyboardButton("🔙 إلغاء", callback_data="cancel_action")])
    return InlineKeyboardMarkup(keyboard)




# --------------------------------------------------------------------------

# القوائم القديمة (للحفاظ على التوافق مع الوظائف التي قد تطلبها)
main_menu = [["➕ إنشاء بوت"], ["🛠 لوحة التحكم (للأدمن)"]]
admin_options = [["📝 تعديل النصوص", "⚙️ إعدادات الموديولات"], ["🔙 العودة للقائمة الرئيسية"]]
types_menu = [["📩 تواصل"], ["🛡 حماية"], ["🎓 منصة تعليمية"], ["🛒 متجر"]]

# --------------------------------------------------------------------------

# ==========================================
# 🛡️ نظام طلبات الإدارة والترقية (إضافات فقط)
# ==========================================

# 1. الدالة المساعدة لجلب إحصائيات الإدارة الشاملة
def get_factory_admin_stats():
    try:
        from sheets import (
            get_total_factory_users, 
            get_total_bots_count, 
            ADMIN_IDS
        )
        # ملاحظة: يمكنك إضافة دوال الحظر لاحقاً إذا كانت متوفرة في sheets.py
        stats = {
            "total_users": get_total_factory_users(),
            "total_bots": get_total_bots_count(),
            "admins_count": len(set(ADMIN_IDS + [DEVELOPER_ID])), # +1 للمطور الأساسي
            "banned_count": 0, # قيمة افتراضية حتى ربطها بدالة الحظر
            "blocked_bot": 0   # قيمة افتراضية
        }
        return stats
    except:
        return {"total_users": "N/A", "total_bots": "N/A", "admins_count": "N/A", "banned_count": "N/A", "blocked_bot": "N/A"}

# 2. معالج طلب الانضمام التلقائي (الكلمة المفتاحية)
async def process_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    # منع الطلب إذا كان المستخدم أدمن بالفعل
    if user_id in set(ALL_ADMINS):
        await update.message.reply_text(f"✅ يا {user.first_name}، أنت بالفعل ضمن فريق إدارة المصنع!")
        return

    stats = get_factory_admin_stats()
    
    # رسالة الإشعار للمطور
    admin_notif_text = (
        f"📶 <b>مستخدم جديد يريد الانضمام لإدارة المصنع</b>\n\n"
        f"👤 <b>معلومات العضو:</b>\n"
        f"• الاسم: {user.full_name}\n"
        f"• المعرّف: @{user.username if user.username else 'لا يوجد'}\n"
        f"• الآيدي: <code>{user_id}</code>\n\n"
        f"📊 <b>إحصائيات المصنع اللحظية:</b>\n"
        f"• إجمالي مستخدمي المصنع: {stats['total_users']}\n"
        f"• إجمالي المحظورين: {stats['banned_count']}\n"
        f"• إجمالي الحاظرين للمصنع: {stats['blocked_bot']}\n"
        f"• إجمالي الأدمنية: {stats['admins_count']}\n\n"
        f"<b>هل تريد ترقية المستخدم إلى أدمن؟</b>"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("✅ قبول الترقية", callback_data=f"promote_user_{user_id}"),
            InlineKeyboardButton("❌ رفض الطلب", callback_data=f"reject_user_{user_id}")
        ]
    ]
    
    # إرسال الطلب للمطور
    await context.bot.send_message(
        chat_id=DEVELOPER_ID, 
        text=admin_notif_text, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode="HTML"
    )
    
    await update.message.reply_text("📨 تم إرسال طلبك إلى مالك المصنع، سيتم إشعارك عند المراجعة.")

async def handle_admin_promotion_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    
    if query.from_user.id != DEVELOPER_ID: return  # للمطور فقط

    if data.startswith("promote_user_"):
        target_id = int(data.replace("promote_user_", ""))
        # منطق الإضافة (يمكنك هنا تحديث ملف .env أو قاعدة البيانات)
        ALL_ADMINS.add(target_id)
        await query.message.edit_text(f"✅ تم قبول الترقية للآيدي: <code>{target_id}</code>", parse_mode="HTML")
        try:
            await context.bot.send_message(chat_id=target_id, text="🎊 <b>مبروك!</b> تم قبول انضمامك لفريق إدارة المصنع بنجاح.", parse_mode="HTML")
        except:
            pass

    elif data.startswith("reject_user_"):
        target_id = int(data.replace("reject_user_", ""))
        await query.message.edit_text(f"❌ تم رفض طلب العضو: <code>{target_id}</code>", parse_mode="HTML")
        try:
            await context.bot.send_message(chat_id=target_id, text="⚠️ نعتذر منك، تم رفض طلب انضمامك للإدارة حالياً.")
        except:
            pass

    elif data == "manual_add_admin":
        await query.message.reply_text("📝 من فضلك أرسل آيدي (ID) المستخدم المراد ترقيته مباشرة:")
        context.user_data["admin_action"] = "manual_promote"


# --------------------------------------------------------------------------

async def handle_manual_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("admin_action") == "manual_promote":
        target_id = int(update.message.text)
        ALL_ADMINS.add(target_id)
        context.user_data.pop("admin_action")
        await update.message.reply_text("✅ تم إضافة الأدمن بنجاح")

# يمكنك كتابة أي دوال جديدة هنا (مثل دوال الإحصائيات المتقدمة أو أنظمة الدفع)
# --------------------------------------------------------------------------
# --- [ القسم 1: الدوال التشغيلية (يجب أن تظل في الأعلى) ] ---
def start_scheduler():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from cache_manager import db_manager, smart_sync_check
    from sheets import sync_ad_campaign_results

    # تعريف المجدل مع ضبط التوقيت المحلي لليمن/الرياض
    scheduler = AsyncIOScheduler(timezone="Asia/Riyadh")

    # 1. المزامنة الصامتة (Pull): تحديث الكاش المحلي من جوجل كل 15 دقيقة  
    # تم تعديل الاستدعاء ليمر عبر محرك البحث عن التوكنات النشطة
    scheduler.add_job(  
        lambda: [smart_sync_check(token) for token in get_all_active_tokens()],   
        'interval',   
        minutes=15,  
        id='pull_sync'  
    )  

    # 2. مزامنة النتائج: تحديث إحصائيات الحملات الإعلانية كل ساعة  
    scheduler.add_job(  
        sync_ad_campaign_results,   
        'interval',   
        hours=1,  
        id='ads_sync'  
    )  

    # 3. الرفع الشامل (Push): رفع كل العمليات المعلقة (Pending) لجوجل شيت فجراً  
    # تصحيح: الاستدعاء يجب أن يتم عبر db_manager.push_to_google_sheets
    # وتأكدنا أن الدالة في الكلاس لا تطلب spreadsheet كوسيط إجباري أو يتم تمريره من الإعدادات
    scheduler.add_job(  
        db_manager.push_to_google_sheets,   
        'cron',   
        hour=3,   
        minute=30,  
        id='daily_push_sync',
        args=[None] # تمرير None إذا كانت الدالة تنتظر المتغير spreadsheet ليتم جلبه داخلياً
    )  

    scheduler.start()  
    print("⏰ تم تشغيل المجدل الزمني المطور: المزامنة كل 15 دقيقة، والرفع الشامل 03:30 فجراً.")


# ==========================================================================
# ==========================================================================
# ==========================================================================
#دوال انشاء وصناعة البوتات تم نقلها الى ملف startbot.py
# ==========================================================================
# ==========================================================================
# ==========================================================================
# --------------------------------------------------------------------------
    # --- [ إعداد لوحة المفاتيح بناءً على الصلاحيات ] ---
async def owner_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """لوحة تحكم المطور (المالك) - متوافقة مع الأزرار الشفافة"""
    user_id = update.effective_user.id
    # التحقق من الصلاحية: يجب أن يكون المستخدم ضمن قائمة الإداريين أو المطور
    if user_id not in ALL_ADMINS:
        return
    # 1. أزرار متاحة لجميع الإداريين والمطور
    keyboard = [
        [InlineKeyboardButton("📊 إحصائيات البوتات", callback_data="stats_all")],
        [InlineKeyboardButton("📢 إذاعة للمشتركين", callback_data="broadcast_owners")],
        [InlineKeyboardButton("📥 تحميل نسخة", callback_data="download_cache_files")]
    ]
    # 2. أزرار حصرية للمطور فقط (DEVELOPER_ID) - الحفاظ على كافة الوظائف والمفاتيح
    if user_id == DEVELOPER_ID:
        config = get_bot_config(TOKEN)
        m_status = "🔴 (نشط)" if str(config.get("maintenance_mode", "FALSE")).upper() == "TRUE" else "🟢 (متوقف)"    	
        keyboard.extend([
            [InlineKeyboardButton("─── المحرك الهجين (SQLite) ───", callback_data="none")],
            [
                InlineKeyboardButton("📤 نسخة احتياطية للقناة", callback_data="backup_to_channel"),
                InlineKeyboardButton("🔄 استعادة من القناة", callback_data="restore_from_channel")
            ],
            
            [InlineKeyboardButton("─── عمليات النظام الحساسة ───", callback_data="none")],
            [InlineKeyboardButton("💳 إدارة الاشتراكات والترقيات", callback_data="manage_coaches")], 
            [
                InlineKeyboardButton(f"🛠 وضع الصيانة {m_status}", callback_data="toggle_maintenance")
            ],
            [InlineKeyboardButton("⚙️ تهيئة الجداول", callback_data="run_setup_db_now")],
            [
                InlineKeyboardButton("📤 رفع نسخة", callback_data="start_restore_request"),
                InlineKeyboardButton("⏳ بدء المزامنة اليدوية", callback_data="start_sync_shet")
            ],
            [
                InlineKeyboardButton("🔄 تحديث السيرفر", callback_data="restart_factory"), 
                InlineKeyboardButton("♻️ إعادة تشغيل", callback_data="reboot_system")
            ],
            [InlineKeyboardButton("👨‍💼 قسم الأدمن", callback_data="admin_section")], 
            [InlineKeyboardButton("⚠️ تصفير النظام بالكامل", callback_data="confirm_hard_reset")]
        ])

    # 3. زر العودة الدائم
    keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_to_main")])

    text = "🛠 **لوحة تحكم المطور والعمليات المركزية**\nمرحباً بك، اختر الإجراء المطلوب:"
    
    # التعامل مع الضغط من زر شفاف (Callback) أو أمر نصي
    if update.callback_query:
        try:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                text, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"⚠️ خطأ في تحديث لوحة التحكم: {e}")
    else:
        await update.message.reply_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
#~~~~~~~~~~~~~~~~
        
#  معالج الرسائل النصية الأزرار الدائمة  لجميع البوتات 
#  معالج الرسائل النصية الأزرار الدائمة 
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الرسائل النصية والأزرار الدائمة"""
    
    # ✅ حماية إضافية من الرسائل الفارغة أو غير النصية
    if not update.message or not update.message.text:
        return
    
    text = update.message.text.strip()  # تحسين: إزالة الفراغات
    user_id = update.effective_user.id

    # ✅ سجل تتبع (Debug بسيط)
    print(f"[MESSAGE] User:{user_id} Text:{text}")

    if text == "🔙 العودة للقائمة الرئيسية":
        await start(update, context)
        return
    # تم تصحيح المسافات البادئة والمنطق هنا بناءً على طلبك
    elif text == "🛠 لوحة التحكم (للأدمن)":
        if user_id == DEVELOPER_ID or user_id in ADMIN_IDS:
            await owner_dashboard(update, context)
        else:
            await update.message.reply_text("🚫 عذراً، هذه اللوحة مخصصة للإدارة فقط.")
            
    elif text == "➕ إنشاء بوت":
        await start_create_bot(update, context)
        
    elif text == "طلب_انضمام_الى_فريق_ادارة_المصنع":
        await process_admin_request(update, context)

    elif context.user_data.get("admin_action") == "manual_promote" and user_id == DEVELOPER_ID:
        try:
            target_id = int(text)
            await update.message.reply_text(f"✅ جاري ترقية العضو ذو الآيدي {target_id}...")
            await context.bot.send_message(chat_id=target_id, text="🎊 مبروك! تم قبول ترقيتك كأدمن في المصنع.")
            context.user_data["admin_action"] = None
        except ValueError:
            await update.message.reply_text("❌ خطأ: يرجى إرسال رقم آيدي (ID) صحيح فقط.")

    # تم استبدال ADMIN_ID بـ DEVELOPER_ID لضمان صلاحية المطور (أنت)
    elif text == "📝 تعديل النصوص" and user_id == DEVELOPER_ID:
        await update.message.reply_text("أرسل ID البوت أو التوكن الذي تريد تعديل نصوصه:")
        context.user_data["admin_action"] = "edit_texts"
        context.user_data["action_timestamp"] = asyncio.get_event_loop().time()  # ✅ تتبع الوقت

    # تم استبدال ADMIN_ID بـ DEVELOPER_ID هنا أيضاً لضمان استمرارية الوظيفة
    elif context.user_data.get("admin_action") == "edit_texts" and user_id == DEVELOPER_ID:
        
        # ✅ تحقق من انتهاء المهلة (Timeout حماية)
        action_time = context.user_data.get("action_timestamp")
        if action_time:
            now = asyncio.get_event_loop().time()
            if now - action_time > 300:  # 5 دقائق
                await update.message.reply_text("⏳ انتهت مهلة العملية، يرجى إعادة المحاولة.")
                context.user_data["admin_action"] = None
                return

        target_bot = text

        # ✅ تحقق بسيط من المدخل
        if len(target_bot) < 5:
            await update.message.reply_text("⚠️ الإدخال غير صالح، حاول مرة أخرى.")
            return

        context.user_data["target_bot"] = target_bot

        keyboard = [
            [InlineKeyboardButton("الرسالة الترحيبية", callback_data="set_welcome")],
            [InlineKeyboardButton("القوانين", callback_data="set_rules")]
        ]

        await update.message.reply_text(
            f"ماذا تريد أن تعدل في سجلات البوت {target_bot}؟", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

        context.user_data["admin_action"] = None

    # ✅ NEW: fallback لأي نص غير معروف
    else:
        await update.message.reply_text(
            "❓ لم أفهم طلبك.\n"
            "يرجى استخدام الأزرار أو اختيار أمر صحيح من القائمة."
        )

#>>>>>>>>>>>>>>>>#~~~~~~~~~~~~~~~~
# ✅ سجل تتبع مركزي
def log_action(user_id, action):
    print(f"[CALLBACK] User:{user_id} Action:{action}")

# ✅ دالة موحدة للرفض (بدون تغيير الكود الأصلي)
async def deny_access(query, message="🚫 لا تمتلك صلاحية."):
    try:
        await query.answer(message, show_alert=True)
    except:
        pass
        

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة ضغطات الأزرار الشفافة المركزية"""
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    # --- [ إضافة جديدة: سجل التتبع ] ---
    log_action(user_id, data)
    
    await query.answer() # لإيقاف مؤشر التحميل في تليجرام
        # --- [ الإجراء الجديد: إرسال نسخة للقناة ] ---
    if data == "backup_to_channel":
        if user_id != DEVELOPER_ID: return
        
        await query.edit_message_text("⏳ جاري تشفير قاعدة البيانات وإرسالها للقناة...")
        try:
            # استدعاء الدالة التي صممناها في كلاس DataManager
            await db_manager.create_backup_to_telegram()
            await query.edit_message_text("✅ تم إرسال النسخة الاحتياطية المشفرة إلى القناة بنجاح! 🛡️")
        except Exception as e:
            await query.edit_message_text(f"❌ فشل الإرسال: {str(e)}")

  

    elif data == "confirm_hard_reset":
        # --- [ إضافة جديدة: حماية المطور ] ---
        if user_id != DEVELOPER_ID:
            await deny_access(query, "⚠️ هذا الإجراء الخطير متاح للمطور الأساسي فقط.")
            return

        keyboard = [
            [InlineKeyboardButton("✅ نعم، متأكد", callback_data="execute_hard_reset")],
            [InlineKeyboardButton("❌ تراجع", callback_data="dev_panel")]
        ]
        await query.edit_message_text("‼️ **تحذير حرج:**\nهذا الإجراء سيحذف كافة البيانات في جوجل شيت. هل أنت متأكد؟", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    elif data == "execute_hard_reset":
        # --- [ إضافة جديدة: حماية المطور ] ---
        if user_id != DEVELOPER_ID:
            return

        await query.edit_message_text("⏳ جاري التصفير...")
        if reset_entire_database():
            await query.edit_message_text("✅ تم تصفير النظام بنجاح.\nيرجى إعادة تشغيل السيرفر الآن.")
        else:
            await query.edit_message_text("❌ فشلت العملية. راجع السجلات.")



    elif data in ["restart_factory", "run_pull_sync_manual"]:
        if user_id != DEVELOPER_ID:
            await deny_access(query, "🚫 إعادة تشغيل المصنع صلاحية حصرية للمطور.")
            return

        # 1. إرسال رسالة تنبيه ببدء العملية (لتحسين تجربة المستخدم)
        await query.edit_message_text(
            "🔄 <b>جاري تحديث كاش المصنع...</b>\n"
            "يتم الآن إعادة سحب كافة البيانات من جوجل شيت وتحديث المحرك المحلي.",
            parse_mode="HTML"
        )

        try:
            # 2. استدعاء وظيفة تحديث الكاش الشاملة
            from cache_manager import fetch_full_factory_data
            
            # ملاحظة: إذا كانت الدالة async استخدم await، وإذا كانت def عادية اتركها كما هي
            # بناءً على سجلاتك السابقة، المحرك يحتاج لتنفيذ المزامنة الشاملة
            fetch_full_factory_data()

            # 3. تحديث الرسالة بعد الانتهاء بنجاح
            await query.edit_message_text(
                "✅ <b>تم تحديث المصنع بنجاح!</b>\n"
                "تمت مزامنة كافة الإعدادات والاشتراكات مع الكاش المحلي.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
                ]),
                parse_mode="HTML"
            )
        except Exception as e:
            # معالجة الخطأ في حال فشل الاتصال بجوجل أثناء التحديث
            await query.edit_message_text(
                f"❌ <b>فشل تحديث الكاش:</b>\n<code>{str(e)}</code>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
                ]),
                parse_mode="HTML"
            )


#~~~~~~~~~~~~~~~~
    # --- [ معالج زر إعادة تشغيل المحرك لقتل النسخ المتضاربة ] ---
    elif data == "reboot_system":
        if user_id != DEVELOPER_ID:
            await deny_access(query)
            return

        if restart_bot_logic:
            await restart_bot_logic(update, context)
        else:
            await query.answer("⚠️ موديول course_engine غير متاح حالياً.", show_alert=True)

# ==========================================================================


    # 1. الدخول للوحة إدارة الاشتراكات (يدعم الصفحات الآن)
    elif data == "manage_subscriptions" or data.startswith("bots_page_"):
        if user_id != DEVELOPER_ID: return
        
        page = int(data.replace("bots_page_", "")) if data.startswith("bots_page_") else 0
        from SubscriptionManager import get_all_bots_keyboard
        
        # استخدام نظام Pagination الجديد المضاف للملف
        kb = get_all_bots_keyboard(page=page)
        await query.edit_message_text(
            f"📋 **قائمة البوتات المصنوعة (صفحة {page + 1}):**\nإختر البوت الذي تريد إدارة اشتراكه:", 
            reply_markup=kb
        )

    # 2. عرض تفاصيل بوت معين (واجهة UX المطورة)
    elif data.startswith("sub_view_"):
        bot_token_to_view = data.replace("sub_view_", "")
        from SubscriptionManager import get_bot_subscription_interface
        
        # استدعاء الواجهة التي تعرض المدة المتبقية والمميزات
        text, reply_markup = get_bot_subscription_interface(bot_token_to_view)
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="Markdown")

    # 3. تنفيذ عملية الترقية (يدعم المدة الزمنية التراكمية)
    elif data.startswith("exec_sub_"):
        # تنسيق الداتا الجديد: exec_sub_TOKEN_PLAN_DAYS
        parts = data.split("_")
        if len(parts) < 5: return # تم تعديل التحقق ليتناسب مع عدد الأجزاء (exec, sub, token, plan, days)
        
        target_token = parts[2]
        target_plan = parts[3]
        target_days = int(parts[4]) if len(parts) > 4 else 30

        from SubscriptionManager import upgrade_bot_plan, _fetch_bot_by_token
        
        await query.edit_message_text(f"⏳ جاري تنفيذ الترقية لباقة {target_plan} لمدة {target_days} يوم...")
        
        # تنفيذ الترقية الفعلية والتحقق من النتيجة
        if await upgrade_bot_plan(target_token, target_plan, duration_days=target_days):
            # --- ميزة إخطار صاحب البوت (بدون تغيير في منطق الترقية) ---
            bot_info = _fetch_bot_by_token(target_token)
            if bot_info and bot_info.get("ID المالك"):
                try:
                    owner_id = bot_info["ID المالك"]
                    notification_text = (
                        f"🎉 تهانينا! تم تحديث اشتراك بوتك: **{bot_info['اسم البوت']}**\n"
                        f"📊 الباقة الجديدة: **{target_plan.upper()}**\n"
                        f"⏳ المدة المضافة: **{target_days} يوم**\n\n"
                        f"✨ يمكنك الآن الاستمتاع بكافة المميزات الجديدة."
                    )
                    await context.bot.send_message(chat_id=owner_id, text=notification_text, parse_mode="Markdown")
                except Exception as e:
                    print(f"⚠️ Could not notify owner: {e}")
            # -------------------------------------------------------
            
            await query.edit_message_text(
                f"✅ تم تحديث اشتراك البوت بنجاح!\n"
                f"📦 الباقة: **{target_plan}**\n"
                f"🗓️ المدة المضافة: **{target_days} يوم**",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("❌ فشلت عملية الترقية، راجع سجلات السيرفر (SUB LOG).")

    # 4. تمديد الاشتراك الحالي (الميزة الجديدة)
    elif data.startswith("extend_sub_"):
        target_token = data.replace("extend_sub_", "")
        from SubscriptionManager import _fetch_bot_by_token, upgrade_bot_plan
        
        bot_data = _fetch_bot_by_token(target_token)
        if bot_data:
            current_plan = bot_data['plan'].upper()
            await query.edit_message_text(f"⏳ جاري تمديد باقة {current_plan} لمدة 30 يوم إضافية...")
            
            if await upgrade_bot_plan(target_token, current_plan, duration_days=30):
                # --- إخطار صاحب البوت بالتمديد ---
                if bot_data.get("ID المالك"):
                    try:
                        owner_id = bot_data["ID المالك"]
                        ext_text = (
                            f"✅ تم تمديد اشتراك بوتك: **{bot_data['اسم البوت']}**\n"
                            f"🗓️ المدة المضافة: **30 يوم إضافي**\n"
                            f"📊 الباقة الحالية: **{current_plan}**"
                        )
                        await context.bot.send_message(chat_id=owner_id, text=ext_text, parse_mode="Markdown")
                    except Exception as e:
                        print(f"⚠️ Could not notify owner about extension: {e}")
                # ----------------------------------
                
                await query.edit_message_text(f"✅ تم تمديد باقة **{current_plan}** بنجاح لمدة شهر إضافي!")
            else:
                await query.edit_message_text("❌ فشل التمديد.")

# ==========================================================================


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
    elif data == "start_sync_shet":
        if user_id != DEVELOPER_ID: 
            return
        
        text = (
            "🔄 <b>مركز المزامنة اليدوية والتحكم بالسحابة:</b>\n\n"
            "إختر نوع العملية المطلوبة للتحكم في تدفق البيانات:\n\n"
            "📥 <b>شيت ⬅️ الرام:</b> سحب البيانات من جوجل وتحديث البوت فوراً.\n"
            "📤 <b>الرام ⬅️ الشيت:</b> تأمين بيانات البوت الحالية ورفعها للسحابة."
        )
        
        keyboard = [
            # الزر الأول: يستدعي دالة fetch_full_factory_data (Pull)
            [InlineKeyboardButton("📥 شيت ⬅️ الرام (تحديث البوت)", callback_data="run_pull_sync_manual")],
            
            # الزر الثاني: يستدعي دالة sync_factory_to_sheets_smart (Push)
            [InlineKeyboardButton("📤 الرام ⬅️ الشيت (تأمين البيانات)", callback_data="run_push_sync_manual")],
            
            # زر العودة المطلوبة
            [InlineKeyboardButton("🔙 عودة", callback_data="open_admin_dashboard")]
        ]
        
        await query.edit_message_text(
            text=text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode="HTML"
        )





    # --- [ معالج زر بدء المزامنة اليدوية ] ---
    elif data == "run_push_sync_manual":
        # --- [ إضافة جديدة: حماية الإدارة ] ---
        if user_id not in ALL_ADMINS:
            await deny_access(query)
            return

        # إرسال رسالة أولية للمستخدم
        msg = await query.edit_message_text("🔄 جاري بدء مزامنة المصنع مع السحابة... يرجى الانتظار")
        
        try:
            # استدعاء دالة المزامنة الذكية التي صممناها في cache_manager
            from cache_manager import sync_factory_to_sheets_smart

            # تشغيل المزامنة
            await sync_factory_to_sheets_smart()
            
            # تحديث الرسالة بعد النجاح
            await query.edit_message_text("✅ اكتملت المزامنة اليدوية بنجاح وتم تحديث كافة البيانات.")
        except Exception as e:
            await query.edit_message_text(f"❌ فشلت المزامنة اليدوية: {str(e)}")
        
    elif data == "open_admin_panel" or data == "open_admin_dashboard":
        # --- [ إضافة جديدة: حماية الإدارة ] ---
        if user_id not in ALL_ADMINS:
            await deny_access(query)
            
    elif data.startswith("promote_user_") or data.startswith("reject_user_") or data == "manual_add_admin":
        await handle_admin_promotion_callbacks(update, context)
        return


        await owner_dashboard(update, context)
        
    elif data == "download_cache_files":
        # --- [ إضافة جديدة: حماية الإدارة ] ---
        if user_id not in ALL_ADMINS:
            await deny_access(query)
            return

        await download_bot_cache(update, context)
        
    elif data == "start_restore_request":
        # 1. حماية المطور (صحيحة)
        if user_id != DEVELOPER_ID:
            await deny_access(query, "📥 نظام الاستعادة متاح للمطور فقط.")
            return

        # 2. السطر الجوهري (المفقود عندك): تفعيل حالة الانتظار
        # بدون هذا السطر، لن تعمل دالة handle_document التي صممناها
        context.user_data['action'] = 'awaiting_json_backup'

        await query.answer()
        await query.edit_message_text(
            "📥 <b>نظام الاستعادة (المحرك المطور):</b>\n"
            "━━━━━━━━━━━━━━\n"
            "يرجى إرسال ملف النسخة الاحتياطية المشفر <code>.json</code> الآن.\n\n"
            "⚠️ <b>ملاحظة:</b> سيتم فك التشفير وتحديث الـ 37 ورقة فوراً.", 
            parse_mode="HTML"
        )

        
    # استعادة النسخة - القرار النهائي
    elif data == "confirm_restore":
        # --- [ حماية المطور ] ---
        if str(user_id) != str(DEVELOPER_ID):
            await query.answer("🚫 غير مصرح لك.", show_alert=True)
            return

        # جلب محتوى الملف الذي تم تخزينه مؤقتاً عند استلامه في handle_document
        content = context.user_data.get('pending_restore_content')
        if not content:
            await query.edit_message_text("❌ انتهت صلاحية الجلسة أو الملف غير موجود، يرجى المحاولة مجدداً.")
            return

        await query.answer("جاري بدء الاستعادة الشاملة...")
        
        # إظهار رسالة بدء العمليات (المرحلة 1)
        await query.edit_message_text("⏳ <b>المرحلة 1:</b> جاري فك التشفير Base64 وتحديث الذاكرة المحلية...", parse_mode="HTML")
        
        from cache_manager import restore_from_telegram
        
        # تنفيذ المحرك المطور (يتعامل مع الـ 37 ورقة والـ 41 عموداً)
        success = await restore_from_telegram(content, user_id)
        
        if success:
            # المرحلة 2: محاكاة بصرية للمستخدم (المزامنة الفعلية تمت داخل الدالة)
            await query.edit_message_text("📡 <b>المرحلة 2:</b> نجح تحديث السيرفر، جاري تأكيد مزامنة الـ 37 ورقة مع Google Sheets...", parse_mode="HTML")
            
            import asyncio
            await asyncio.sleep(2) 
            
            # الرسالة النهائية للنجاح (كما في كودك الأصلي)
            await query.edit_message_text(
                "🎊 <b>تمت الاستعادة والمزامنة بنجاح!</b>\n"
                "━━━━━━━━━━━━━━\n"
                "✅ تم تحديث Google Sheets.\n"
                "✅ تم شحن كاش الرام.\n"
                "✅ تم تحديث مرآة القرص.", 
                parse_mode="HTML"
            )
        else:
            # في حال فشل المحرك (خطأ في التشفير أو الأوراق)
            await query.edit_message_text("❌ فشلت عملية الاستعادة. الملف قد يكون تالفاً أو هناك خلل في الاتصال بجوجل شيت.")
        
        # تنظيف الذاكرة المؤقتة لضمان أمن البيانات
        context.user_data.pop('pending_restore_content', None)
        context.user_data['action'] = None

    elif data == "cancel_restore":
        context.user_data.pop('pending_restore_content', None)
        context.user_data['action'] = None
        await query.answer("تم الإلغاء")
        await query.edit_message_text("❌ تم إلغاء عملية الاستعادة ومسح الملف المؤقت بنجاح.")

    elif data == "back_to_main":
        context.user_data['action'] = None # تصفير أي أكشن معلق عند العودة
        await query.answer()
        await query.edit_message_text(
            "✨ أهلاً بك في مصنع البوتات المتطور 🤖\n\nاختر ما تريد القيام به:",
            parse_mode="HTML", 
            reply_markup=get_main_menu_inline(user_id)
        )

        
    elif data == "open_admin_panel":
        await query.answer()
        await owner_dashboard(update, context)      

        
# --------------------------------------------------------------------------

    # تهيئة الورق والإعدادات - النسخة الاحترافية النهائية
    elif data == "run_setup_db_now":
        # --- [ إضافة جديدة: حماية المطور ] ---
        if user_id != DEVELOPER_ID:
            await deny_access(query, "⚙️ تهيئة الجداول متاحة للمطور فقط.")
            return

        # 1. نظام الحماية من التشغيل المزدوج
        if context.user_data.get("setup_running"):
            await query.answer("⚠️ العملية قيد التنفيذ بالفعل...", show_alert=True)
            return

        context.user_data["setup_running"] = True
        context.user_data["cancel_setup"] = False
        
        loading_colors = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣"]
        base_loading_msg = (
            "⏳ <b>جاري تشغيل محركات المصنع...</b>\n"
            "━━━━━━━━━━━━━━\n"
            "🔄 جاري فحص وإنشاء جداول قاعدة البيانات...\n"
            "🎨 جاري تنسيق الصفوف والألوان تلقائياً...\n"
            "⚙️ جاري زرع الإعدادات الافتراضية للبوت...\n\n"
            "<i>يرجى الانتظار، لا تغلق هذه الصفحة...</i>"
        )

        from sheets import setup_bot_factory_database
        import time

        # 2. تشغيل المهمة في مسار خلفي لضمان استجابة البوت
        setup_task = asyncio.create_task(asyncio.to_thread(setup_bot_factory_database, context.bot.token))
        
        try:
            color_index = 0
            start_time = time.time()
            
            # 3. حلقة الوميض وشريط التقدم (Loop)
            while not setup_task.done():
                if context.user_data.get("cancel_setup"):
                    setup_task.cancel()
                    break

                # حساب التقدم (Progress Bar)
                elapsed = time.time() - start_time
                progress = min(98, int((elapsed / 60) * 100))
                bar = "🟩" * (progress // 10) + "⬜" * (10 - (progress // 10))
                
                current_color = loading_colors[color_index % len(loading_colors)]
                status_text = (
                    f"{current_color} {base_loading_msg}\n\n"
                    f"📊 <b>التقدم:</b> [{bar}] {progress}%\n"
                    f"⏱️ الوقت المنقضي: {int(elapsed)} ثانية"
                )

                try:
                    # تحديث الرسالة كل 2.5 ثانية (التوقيت الذهبي)
                    await query.edit_message_text(
                        status_text, 
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء", callback_data="cancel_setup")]]),
                        parse_mode="HTML"
                    )
                except:
                    pass

                color_index += 1
                await asyncio.sleep(2.5) 

            # 4. انتظار النتيجة النهائية
            try:
                result = await setup_task
                if isinstance(result, int) and result > 0:
                    sheets_count = int(result)
                else:
                    sheets_count = total_sheets if result else 0
                    
                if sheets_count > 0:
                    result_text = (
                        "✅ <b>تمت العملية بنجاح!</b>\n"
                        "━━━━━━━━━━━━━━\n"
                        f"📦 تم إنشاء وتنسيق (<b>{sheets_count} ورقة</b>) بالكامل.\n"
                        "🛡️ نظام الحماية والتحقق من المخطط (Schema) نشط الآن."
                    )

                else:
                    result_text = "⚠️ <b>النظام مهيأ بالفعل!</b>\nالجداول موجودة ومحدثة."
                    # التأخير يكون سطر برمجي مستقل وليس داخل علامات التنصيص
                    await asyncio.sleep(5)

                from cache_manager import fetch_full_factory_data
                fetch_full_factory_data()
                    
            except Exception as e:
                print(f"❌ خطأ في التهيئة: {e}")
                result_text = f"❌ <b>فشلت العملية!</b>\nحدث خطأ أثناء المعالجة: {str(e)}"
                
            finally:
                context.user_data["setup_running"] = False

            # إرسال الرسالة النهائية
            keyboard = [[InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="open_admin_panel")]]
            await query.edit_message_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

        except:
            pass
# --------------------------------------------------------------------------
#دالة نسخ بيانات البوتات المدفوعة الاشتراكات
    elif data == "manage_coaches":
        if user_id != DEVELOPER_ID: 
            return
        text = (
            "🤖 <b>إدارة البوتات والاشتراكات:</b>\n\n"
            "يمكنك استعراض البوتات وإدارة النسخ الاحتياطية لنظام الاشتراكات."
        )
        keyboard = [
            [InlineKeyboardButton("📋 استعراض البوتات", callback_data="manage_subscriptions")],
            [InlineKeyboardButton("💾 إنشاء نسخة احتياطية للاشتراكات", callback_data="backup_subs")],
            [InlineKeyboardButton("♻️ استعادة نسخة احتياطية", callback_data="confirm_restorebotvip")],
            [InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="tech_settings")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data == "backup_subs":
        await query.answer("جاري تجهيز النسخة الاحتياطية...")
        
        from SubscriptionManager import export_subscriptions_backup
        backup_json = export_subscriptions_backup()
        
        if backup_json:
            # تحويل النص إلى ملف مؤقت لإرساله
            import io
            file_stream = io.BytesIO(backup_json.encode('utf-8'))
            file_stream.name = f"subs_backup_{datetime.now().strftime('%Y%m%d')}.json"
            
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=file_stream,
                caption=f"✅ تم استخراج نسخة احتياطية للاشتراكات\n📅 التاريخ: {datetime.now().strftime('%Y-%m-%d')}"
            )
        else:
            await query.edit_message_text("❌ فشل في استخراج البيانات.")

    # هذا الكود يوضع داخل CallbackQueryHandler تحت شرط confirm_restore
    elif data == "confirm_restorebotvip":
        await query.answer("جاري استعادة البيانات...")
        
        # جلب المحتوى الذي تم حفظه مؤقتاً عند رفع الملف
        backup_content = context.user_data.get('pending_restore_content')
        
        if not backup_content:
            await query.edit_message_text("❌ لم يتم العثور على بيانات لاستعادتها. يرجى رفع الملف مرة أخرى.")
            return

        from SubscriptionManager import import_subscriptions_from_backup
        
        # تنفيذ الاستعادة
        success = await import_subscriptions_from_backup(backup_content)
        
        if success:
            await query.edit_message_text("✅ تم استعادة كافة الاشتراكات بنجاح وتحديث قاعدة البيانات.")
            # تنظيف الذاكرة المؤقتة
            del context.user_data['pending_restore_content']
        else:
            await query.edit_message_text("❌ حدث خطأ أثناء الاستعادة. تأكد من أن الملف سليم وغير معدل يدويًا.")


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# --- نهاية معالج الأزرار وبداية الدوال المستقلة ---
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# دالة استعراض الأدمن 
async def show_admins_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != DEVELOPER_ID:
        return

    await query.answer()

    admins_list = list(ALL_ADMINS)

    if not admins_list:
        await query.message.edit_text("⚠️ لا يوجد أي أدمن حالياً.")
        return

    text = "🛡 <b>لوحة إدارة الأدمن</b>\n"
    text += "━━━━━━━━━━━━━━━\n\n"

    for i, admin_id in enumerate(admins_list, start=1):
        role = "👑 المطور" if admin_id == DEVELOPER_ID else "🛠 أدمن"
        text += f"{i}. <code>{admin_id}</code>\n"
        text += f"   ↳ {role}\n\n"

    text += "━━━━━━━━━━━━━━━\n"
    text += f"📊 العدد الكلي: {len(admins_list)}\n"

    keyboard = []

    for admin_id in admins_list:
        if admin_id != DEVELOPER_ID:
            keyboard.append([
                InlineKeyboardButton(
                    f"❌ حذف {admin_id}",
                    callback_data=f"remove_admin_{admin_id}"
                )
            ])

    keyboard.append([
        InlineKeyboardButton("➕ إضافة أدمن", callback_data="manual_add_admin")
    ])

    keyboard.append([
        InlineKeyboardButton("❌ حذف أدمن", callback_data="show_admins_for_delete")
    ])
    keyboard.append([
        InlineKeyboardButton("🔄 تحديث", callback_data="refresh_admins")
    ])

    keyboard.append([
        InlineKeyboardButton("🔙 رجوع", callback_data="open_admin_dashboard")
    ])

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    ) 
    
#دالة حذف الأدمن 
async def handle_admin_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if query.from_user.id != DEVELOPER_ID:
        return

    if data.startswith("remove_admin_"):
        target_id = int(data.replace("remove_admin_", ""))

        if target_id == DEVELOPER_ID:
            await query.answer("❌ لا يمكن حذف المطور", show_alert=True)
            return

        if target_id in ALL_ADMINS:
            ALL_ADMINS.remove(target_id)

        await query.answer("✅ تم حذف الأدمن")

        # إعادة عرض القائمة
        await show_admins_dashboard(update, context)

    elif data == "refresh_admins":
        await show_admins_dashboard(update, context)  

# --------------------------------------------------------------------------


# ================================
# 📋 عرض الأدمن للحذف
# ================================
async def show_admins_for_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    admins = list(ALL_ADMINS)

    if not admins:
        await query.message.edit_text("⚠️ لا يوجد أدمن حالياً.")
        return

    text = "❌ <b>اختر الأدمن الذي تريد حذفه:</b>\n\n"

    keyboard = []

    for admin_id in admins:
        if admin_id != DEVELOPER_ID:
            keyboard.append([
                InlineKeyboardButton(
                    f"حذف {admin_id}",
                    callback_data=f"remove_admin_{admin_id}"
                )
            ])

    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="admin_section")])

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
# --------------------------------------------------------------------------
#دالة الاذاعة المركزية 
async def start_global_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تبدأ عملية الإذاعة وتعرض التعليمات"""
    query = update.callback_query
    await query.answer()
    
    instruction_text = (
        "📢 **إذاعة — 👥 الجميع**\n"
        "━━━━━━━━━━━━━━━\n\n"
        "أرسل الرسالة التي تريد إذاعتها\n"
        "نص، صورة، فيديو، مستند، صوت، ملصق — أي نوع مدعوم\n\n"
        "يمكنك التنسيق من تيليجرام مباشرة أو استخدام Markdown / HTML\n\n"
        "📝 **التنسيقات المدعومة**\n\n"
        "أرسل الرسالة بأي تنسيق وسيتم التعرّف عليه تلقائيًا ✨\n"
        "━━━━━━━━━━━━━━━\n"
        "✏️ **التنسيق من تيليجرام**\n"
        "الطريقة الأسهل! نسّق رسالتك مباشرةً من تيليجرام:\n"
        "• حدّد النص ← اختر التنسيق (عريض, مائل, ...)\n"
        "• أرسل الرسالة المنسّقة كما هي وسيتم حفظها بالتنسيق.\n\n"
        "━━━━━━━━━━━━━━━\n"
        "📖 **Markdown**\n\n"
        "*عريض* ← عريض\n"
        "_مائل_ ← مائل\n"
        "__خط سفلي__ ← خط سفلي\n"
        "~يتوسطه خط~ ← يتوسطه خط\n"
        "||مخفي|| ← مخفي (اضغط لإظهار)\n"
        "[نص](https://...) ← رابط قابل للضغط\n"
        "`كود` ← كود\n"
        "━━━━━━━━━━━━━━━\n"
        "🌐 **HTML**\n\n"
        "<b>نص</b> ← عريض\n"
        "<i>نص</i> ← مائل\n"
        "<u>نص</u> ← خط سفلي\n"
        "<s>نص</s> ← يتوسطه خط\n"
        "<tg-spoiler>نص</tg-spoiler> ← مخفي\n"
        "<code>نص</code> ← كود\n"
        "<blockquote>نص</blockquote> ← اقتباس\n\n"
        "━━━━━━━━━━━━━━━\n"
        "✨ **إيموجي مخصص**\n"
        "أرسل إيموجي مخصصًا (Premium) في رسالتك وسيتم دعمه تلقائيًا."
    )
    
    keyboard = [[InlineKeyboardButton("❌ إلغاء الإذاعة", callback_data="cancel_broadcast")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text=instruction_text, reply_markup=reply_markup, parse_mode="Markdown")
    return WAITING_BROADCAST_CONTENT

async def process_global_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message

    # 🔒 حماية من None
    if not msg:
        return ConversationHandler.END

    status_msg = await msg.reply_text("🔍 جاري فحص قاعدة البيانات بالمعايير الجديدة...")
    
    data_store = FACTORY_GLOBAL_CACHE.get("data", {})
    
    # 🔒 تحقق من نوع البيانات
    if not isinstance(data_store, dict):
        data_store = {}

    # جلب البيانات (دعم مسمى المستخدمين كما يظهر في السجلات لديك)
    all_users = data_store.get("المستخدمين") or []
    all_bots = data_store.get("البوتات_المصنوعة") or []
    
    # 🔒 ضمان النوع
    if not isinstance(all_users, list):
        all_users = []
    if not isinstance(all_bots, list):
        all_bots = []

    target_chats = set()
    free_tokens = set()
    
    # 1. تحديد البوتات المجانية (الخطة في عمود plan والتوكن في عمود التوكن)
    for bot in all_bots:
        try:
            plan_value = str(bot.get("plan", "")).lower().strip()
            bot_token = bot.get("التوكن")
            
            if plan_value == "free" and bot_token:
                free_tokens.add(str(bot_token).strip())
        except Exception as e:
            print(f"[ERROR][BOT_PARSE] {e}")

    # 2. جمع معرفات المستخدمين
    for user in all_users:
        try:
            u_id = user.get("ID المستخدم") 
            b_token = user.get("توكن_البوت") or user.get("bot_token")
            
            if u_id:
                clean_id_str = re.sub(r"[^\d-]", "", str(u_id).strip())
                
                # 🔒 تحقق أقوى
                if clean_id_str and re.fullmatch(r"-?\d+", clean_id_str):
                    uid = int(clean_id_str)
                    
                    if not b_token or str(b_token).strip() in ["", "MAIN", "MAIN_FACTORY"]:
                        target_chats.add(uid)
                    
                    elif str(b_token).strip() in free_tokens:
                        target_chats.add(uid)

        except Exception as e:
            print(f"[ERROR][USER_PARSE] {user} => {e}")
            continue

    # 🔒 حالة عدم وجود أهداف (إيقاف فعلي)
    if not target_chats:
        await status_msg.edit_text(
            f"❌ لم يتم العثور على أهداف مطابقة.\n\n"
            f"📊 ملخص الفحص:\n"
            f"• مستخدمين في الكاش: {len(all_users)}\n"
            f"• بوتات مجانية مكتشفة: {len(free_tokens)}\n\n"
            f"💡 تأكد أن عمود التوكن في ورقة المستخدمين مطابق تماماً لعمود التوكن في ورقة البوتات."
        )
        return ConversationHandler.END

    # 🔒 علم لمنع التكرار
    broadcast_executed = False

    # 4. محرك الإرسال الفعلي (النسخة الأولى)
    if not broadcast_executed:
        await status_msg.edit_text(f"🚀 جاري الإرسال لـ {len(target_chats)} مستهدف...")
        success, failed = 0, 0
        
        for chat_id in target_chats:
            try:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                success += 1

                # 🔒 Rate Limit ذكي
                if success % 20 == 0:
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(0.05)

            except Exception as e:
                print(f"⚠️ فشل الإرسال إلى {chat_id}: {e}")
                failed += 1

        # 5. التقرير النهائي
        await status_msg.edit_text(
            f"📢 **اكتملت الإذاعة بنجاح!**\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"✅ تم التسليم لـ: {success}\n"
            f"🚫 فشل (حظر/خطأ): {failed}\n"
            f"🎯 الإجمالي المستهدف: {len(target_chats)}\n\n"
            f"✨ تم الفلترة بناءً على خطة (Free) فقط."
        )

        broadcast_executed = True

    # 6. الإرسال الفعلي (النسخة الثانية)
    if not broadcast_executed:
        await status_msg.edit_text(f"🚀 جاري الإرسال لـ {len(target_chats)} مستلم...")
        success, failed = 0, 0
        
        for chat_id in target_chats:
            try:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                success += 1

                if success % 20 == 0:
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(0.05)

            except Exception as e:
                print(f"⚠️ فشل للإيدي {chat_id}: {e}")
                failed += 1

        await status_msg.edit_text(
            f"✅ **اكتملت الإذاعة بنجاح!**\n\n"
            f"📬 تم الإرسال لـ: {success}\n"
            f"🚫 فشل/حظر: {failed}\n"
            f"🎯 الإجمالي: {len(target_chats)}"
        )

    return ConversationHandler.END

async def cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("❌ تم إلغاء عملية الإذاعة.")
    return ConversationHandler.END

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------


# --- [ إضافة جديدة: سجل تتبع العمليات ] ---
def log_cancel_action(user_id):
    print(f"[CANCEL_ACTION] User:{user_id} has terminated a conversation flow.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """دالة إلغاء عملية إنشاء البوت والعودة للقائمة الرئيسية"""
    user_id = update.effective_user.id
    
    # --- [ إضافة جديدة: تسجيل العملية في السيرفر ] ---
    log_cancel_action(user_id)
    
    text = "❌ تم إلغاء عملية الإنشاء والعودة للقائمة الرئيسية."
    
    if update.callback_query:
        await update.callback_query.answer()
        # --- [ إضافة جديدة: التحقق من صلاحيات الواجهة قبل العرض ] ---
        reply_markup = get_main_menu_inline(user_id)
        await update.callback_query.edit_message_text(text, reply_markup=get_main_menu_inline(user_id))
    else:
        # --- [ إضافة جديدة: تأمين الرد للمطورين والإداريين ] ---
        reply_markup = get_main_menu_inline(user_id)
        await update.message.reply_text(text, reply_markup=get_main_menu_inline(user_id))
        
    context.user_data.clear()
    
    # --- [ إضافة جديدة: ضمان تنظيف الذاكرة المؤقتة للإدارة ] ---
    if user_id in set(ALL_ADMINS):
        context.user_data.pop("admin_action", None)
        context.user_data.pop("setup_running", None)

    return ConversationHandler.END

# --------------------------------------------------------------------------
 
# ==========================================
# 🔒 طبقة الحماية والاستقرار (إضافات فقط)
# ==========================================

if "operation_lock" not in globals():
    operation_lock = asyncio.Lock()


def sync_twin_keys(context):
    try:
        if "pending_twin_file" in context.user_data and "twin_waiting" not in context.user_data:
            context.user_data["twin_waiting"] = context.user_data["pending_twin_file"]
        if "twin_waiting" in context.user_data and "pending_twin_file" not in context.user_data:
            context.user_data["pending_twin_file"] = context.user_data["twin_waiting"]
    except: pass


async def store_env_file_temporarily(file_name, file_obj, context):
    try:
        temp_path = f"./temp_{file_name}"
        await file_obj.download_to_drive(temp_path)
        context.user_data.setdefault("env_temp_files", {})
        context.user_data["env_temp_files"][file_name] = temp_path
    except Exception as e:
        print(f"⚠️ خطأ في حفظ ملف البيئة مؤقتاً: {e}")


async def safe_process_file_decision(update, context, file_name, file_obj):
    async with operation_lock:
        try:
            sync_twin_keys(context)
            if file_name in [".Dockerfile", "requirements.txt"]:
                await store_env_file_temporarily(file_name, file_obj, context)
            return await process_file_decision(update, context, file_name, file_obj)
        except Exception as e:
            print(f"❌ خطأ في محرك القرار: {e}")
            await update.message.reply_text("❌ حدث خطأ أثناء معالجة الملف")
            return "ERROR"


# ==========================================
# ⚙️ محرك القرار (كما هو بدون أي تغيير)
# ==========================================
async def process_file_decision(update: Update, context: ContextTypes.DEFAULT_TYPE, file_name, file_obj):
    user_id = update.effective_user.id

    env_files = [".Dockerfile", "requirements.txt"]
    core_files = ["main.py", "sheets.py", "cache_manager.py"]
    bot_files = ["education_bot.py", "store_bot.py", "contact_bot.py", "protection_bot.py", "downloader_bot.py"]
    logic_files = ["course_engine.py", "educational_manager.py"]

    if file_name in env_files:
        twin = "requirements.txt" if file_name == ".Dockerfile" else ".Dockerfile"
        context.user_data["pending_twin_file"] = file_name

        if context.user_data.get("twin_waiting") == twin:
            keyboard = [
                [InlineKeyboardButton("✅ نعم، إتمام التحديث", callback_data="confirm_env_update")],
                [InlineKeyboardButton("❌ تراجع", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"⚠️ <b>تحذير عالي الخطورة:</b>\nاكتمل التوأم ({file_name} + {twin}).\nهل تريد الإتمام؟",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            return "WAITING_CONFIRM"
        else:
            context.user_data["twin_waiting"] = file_name
            await update.message.reply_text(
                f"📦 تم استلام <code>{file_name}</code>.\nأرسل التوأم <code>{twin}</code>",
                parse_mode="HTML"
            )
            return "TWIN_MISSING"

    if file_name in core_files:
        await file_obj.download_to_drive(f"./{file_name}")
        await update.message.reply_text(f"✅ تم استبدال ملف النظام <code>{file_name}</code>", parse_mode="HTML")
        os.execv(sys.executable, ['python'] + sys.argv)

    if file_name in bot_files:
        await file_obj.download_to_drive(f"./{file_name}")
        await update.message.reply_text(f"✅ تم استبدال بوت <code>{file_name}</code>", parse_mode="HTML")
        return "SUCCESS_DIRECT"

    if file_name in logic_files:
        await file_obj.download_to_drive(f"./{file_name}")

        if file_name == "course_engine.py":
            import course_engine; importlib.reload(course_engine)
        elif file_name == "educational_manager.py":
            import educational_manager; importlib.reload(educational_manager)

        await update.message.reply_text(f"⚙️ Hot Reload تم لـ <code>{file_name}</code>", parse_mode="HTML")
        return "SUCCESS_RELOAD"

    return "PLUGIN"


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    مستلم المستندات: يقوم بالتقاط ملف النسخة الاحتياطية وتجهيزه للتأكيد اليدوي
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from sheets import DEVELOPER_ID

    user_id = update.effective_user.id
    action = context.user_data.get('action')
    doc = update.message.document

    # 1. طباعة فحص فوري في السجلات للتأكد من التقاط البوت للملف
    print(f"📂 [DOCUMENT]: استلمت ملف '{doc.file_name}' من المستخدم {user_id} (الأكشن: {action})")

    # 2. التحقق من تطابق الأكشن وصيغة الملف (JSON)
    if action == 'awaiting_json_backup' and doc.file_name.endswith('.json'):
        
        # حماية إضافية: التأكد من أن المرسل هو المطور فقط
        if str(user_id) != str(DEVELOPER_ID):
            print(f"⚠️ [WARN]: محاولة استعادة من مستخدم غير مصرح له: {user_id}")
            return

        try:
            # إشعار المطور باستلام الملف
            status_msg = await update.message.reply_text("📥 جاري قراءة الملف وتجهيزه للاستعادة...")

            # تحميل الملف وتحويله إلى Bytearray لتخزينه مؤقتاً في الرام
            file_obj = await context.bot.get_file(doc.file_id)
            file_content = await file_obj.download_as_bytearray()

            # --- [ الخطوة الجوهرية ] ---
            # تخزين محتوى الملف في user_data لاستخدامه عند ضغط زر "تأكيد"
            context.user_data['pending_restore_content'] = file_content
            
            # بناء لوحة أزرار التأكيد
            keyboard = [
                [InlineKeyboardButton("✅ تأكيد الاستعادة الآن", callback_data="confirm_restore")],
                [InlineKeyboardButton("❌ إلغاء العملية", callback_data="cancel_restore")]
            ]
            
            await status_msg.edit_text(
                f"📦 <b>تم استلام النسخة الاحتياطية بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📄 اسم الملف: <code>{doc.file_name}</code>\n"
                f"📊 الحجم: {round(doc.file_size / 1024, 2)} KB\n\n"
                f"⚠️ <b>تنبيه:</b> عند الضغط على تأكيد، سيتم استبدال بيانات الـ 37 ورقة في السحاب والرام فوراً.\n\n"
                f"هل تود المتابعة؟",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            # لا نصفر الأكشن هنا لأننا ننتظر ضغطة الزر
            print(f"✅ [SUCCESS]: الملف جاهز بانتظار التأكيد اليدوي من المطور.")

        except Exception as e:
            print(f"❌ [ERROR]: خطأ أثناء تحميل الملف: {e}")
            await update.message.reply_text(f"❌ حدث خطأ فني أثناء قراءة الملف: {e}")
            context.user_data['action'] = None




# ==========================================
# 🚀 رفع الموديول (بدون تعديل المنطق)
# ==========================================
async def handle_module_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # تم حذف السطر المكرر والاكتفاء بهذا التحقق المنظم
    if update.effective_user.id != DEVELOPER_ID:
        return

    doc = update.message.document

    decision = await safe_process_file_decision(
        update, context, doc.file_name, await doc.get_file()
    )

    if decision != "PLUGIN":
        return

    if doc.file_name.endswith(".py"):
        file = await doc.get_file()
        file_path = f"./{doc.file_name}"
        await file.download_to_drive(file_path)

        context.user_data["uploaded_module_file"] = doc.file_name

        await update.message.reply_text(
            f"✅ تم رفع الملف <code>{doc.file_name}</code>\n"
            f"أرسل الاسم الوصفي:",
            parse_mode="HTML"
        )
        return WAITING_FOR_MODULE_NAME


# ==========================================
# 🧩 حفظ اسم الموديول (كما هو)
# ==========================================
async def finalize_module_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != DEVELOPER_ID: return

    module_display_name = update.message.text.strip()
    file_name = context.user_data.get("uploaded_module_file")
    key_name = f"desc_{file_name}"

    status_msg = "تمت إضافته كنوع جديد"

    try:
        from sheets import meta_sheet
        from datetime import datetime

        if meta_sheet:
            cell = None
            try:
                cell = meta_sheet.find(key_name)
            except:
                pass

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if cell:
                meta_sheet.update_cell(cell.row, 2, module_display_name)
                meta_sheet.update_cell(cell.row, 3, now_str)
                status_msg = "تم تحديث بيانات الموديول الحالي"
            else:
                meta_sheet.append_row([key_name, module_display_name, now_str])

    except Exception as e:
        print(f"⚠️ خطأ: {e}")
        status_msg = "تم الرفع مع خطأ قاعدة البيانات"

    await update.message.reply_text(
        f"🚀 {status_msg}\n📛 {module_display_name}",
        parse_mode="HTML"
    )

    context.user_data.clear()
    os.execv(sys.executable, ['python'] + sys.argv)


# ==========================================
# 🔘 تأكيد ملفات البيئة (كما هو)
# ==========================================
async def confirm_env_update_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != DEVELOPER_ID: return

    if query.data == "confirm_env_update":
        await query.message.edit_text("📤 إرسال نسخة احتياطية...")


        await download_bot_cache(update, context)

        await query.message.reply_text("🔄 جاري تحديث المصنع...")

        try:
            env_files = context.user_data.get("env_temp_files", {})
            for f_name, t_path in env_files.items():
                if os.path.exists(t_path):
                    os.replace(t_path, f"./{f_name}")
        except: pass

        if os.path.exists("./factory_cache.json"):
            os.remove("./factory_cache.json")

        context.user_data.clear()
        os.execv(sys.executable, ['python'] + sys.argv)

#-----------------------



# إعداد الـ ConversationHandler لرفع الموديولات للمطور
admin_module_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Document.FileExtension("py"), handle_module_upload)],
    states={
        WAITING_FOR_MODULE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, finalize_module_name)],
    },
    fallbacks=[CommandHandler('cancel', cancel)],
)

# --------------------------------------------------------------------------


# --- دالة تحميل مرآة الكاش (توضع في main.py) ---
async def download_bot_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استدعاء محرك تحميل ملفات المرآة وإرسالها حسب صلاحية المستخدم"""
    user_id = update.effective_user.id
    # (تمت إزالة شرط التحقق الصارم من ADMIN_ID للسماح للعملاء بتحميل بياناتهم)
    
    query = update.callback_query
    if query: await query.answer()

    from cache_manager import create_backup_to_telegram
    
    # التعديل هنا: نمرر user_id (المستخدم الحالي) بدلاً من ADMIN_ID الثابت
    await create_backup_to_telegram(context.bot, user_id)

#رفع النسخة 
async def start_restore_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المرحلة الأولى: استقبال الملف وعرض التحذير"""
    if not update.effective_user: 
        return
    
    user_id = update.effective_user.id
    
    # تصحيح الحماية: منع العمليات لغير المطور (بناءً على المنطق الموجود في كودك)
    if user_id != DEVELOPER_ID:
        return
        
    doc = update.message.document
    
    if not doc.file_name.endswith('.json'):
        await update.message.reply_text("❌ عذراً، يجب أن يكون الملف بصيغة .json المشفرة.")
        return

    # حفظ محتوى الملف مؤقتاً في ذاكرة المستخدم
    file = await context.bot.get_file(doc.file_id)
    
    # تحميل الملف كبايتات مباشرة ومعالجته (كما ورد في الكود الأصلي تماماً)
    file_bytes = await file.download_as_bytearray()
    content = file_bytes.decode('utf-8')

    # استخدام المفتاح الأصلي لبيانات المستخدم
    context.user_data['pending_restore_content'] = content

    keyboard = [
        [
            InlineKeyboardButton("✅ نعم، أوافق", callback_data="confirm_restore"),
            InlineKeyboardButton("❌ لا، إلغاء", callback_data="cancel_restore")
        ]
    ]
    
    warn_text = (
        "⚠️ <b>تحذير هام جداً!</b>\n"
        "━━━━━━━━━━━━━━\n"
        "لقد قمت برفع نسخة احتياطية. إذا وافقت:\n"
        "1. سيتم استبدال البيانات الحالية ببيانات النسخة.\n"
        "2. قد تفقد أي تحديثات تمت بعد تاريخ هذه النسخة.\n\n"
        "<b>هل أنت متأكد من رغبتك في التنفيذ؟</b>"
    )
    await update.message.reply_text(warn_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


# --------------------------------------------------------------------------
# --- [ أوامر إدارة ملفات الإداريين ] ---

async def export_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تصدير قائمة الأدمنية الحالية إلى ملف JSON"""
    if update.effective_user.id != DEVELOPER_ID:
        return

    # جمع كافة الأدمنية من القائمة الثابتة والمتغيرة
    from datetime import datetime
    data = {
        "admin_ids": list(ALL_ADMINS),
        "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    
    file_path = "admins_backup.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    await update.message.reply_document(
        document=open(file_path, "rb"),
        filename=file_path,
        caption="✅ تم تصدير قائمة الإداريين بنجاح."
    )

async def import_admins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المرحلة الأولى: طلب ملف الإداريين"""
    if update.effective_user.id != DEVELOPER_ID:
        return
    await update.message.reply_text("📥 من فضلك أرسل ملف `admins_backup.json` الآن لترقية الجميع.")
    context.user_data["admin_action"] = "waiting_admin_file"

async def process_admin_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المرحلة الثانية: معالجة الملف وترقية الجميع"""
    if context.user_data.get("admin_action") != "waiting_admin_file":
        return

    doc = update.message.document
    if not doc.file_name.endswith('.json'):
        await update.message.reply_text("❌ الملف غير مدعوم.")
        return

    file = await doc.get_file()
    content = await file.download_as_bytearray()
    data = json.loads(content.decode('utf-8'))
    
    new_admins = data.get("admin_ids", [])
    
    # تحديث القائمة في الذاكرة الحالية (Hot Reload)
    global ALL_ADMINS
    ALL_ADMINS = list(set(ALL_ADMINS + new_admins))
    
    await update.message.reply_text(f"✅ تمت المهمة! تم ترقية {len(new_admins)} مستخدم إلى إداريين بنجاح.")
    context.user_data["admin_action"] = None

# --------------------------------------------------------------------------

# دالة مساعدة جديدة لضمان المزامنة الصارمة بدون حظر API
async def pull_sync_task_wrapper():
    """تغليف عملية المزامنة لضمان التتابع الزمني وحماية الكوتا"""
    from sheets import get_all_active_tokens, smart_sync_check
    try:
        tokens = get_all_active_tokens()
        for token in tokens:
            # تنفيذ المزامنة لكل بوت على حدة
            await smart_sync_check(token)
            # فاصل زمني بسيط (2 ثانية) بين كل بوت لضمان عدم تجاوز حصة جوجل (Avoid 429)
            await asyncio.sleep(2.5) 
    except Exception as e:
        print(f"⚠️ فشل في مهمة pull_sync المجدولة: {e}")

# دالة المزامنة مع جوجل شيت (المصححة)
def start_scheduler():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from sheets import sync_ad_campaign_results
    from cache_manager import db_manager
    scheduler = AsyncIOScheduler(timezone="Asia/Riyadh")

    # 1. المزامنة الصامتة (Pull): تم استبدال الـ lambda بالدالة الجديدة pull_sync_task_wrapper
    scheduler.add_job(  
        pull_sync_task_wrapper,   
        'interval',   
        minutes=15,  
        id='pull_sync'  
    )  

    # 2. مزامنة النتائج: تحديث إحصائيات الحملات الإعلانية كل ساعة  
    scheduler.add_job(  
        sync_ad_campaign_results,   
        'interval',   
        hours=1,  
        id='ads_sync'  
    )  

    # 3. الرفع الشامل (Push): رفع كل العمليات المعلقة لجوجل شيت فجراً  
    scheduler.add_job(  
        db_manager.push_to_google_sheets,   
        'cron',   
        hour=3,   
        minute=30,  
        id='daily_push_sync'  
    )  

    scheduler.start()  
    print("⏰ تم تشغيل المجدل الزمني بنجاح: حماية الكوتا مفعلة (فاصل 2.5 ثانية بين البوتات).")

    
    
   

# --------------------------------------------------------------------------
# --- [ 2. ثانياً: تعريف الـ Handler الخاص بالإذاعة (خارج الدالة الرئيسية) ] ---
broadcast_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(start_global_broadcast, pattern="^broadcast_owners$")],
    states={
        WAITING_BROADCAST_CONTENT: [
            MessageHandler(filters.ALL & ~filters.COMMAND, process_global_broadcast)
        ],
    },
    fallbacks=[CallbackQueryHandler(cancel_broadcast, pattern="^cancel_broadcast$")],
)

# --------------------------------------------------------------------------
async def delete_database_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """حذف قاعدة البيانات المحلية وإعادة تشغيل المصنع لبنائها بالهيكل الجديد"""
    user_id = update.effective_user.id
    
    # حماية: المطور فقط من يمكنه تنفيذ هذا الأمر
    if user_id != DEVELOPER_ID:
        await update.message.reply_text("🚫 عذراً، هذا الأمر مخصص للمطور الأساسي فقط.")
        return

    try:
        from cache_manager import db_manager, DB_PATH
        import os
        import sys

        await update.message.reply_text("⏳ جاري إغلاق الاتصال وتدمير القاعدة القديمة...")

        # 1. إغلاق الاتصال لتجنب خطأ "ملف قيد الاستخدام"
        if db_manager and db_manager.conn:
            db_manager.conn.close()

        # 2. حذف الملف فيزيائياً
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            await update.message.reply_text("✅ تم حذف ملف database.db بنجاح.")
        else:
            await update.message.reply_text("⚠️ الملف غير موجود بالفعل، سيتم بناء واحد جديد.")

        # 3. حذف ملف القفل (Lock) لضمان عدم تعليق المصنع
        lock_file = "/app/cache_data/bot_factory.lock"
        if os.path.exists(lock_file):
            os.remove(lock_file)

        await update.message.reply_text("🔄 جاري إعادة تشغيل السيرفر الآن لبناء الهيكل العربي... يرجى الانتظار دقيقة.")
        
        # 4. إعادة تشغيل السيرفر بالكامل (Hot Restart)
        os.execv(sys.executable, ['python'] + sys.argv)

    except Exception as e:
        await update.message.reply_text(f"❌ فشل الحذف: {str(e)}")


# --- [ دالة معالجة الأزرار اليدوية ] ---
# --- [ دالة معالجة الأزرار اليدوية المحدثة بنظام التأكيد ] ---
async def manual_init_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    from sheets import get_sheets_structure
    
    structure = get_sheets_structure()

    if data == "pull_google_data":
        await query.answer("جاري الاتصال بجوجل...")
        print("⏳ [MANUAL LOG]: تم طلب سحب البيانات من جوجل شيت.")
        await query.edit_message_text("⏳ جاري سحب البيانات الكاملة من جوجل (قد يستغرق وقتاً بسبب الـ Quota)...")
        db_manager.sync_schema(structure)
        await query.message.reply_text("✅ تمت المزامنة بنجاح من جوجل شيت.")
        
    elif data == "init_tables_only":
        await query.answer("جاري تهيئة الهيكل...")
        from sheets import connect_to_google
        ss = connect_to_google() 
        print("⚙️ [MANUAL LOG]: تم طلب تهيئة الجداول المحلية فقط.")
        await query.edit_message_text("⚙️ جاري بناء الهياكل المحلية (SQLite) فقط...")
        db_manager.sync_schema(spreadsheet=ss)
        await query.message.reply_text("✅ تم إنشاء الجداول محلياً بنجاح.")



    elif data == "restore_last_backup":
        await query.answer()
        print("❓ [MANUAL LOG]: طلب استعادة نسخة - إظهار رسالة التأكيد.")
        # إرسال رسالة التأكيد بوجود زرين (نعم / لا)
        confirm_keyboard = [
            [
                InlineKeyboardButton("✅ نعم، ابدأ الاستعادة", callback_data="confirm_restore_yes"),
                InlineKeyboardButton("❌ لا، إلغاء", callback_data="confirm_restore_no")
            ]
        ]
        await query.edit_message_text(
            "⚠️ **تأكيد الاستعادة:**\nهل أنت متأكد أنك تريد استعادة آخر نسخة احتياطية؟\nسيتم استبدال القاعدة الحالية تماماً.",
            reply_markup=InlineKeyboardMarkup(confirm_keyboard),
            parse_mode="Markdown"
        )


    elif data in ["confirm_restore_yes", "restore_from_channel"]:
        await query.answer("بدء الاستعادة...")
        print("📥 [MANUAL LOG]: تم تأكيد الاستعادة. جاري جلب الملف...")
        await query.edit_message_text("⏳ **بدء عملية الاستعادة...**\nجاري جلب الملف من القناة.")
        
        # 1. تنفيذ الاستعادة (تحديث ملف database.db المحلي)
        success = await db_manager.restore_from_telegram()
        
        if success:
            print("✅ [MANUAL LOG]: اكتملت عملية الاستعادة بنجاح. جاري تنشيط البيانات...")
            
            try:
                # 2. التصحيح المنطقي: مسح الكاش القديم فقط
                # سيجبر البوت على قراءة البيانات الجديدة من ملف الـ DB الذي استعدناه للتو
                FACTORY_GLOBAL_CACHE.clear() 
                
                # 3. تحديث قائمة البوتات في الرام من الملف المحلي الجديد
                # ملاحظة: لا نسحب من جوجل هنا لكي لا نمسح ما استعدناه
                from cache_manager import db_manager
                all_bots = await db_manager.get_all_bots()
                FACTORY_GLOBAL_CACHE["all_bots"] = all_bots
                
                final_msg = "✅ **اكتملت استعادة البيانات بنجاح!**\nتم جلب النسخة من تليجرام وتنشيطها في ذاكرة البوت فوراً."
            except Exception as cache_err:
                print(f"⚠️ [RESTORE LOG]: فشل تنشيط الرام: {cache_err}")
                final_msg = "✅ **اكتملت الاستعادة بنجاح!**\nتم تحديث الملف، يرجى إعادة تشغيل المصنع لضمان تحديث كافة القوائم."

            await query.edit_message_text(final_msg, parse_mode="Markdown")
        else:
            await query.edit_message_text("❌ **فشلت عملية الاستعادة!**\nتحقق من وجود نسخة مثبتة في القناة.")

# --- [ القسم 3: المحرك الرئيسي (نهاية الملف) ] ---
async def main_factory_launcher():
    global app
    try:
        from datetime import datetime 
        import os 
        import asyncio
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup 
        from cache_manager import DB_PATH, db_manager, DEVELOPER_ID, BACKUP_CHANNEL_ID
        
        # استيراد الأدوات من startbot لضمان الاتصال بالمحرك الموحد
        from startbot import (
            start, start_create_bot, select_type, receive_token, 
            finalize_bot, run_dynamic_bot, start_all_sub_bots,
            create_bot_conv, ACTIVE_RUNTIME_BOTS
        )

        TOKEN = os.getenv("BOT_TOKEN") 
        if not TOKEN:
            print("❌ [ERROR]: لم يتم العثور على BOT_TOKEN في متغيرات البيئة!")
            return

        print(f"--- [ {datetime.now().strftime('%H:%M:%S')} ] استهلال محرك المصنع المطور ---")
        
        # 1. بروتوكول الأمان
        print("🔍 [LOG]: المحرك يعتمد الآن على نظام الـ Lock الخارجي المستقر.")

        # 2. فحص حالة قاعدة البيانات 
        if os.path.exists(DB_PATH):
            size = os.path.getsize(DB_PATH)
            print(f"📦 [LOG]: ملف القاعدة جاهز للعمل. المسار: {DB_PATH} | الحجم: {size} بايت")
            print("🛡️ [LOG]: تم تجاوز التأمين التلقائي المكرر (البيانات مؤمنة بالفعل).")
        else:
            print("ℹ️ [LOG]: بيئة تشغيل نظيفة، بانتظار تهيئة البيانات.")

        # 3. بناء المحرك وتسجيل كافة المعالجات بالكامل
        print("🔧 [LOG]: جاري بناء محرك البوت الرئيسي وتسجيل المعالجات...")
        from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters
        
        app = ApplicationBuilder().token(TOKEN).build()

        # [A] ربط معالجات المستندات (المحرك الجديد) - أولوية قصوى
        app.add_handler(MessageHandler(filters.Document.MimeType("application/json"), handle_document), group=-1)

        # [B] المعالجات الأساسية والمحادثات (بدون أي حذف)
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("Delete_database", delete_database_handler))     
        app.add_handler(create_bot_conv) 
        app.add_handler(admin_module_conv) 
        app.add_handler(broadcast_handler)

        # [C] معالجات الـ CallbackQuery (لوحة التحكم والإدارة)
        app.add_handler(CallbackQueryHandler(owner_dashboard, pattern="^open_admin_dashboard$"))
        app.add_handler(CallbackQueryHandler(show_admins_dashboard, pattern="^admin_section$"))
        app.add_handler(CallbackQueryHandler(handle_admin_management, pattern="^(remove_admin_|refresh_admins)"))
        app.add_handler(CallbackQueryHandler(show_admins_for_delete, pattern="^show_admins_for_delete$"))        
        
        app.add_handler(CallbackQueryHandler(
            button_callback, 
            pattern=r"^(stats_all|run_setup_db_now|broadcast_owners|restart_factory|download_cache_files|reboot_system|run_push_sync_manual|run_pull_sync_manual|confirm_hard_reset|execute_hard_reset|start_sync_shet|start_restore_request|back_to_main|toggle_maintenance|confirm_restore|backup_subs|manage_coaches|confirm_restorebotvip|cancel_restore|dev_panel|promote_user_.*|reject_user_.*|manual_add_admin|backup_to_channel|restore_from_channel|manage_subscriptions|bots_page_.*|sub_view_.*|exec_sub_.*|extend_sub_.*)$"
        ))        
        
        # [D] معالجات الإقلاع اليدوي والاستعادة
        app.add_handler(CallbackQueryHandler(manual_init_handler, pattern="^(pull_google_data|restore_last_backup|init_tables_only|confirm_restore_yes|confirm_restore_no)$"))

        app.add_handler(CommandHandler("admin_export", export_admins))
        app.add_handler(CommandHandler("import_admin", import_admins_handler))
        
        # [E] معالجات الرسائل والمستندات الإضافية
        app.add_handler(MessageHandler(filters.Document.MimeType("application/json"), process_admin_file))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document), group=-1)
        
        # معالج النصوص العامة
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))

        # تشغيل محرك المصنع
        await app.initialize()
        await app.start() 
        await app.updater.start_polling(drop_pending_updates=True)
        
        print("🚀 [LOG]: البوت الرئيسي يعمل الآن بكفاءة وبانتظار التعليمات.")
        
        # --- [ تصحيح: إحياء البوتات الفرعية تلقائياً باستخدام الدالة المحدثة ] ---
        try:
            # استدعاء الدالة السيادية الموحدة لضمان التشغيل مع نظام الـ Queue
            asyncio.create_task(start_all_sub_bots())
            print("✅ [LOG]: تم إرسال أمر إحياء كافة البوتات الفرعية للمحرك.")
        except Exception as startup_err:
            print(f"⚠️ [LOG]: فشل استدعاء محرك إحياء البوتات: {startup_err}")

        # --- [ الخطوة 4: واجهة التحكم السيادية ] ---
        keyboard = [
            [InlineKeyboardButton("📥 سحب البيانات من جوجل شيت", callback_data="pull_google_data")],
            [InlineKeyboardButton("🔄 استعادة آخر نسخة احتياطية", callback_data="restore_last_backup")],
            [InlineKeyboardButton("⚙️ تهيئة الجداول (محلي فقط)", callback_data="init_tables_only")],
            [InlineKeyboardButton("⏳ بدء المزامنة اليدوية", callback_data="start_manual_sync")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        success_msg = (
            "🔔 **إشعار السيادة والجاهزية القصوى**\n\n"
            "🛡️ **تم بحمد الله استعادة الحصن الحصين؛** النسخة الاحتياطية الأخيرة المودعة في قناة المصنع أصبحت الآن نبضاً للمحرك المحلي.\n\n"
            "🎊 **تمت عملية الإقلاع بنجاح باهر!**\n"
            "📊 حالة النظام: `مستقر وجاهز للعمل` ✅\n"
            "🛑 المزامنة التلقائية: `بانتظار قرارك السيادي`\n\n"
            "🚀 **المصنع الآن في حالة انطلاق.. لا شيء يقف أمامنا!**\n"
            "✨ _البيانات آمنة، والتحكم المطلق بين يديك الآن._"
        )
        
        try:
            # 1. إرسال للمطور في الخاص
            await app.bot.send_message(chat_id=DEVELOPER_ID, text=success_msg, reply_markup=reply_markup, parse_mode="Markdown")
            
            # 2. إرسال للقناة الرسمية
            await app.bot.send_message(chat_id=BACKUP_CHANNEL_ID, text=f"🚀 **إشعار إقلاع جديد:**\n{success_msg}", reply_markup=reply_markup, parse_mode="Markdown")
            print("📨 [LOG]: تم إرسال رسالة التحكم اليدوي إلى القناة والخاص بنجاح.")
        except Exception as msg_err:
            print(f"⚠️ [LOG]: فشل إرسال رسائل الإقلاع (تحقق من وجود البوت في القناة): {msg_err}")
        
        # الحفاظ على الجلسة حية
        from telegram.ext import Application
        await asyncio.Event().wait()

            await asyncio.sleep(3600)

    except Exception as e:
        print(f"🔴 [LOG - CRITICAL]: خطأ حرج في إقلاع المصنع: {e}")


# --- [ تعديل كتلة التشغيل الخاصة بك ] ---
if __name__ == "__main__":
    import asyncio
    import logging
    import os
    from telegram import Bot
    from cache_manager import db_manager, DB_PATH

    # 1. دالة القتل الإجباري والصارم المدمجة
    async def force_kill_old_sessions(token: str):
        """تنهي أي اتصال قديم وتطهر الجلسة في سيرفرات تليجرام لفتح الطريق للنسخة الجديدة"""
        print("⚔️ [SLAUGHTER]: بدء عملية التطهير العرقي للنسخ القديمة...")
        try:
            # تم تصحيح المسافات البادئة (Indentation) هنا لتعمل داخل بلوك try
            async with Bot(token=token) as temp_bot:
                print("🧨 [SLAUGHTER]: تدمير الـ Webhook ومسح كافة التحديثات المعلقة...")
                await temp_bot.delete_webhook(drop_pending_updates=True)
                print("🗡️ [SLAUGHTER]: إغلاق الجلسات المفتوحة في سيرفرات تليجرام...")
                await temp_bot.close()
                await asyncio.sleep(5)
                print("✅ [SLAUGHTER]: تمت إبادة الجلسات القديمة بنجاح.")
        except Exception as e:
            print(f"⚠️ [SLAUGHTER]: تنبيه أثناء التطهير (قد يكون الجلسة مغلقة بالفعل): {e}")
        finally:
            # تم الحفاظ على بنية التنظيف النهائي
            try: await temp_bot.shutdown()
            except: pass

    # 2. المشغل الاستراتيجي المطور (الاحتيال على حذف الاستضافة)
    async def final_launcher():
        """المشغل النهائي: استعادة (عند الحذف) -> تأمين (إذا وجد) -> تطهير -> إطلاق"""
        try:
            token = os.getenv("BOT_TOKEN")
            if not token:
                print("🔴 خطأ حرج: BOT_TOKEN غير موجود!")
                return

            # --- [ مرحلة فحص سلامة البيانات والاستعادة الآلية ] ---
            db_exists = os.path.exists(DB_PATH)
            db_size = os.path.getsize(DB_PATH) if db_exists else 0

            if db_size == 0:
                # الحالة الأولى: الملف مفقود أو فارغ (بسبب تحديث الاستضافة)
                print("🚨 [CRITICAL]: تم اكتشاف مسح البيانات! جاري الاستعادة التلقائية من التليجرام...")
                
                # دالة الاستعادة التي تبحث عن آخر ملف مثبت (Pinned) في قناتك
                restore_success = await db_manager.restore_from_telegram()
                
                if restore_success:
                    print("✅ [RESTORE]: تمت استعادة قاعدة البيانات بنجاح قبل الإقلاع.")
                    print("⏳ [WAIT]: انتظار 10 ثوانٍ لتهدئة الاتصال بطلب من تليجرام...")
                    await asyncio.sleep(10) 
                else:
                    print("⚠️ [RESTORE]: فشلت الاستعادة (قد لا توجد نسخة مثبتة). سيتم بدء قاعدة جديدة.")

            # --- [ الخطوة 2: القتل الإجباري ] ---
            await force_kill_old_sessions(token)

            # --- [ الخطوة 3: الإقلاع الفعلي للمصنع ] ---
            print("🚀 [LAUNCH]: انطلاق المحرك الرئيسي للمصنع الآن...")
            await main_factory_launcher()

        except Exception as e:
            print(f"🔴 فشل تسلسل الإقلاع الحرج: {e}")

    # 3. تشغيل الحلقة (Event Loop) الوحيدة للنظام
    try:
        asyncio.run(final_launcher())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 تم إيقاف المصنع يدوياً.")
    except Exception as e:
        print(f"🔴 انهيار المحرك الرئيسي الحرج: {e}")
