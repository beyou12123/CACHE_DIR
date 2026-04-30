# --- [ 1. المكتبات القياسية ونظام التشغيل ] ---
import logging
import re
import io
import os
import g4f
import pandas as pd
import openpyxl
import uuid
import json
import secrets
import importlib
import importlib.util
import set_org
from datetime import datetime
from telegram.ext import CallbackQueryHandler, MessageHandler, filters
from google import genai
from google.genai import types
from contact_message import handle_contact_message

# جلب المفتاح من Variables المنصة
api_key = os.getenv("GEMINI_API_KEY")

if api_key:
    # الطريقة الصحيحة للمكتبة الجديدة google-genai
    client = genai.Client(api_key=api_key)
    print("✅ تم سحب المفتاح بنجاح من إعدادات المنصة")
 

 # ضروري لعمليات استيراد وتصدير الإكسل
   # محرك معالجة ملفات xlsx
# --- [ 3. مكتبات تليجرام بوت (النسخة الحديثة) ] ---
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    Bot, 
    ChatMember
)
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    ChatMemberHandler, 
    CommandHandler,      
    CallbackQueryHandler, 
    MessageHandler, 
    filters,
    JobQueue
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
    BASE_DIR, 
    CACHE_DIR, 
    BOT_PROCESS_LOCK_FILE,
    CHECK_INTERVAL, 
    LAST_CHECK_TIME, 
    
    # --- الدوال الأساسية وإدارة النظام ---
    acquire_process_lock,
    release_process_lock,
    is_bot_running,
    mark_bot_running,
    mark_bot_stopped,
    
    
    # --- دوال التشغيل والمحركات الفرعية ---
    start_all_sub_bots,
    DB_PATH,
    run_dynamic_bot,
    
    # --- معالجات الأوامر والمحادثات (Handlers) ---
    
    start_create_bot,
    select_type,
    receive_token,
    cancel
)
from sheets import (
    get_bot_config, 
    add_log_entry, 
    get_bot_users_count, 
    get_bot_blocks_count,
    save_user,
    get_all_categories,
    add_new_category,
    delete_category_by_id,
    update_category_name,
    add_new_course,
    get_courses_by_category,
    delete_course_by_id,
    get_ai_setup,
    link_user_to_inviter,
    check_user_permission,
    ss,
    get_user_referral_stats,
    get_bot_setting,
    redeem_points_for_course,
    courses_sheet,
    get_all_coaches,
    delete_coach_from_sheet,
    add_new_coach_advanced,
    smart_sync_check,
    get_bot_data_from_cache,
    delete_question_from_bank,
    add_question_to_bank,
    create_auto_quiz,
    toggle_quiz_visibility,
    ensure_permission_row_exists,
    get_employee_permissions,
    save_group_to_db,
    delete_group_by_id,
    update_group_field,
    toggle_scope_id,
    get_all_personnel_list,
    toggle_employee_permission,
    get_newly_activated_students,
    update_global_version,
    find_user_by_username,
    add_new_branch_db,
    update_content_setting,
    client,
    save_ai_setup,
    add_new_employee_advanced,
    process_referral_reward_on_purchase,
    seed_default_settings,
    update_withdrawal_status,
    create_withdrawal_request,
    get_system_time, 
    get_courses_knowledge_base, 
    delete_branch_db
)
# --- [ استيرادات الموديلات الأخرى ] ---

from educational_manager import (
    list_all_discounts_ui,
    process_dsc_ask_desc,
    process_dsc_check,
    add_discount_start,
    manage_control_ui,
    validate_dsc_max,
    validate_dsc_expiry,
    validate_dsc_value,
    validate_dsc_desc,
    show_lectures_logic,
    view_discount_details_ui,
    show_discount_codes_logic,
    manage_library_selector,
    manage_groups_main,
    manage_categories_main,
    quiz_create_start_ui,
    start_add_question_flow, 
    process_q_flow,
    quiz_gen_select_groups_ui,
    q_bank_manager_ui,
    browse_q_bank_ui,
    view_question_details_ui,
    start_add_question_ui,
    quiz_activation_start,
    quiz_activation_groups,
    employee_quiz_view,
    quiz_options_ui,
    start_add_group,
    confirm_group_save,
    group_options_ui,
    confirm_delete_group_ui,
    process_grp_name,
    process_grp_days,
    process_grp_time
)
# --- [ محرك الدورات ] ---

from course_engine import (
    # --- إدارة الإعلانات والحملات ---
    ad_create_start, 
    ad_report_view, 
    manage_ads_main_ui,
    process_ad_campaign_flow,

    # --- إعدادات النظام والعملة ---
    show_system_setup_information,
    set_currency_unit_flow,
    save_currency_unit_logic,
    set_default_payment_flow,
    save_payment_info_logic,

    # --- نظام التسويق بالعمولة والنقاط ---
    set_marketers_commission_flow,
    save_marketers_commission_logic,
    set_ref_points_join_flow,
    save_ref_points_join_logic,
    set_ref_points_purchase_flow,
    save_ref_points_purchase_logic,
    set_min_payout_flow,
    save_min_payout_logic,

    # --- إدارة الواجبات والدرجات ---
    set_homework_grade_flow,
    save_homework_grade_logic,
    set_min_passing_grade_flow,
    save_min_passing_grade_logic,
    set_max_passing_grade_flow,
    save_max_passing_grade_logic,

    # --- عرض المحتوى ولوحة الشرف ---
    show_honors_main_menu,
    show_course_content_ui
)
# --- [ إدارة الكاش والبيانات ] ---
from cache_manager import (
    FACTORY_GLOBAL_CACHE,
    save_cache_to_disk, 
    fetch_full_factory_data,
    export_bot_data_to_excel,
    db_manager,
    update_global_version,
    export_bot_data_to_excel,
    fetch_full_factory_data,
    check_excel_permission_from_cache
)
from contact_callback import contact_callback_handler
from contact_message import handle_contact_message

from ContentManager import (
    get_coach_panel,
    get_student_menu,
    get_admin_panel,
    get_employee_panel,
    get_tech_settings,
    content_management_handler,
    get_main_config,
    smart_navigate,
    auto_reply_engine
)

from set_org import (
    show_org_name_panel, 
    show_ai_prompt_panel, 
    show_payment_panel,
    trigger_edit_ai,
    trigger_edit_payment,
    org_input_handler,
    trigger_add_org_handler 
)

# --- [ ذاكرة المحادثات المؤقتة للطلاب ] ---
user_messages = {} 




# --- [ القوائم الرئيسية للمنصة - أزرار واجهة المستخدم ] ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# 2. جلب المفتاح من المنصة (Variables) وليس كتابته يدوياً
GEMINI_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_KEY:
    # الطريقة الصحيحة للمكتبة الجديدة
    client = genai.Client(api_key=GEMINI_KEY)
    # ملاحظة: في النسخة الجديدة لا نحتاج لتعريف model كمتغير مستقل هنا 
    # بل نستخدم العميل مباشرة عند الحاجة لتوليد النص
    logging.info("✅ تم تهيئة عميل Gemini الجديد بنجاح")
else:
    client = None
    logging.warning("⚠️ GEMINI_API_KEY غير موجود في إعدادات المنصة")

# النظام الجديد: إنشاء عميل (Client)


# استدعاء الموديل (بشكل محدث)


#لوحة المدرب 


# --- [ المعالجات الأساسية - أمر البداية المطوّر ] ---
# --- [ المعالجات الأساسية - أمر البداية المطوّر ] ---
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start برسائل ترحيبية ذكية ودعم نظام الإحالة والأدوار (مالك، موظف، مدرب، طالب)"""
    

    user = update.effective_user
    bot_token = context.bot.token
    query = update.callback_query
    
    # جلب كافة الإعدادات من قاعدة البيانات
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))
    ai_config = get_ai_setup(bot_token)
    
    # --- [ تصحيح جلب الاسم: التحقق المزدوج من الكاش والقاعدة المحلية ] ---
    if user.id == bot_owner_id:
        # إذا كان الكاش فارغاً، نتحقق من قاعدة البيانات المحلية مباشرة لتجنب التكرار
        if not ai_config or not ai_config.get('اسم_المؤسسة') or str(ai_config.get('اسم_المؤسسة')) in ["0", "None", ""]:
            
            try:
                # البحث المباشر في جدول إعدادات_المحتوى عن العمود رقم 20 (اسم_المؤسسة)
                db_manager.cursor.execute('SELECT "اسم_المؤسسة" FROM "إعدادات_المحتوى" WHERE "bot_id" = ?', (str(bot_token),))
                db_row = db_manager.cursor.fetchone()
                if db_row and db_row[0] not in [None, "0", "", "None"]:
                    # تحديث ai_config محلياً فوراً ليتجاوز الفحص أدناه
                    if not ai_config: ai_config = {}
                    ai_config['اسم_المؤسسة'] = db_row[0]
                    # تحديث الكاش العالمي لضمان عدم تكرار الفحص في المرات القادمة
                    
                    if bot_token not in FACTORY_GLOBAL_CACHE["data"]: FACTORY_GLOBAL_CACHE["data"][bot_token] = {}
                    FACTORY_GLOBAL_CACHE["data"][bot_token]['اسم_المؤسسة'] = db_row[0]
            except Exception as e:
                logging.error(f"⚠️ خطأ في فحص اسم المؤسسة من القاعدة المحلية: {e}")

    # --- [ 1. فحص إعدادات المالك (التهيئة الأولى) ] ---
    if user.id == bot_owner_id:
        if not ai_config or not ai_config.get('اسم_المؤسسة') or str(ai_config.get('اسم_المؤسسة')) in ["0", "None", ""]:
            context.user_data['action'] = 'awaiting_institution_name'
            text = (
                "👋 <b>أهلاً بك يا دكتور!</b>\n\n"
                "قبل البدء، يرجى إرسال <b>اسم المنصة التعليمية</b> الخاصة بك:"
            )
            if query:
                await query.answer()
                await query.edit_message_text(text, parse_mode="HTML")
            else:
                await update.message.reply_text(text, parse_mode="HTML")
            return
 
   # --- [ 1. معالجة روابط انضمام الكوادر (مدرب/موظف) ] ---
    if context.args and context.args[0].startswith("reg_"):
        token = context.args[0].replace("reg_", "")
        
        if token in FACTORY_GLOBAL_CACHE.get("temp_registration_tokens", {}):
            role = FACTORY_GLOBAL_CACHE["temp_registration_tokens"][token]
            del FACTORY_GLOBAL_CACHE["temp_registration_tokens"][token]
            
            context.user_data['reg_role'] = role
            context.user_data['action'] = 'awaiting_reg_full_name'
            
            role_text = "كادرنا التعليمي (مدرب)" if role == "coach" else "كادرنا الإداري (موظف)"
            await update.message.reply_text(
                f"👋 <b>أهلاً بك!</b> نتشرف بانضمامك إلى {role_text}.\n\n"
                f"يرجى إرسال <b>اسمك الثلاثي</b> باللغة العربية للبدء:"
            , parse_mode="HTML")
            return
        else:
            await update.message.reply_text("⚠️ معذرة، هذا الرابط غير صالح أو تم استخدامه مسبقاً.")
            return

    # --- [ معالجة رابط الهدية للمستلم ] ---
    if context.args and context.args[0].startswith("gift_"):
        gift_code = context.args[0].replace("gift_", "")
        
        sheet_coupons = ss.worksheet("الكوبونات")
        coupon = sheet_coupons.find(gift_code, in_column=3)
        
        if coupon:
            coupon_data = sheet_coupons.row_values(coupon.row)
            if coupon_data[7] == "نشط":
                course_id = coupon_data[10].replace("دورة_", "")
                context.user_data['reg_flow'] = {'gift_code': gift_code}
                await course_engine.start_registration_flow(update, context, course_id, payment_method="Gift")
                return
            else:
                await update.message.reply_text("⚠️ معذرة، هذا الرابط تم استخدامه مسبقاً.")
                return

    # --- [ 2. نظام الإحالة المتطور (للطلاب والزوار) ] ---
    inviter_id = None
    if context.args and context.args[0].startswith("ref_"):
        potential_inviter = context.args[0].replace("ref_", "")
        if str(potential_inviter) != str(user.id):
            inviter_id = potential_inviter

    # --- [ 3. تسجيل المستخدم في القاعدة ] ---
    save_user(user.id, user.username, inviter_id, bot_token=context.bot.token)

    # --- [ 3. محرك اختيار الكليشة الذكي ] ---
    hour = datetime.now().hour
    if 5 <= hour < 12:
        msg = config.get("welcome_morning", "صباح العلم والهمة.. أي مهارة سنبني اليوم؟")
    elif 12 <= hour < 17:
        msg = config.get("welcome_noon", "طاب يومك.. الاستمرارية هي سر النجاح، لنكمل التعلم.")
    elif 17 <= hour < 22:
        msg = config.get("welcome_evening", "مساء الفكر المستنير.. حان وقت الحصاد المعرفي.")
    else:
        msg = config.get("welcome_night", "أهلاً بالمثابر.. العظماء يصنعون مستقبلهم في هدوء الليل.")

    # --- [ 4. فرز الرتب وإرسال الواجهة المناسبة ] ---
    try:
        current_owner_id = int(bot_owner_id)
    except:
        current_owner_id = 0

    if user.id == current_owner_id:
        final_text = (
            f"<b>مرحباً بك يا دكتور {user.first_name} في مركز قيادة منصتك</b> 🎓\n\n"
            f"{msg}\n\n"
            f"يمكنك إدارة كافة تفاصيل المنصة من الأزرار أدناه:"
        )
        reply_markup = get_admin_panel()

    elif (check_user_permission(bot_token, user.id, "الصلاحيات") == True) or \
         (check_user_permission(bot_token, user.id, "صلاحية_الأقسام") == True):
        
        employees_data = FACTORY_GLOBAL_CACHE["data"].get("إدارة_الموظفين", [])
        user_row = next((row for row in employees_data if len(row) > 2 and str(row[2]) == str(user.id)), None)
        
        if user_row and len(user_row) >= 42 and str(user_row[41]).strip() == "مدرب":
            final_text = (
                f"<b>مرحباً بك يا كابتن {user.first_name} في غرفتك الأكاديمية</b> 👨‍🏫\n\n"
                f"{msg}\n\n"
                f"يمكنك متابعة طلابك وتصحيح الواجبات من الأزرار أدناه:"
            )
            reply_markup = get_coach_panel()
        else:
            final_text = (
                f"<b>مرحباً بك يا {user.first_name} في لوحة الإدارة التعليمية</b> 💼\n\n"
                f"{msg}\n\n"
                f"لديك صلاحيات الموظفين المعتمدة، يمكنك البدء بالإدارة من الأزرار أدناه:"
            )
            reply_markup = get_employee_panel()
    else:
        final_text = f"<b>{msg}</b>"
        reply_markup = get_student_menu()

    # --- [ 5. تنفيذ الإرسال النهائي ] ---
    try:
        if query:
            await query.answer()
            await query.edit_message_text(final_text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.message.reply_text(final_text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"❌ خطأ في الإرسال النهائي لـ start_handler: {e}")




# --------------------------------------------------------------------------
# دالة توليد لوحة الصلاحيات (التي أرسلتها أنت)
def get_permissions_keyboard(bot_token, employee_id, current_perms):
    perms_map = {
        "📁 الأقسام": "صلاحية_الأقسام", "📚 الدورات": "صلاحية_الدورات",
        "👨‍🏫 المدربين": "صلاحية_المدربين", "👥 الموظفين": "صلاحية_الموظفين",
        "📊 الإحصائيات": "صلاحية_الإحصائيات", "📢 الإذاعة": "صلاحية_الإذاعة",
        "💬 رسائل خاصة": "صلاحية_الرسائل_الخاصة", "🎟 الكوبونات": "صلاحية_الكوبونات",
        "🏷 أكواد الخصم": "صلاحية_أكواد_الخصم"
    }
    keyboard = []
    items = list(perms_map.items())
    for i in range(0, len(items), 2):
        row = []
        for label, col in items[i:i+2]:
            status = current_perms.get(col, "FALSE")
            icon = "☑️" if str(status).upper() == "TRUE" else "✖️"
            row.append(InlineKeyboardButton(f"{label} {icon}", callback_data=f"p_toggle_{employee_id}_{col}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🎯 تحديد الدورات المسموحة", callback_data=f"p_limit_crs_{employee_id}")])
    keyboard.append([InlineKeyboardButton("🔙 عودة لقائمة الموظفين", callback_data="manage_personnel")])

    return InlineKeyboardMarkup(keyboard)

# دالة معالجة التبديل (Toggle) في CallbackQueryHandler
# تذكر إضافتها داخل contact_callback_handler
async def handle_permission_toggle(query, bot_token, employee_id, col_name):
    
    
    # 1. تحديث القيمة في القاعدة
    new_status = toggle_employee_permission(bot_token, employee_id, col_name)
    
    # 2. جلب الصلاحيات المحدثة لإعادة رسم الكيبورد
    updated_perms = get_employee_permissions(bot_token, employee_id)
    
    # 3. تحديث الرسالة فوراً للمالك
    await query.edit_message_reply_markup(
        reply_markup=get_permissions_keyboard(bot_token, employee_id, updated_perms)
    )
 
 
 # دالة توليد أزرار الدورات لاختيارها للموظف
async def show_course_selector(update, context, employee_id):
    
    
    bot_token = context.bot.token
    # جلب الصلاحيات الحالية لمعرفة ما هو "مختار" سابقاً
    perms = get_employee_permissions(bot_token, employee_id)
    allowed_courses = str(perms.get("الدورات_المسموحة", "")).split(",")
    
    # جلب كل دورات البوت
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c['bot_id']) == str(bot_token)]
    
    keyboard = []
    for crs in bot_courses:
        crs_id = str(crs['معرف_الدورة'])
        icon = "☑️" if crs_id in allowed_courses else "✖️"
        # callback يبدأ بـ p_limit لتمييزه
        keyboard.append([InlineKeyboardButton(f"{crs['اسم_الدورة']} {icon}", 
                                             callback_data=f"p_limit_crs_{employee_id}_{crs_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 عودة للصلاحيات", callback_data=f"manage_perms_{employee_id}")])
    
    await update.callback_query.edit_message_text("🎯 اختر الدورات التي يمكن للموظف إدارتها:", 
                                                 reply_markup=InlineKeyboardMarkup(keyboard))
# --------------------------------------------------------------------------
# دالة فحص الطلاب وإرسال الرسائل (النسخة المعدلة والمدمجة)
async def activation_monitor(context: ContextTypes.DEFAULT_TYPE):
    """وظيفة خلفية تراقب تفعيلات الطلاب وترسل تنبيهات فورا"""
    bot_token = context.bot.token
    
    # جلب الطلاب الذين تم تفعيلهم ولم يتم إشعارهم بعد
    new_activations = get_newly_activated_students(bot_token)
    
    for student in new_activations:
        try:
            # 1. إرسال رسالة التهنئة للطالب
            msg = (
                f"🎉 <b>تهانينا يا {student['name']}!</b>\n\n"
                f"تم تفعيل اشتراكك في الدورة بنجاح. ✅\n"
                f"يمكنك الآن الدخول إلى 👤 <b>(ملفي الدراسي)</b> لمشاهدة كافة الدروس والمحتوى المدفوع.\n\n"
                f"نتمنى لك رحلة تعليمية ممتعة! 🚀"
            )
            await context.bot.send_message(chat_id=student['user_id'], text=msg, parse_mode="HTML")
            
            # 2. 🔥 [التعديل الجوهري]: استدعاء نظام الإحالة المتطور للداعي داخل الحلقة
            # نستخدم student['user_id'] الذي يمثل الشخص الذي تم تفعيله الآن
            success, inviter_id, points = process_referral_reward_on_purchase(bot_token, student['user_id'])

            if success and inviter_id:
                try:
                    ref_msg = (
                        f"🎉 <b>بشرى سارة!</b>\n\n"
                        f"أحد الطلاب الذين دعوتهم قام بالتسجيل الفعلي الآن.\n"
                        f"💰 تم إضافة <b>{points} نقطة</b> إلى رصيدك بنجاح!"
                    )
                    await context.bot.send_message(chat_id=inviter_id, text=ref_msg, parse_mode="HTML")
                except:
                    pass # حماية في حال قام الداعي بحظر البوت

            # 3. تحديث القاعدة (العمود 21) لضمان عدم تكرار العملية
            sheet = ss.worksheet("قاعدة_بيانات_الطلاب")
            # نسجل الوقت ونؤكد إتمام الإشعار والمكافأة
            sheet.update_cell(student['row'], 21, f"تم الإشعار والمكافأة: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            
        except Exception as e:
            print(f"⚠️ فشل إرسال إشعار أو مكافأة للطالب {student['user_id']}: {e}")

# --------------------------------------------------------------------------
# ==========================================================================
# --------------------------------------------------------------------------


# --- [ محرك التشغيل المتوافق مع المصنع ] ---
async def run_bot(token, owner_id):
    """هذه الدالة هي التي يستدعيها ملف main.py لتشغيل البوت ديناميكياً"""
    # تصحيح: تحويل owner_id إلى int لضمان عمل الفلتر filters.Chat(owner_id)
    owner_id = int(owner_id) 
    
    application = ApplicationBuilder().token(token).build()
    
    # 1. إضافة المعالجات (Handlers)
    application.add_handler(CommandHandler("start", start_handler))

    # --- [أولاً: أزرار إعدادات المؤسسة والهوية المضافة حديثاً] ---
    application.add_handler(CallbackQueryHandler(show_org_name_panel, pattern="^set_org_name$"))
    
    application.add_handler(CallbackQueryHandler(trigger_add_org_handler, pattern="^org_add$"))
    application.add_handler(CallbackQueryHandler(show_ai_prompt_panel, pattern="^set_ai_prompt$"))
    application.add_handler(CallbackQueryHandler(_guard_trigger_edit_ai, pattern="^trigger_edit_ai$"))
    application.add_handler(CallbackQueryHandler(show_payment_panel, pattern="^set_payment$"))
    application.add_handler(CallbackQueryHandler(trigger_edit_payment, pattern="^trigger_edit_payment$"))

    # ==================================================================
    # 🛡️ [إضافة ذكية بدون تعديل]: Guard Handler لمنع تعارض add_
    # تم تغيير الترتيب ليكون قبل الـ Regex العام لضمان الفاعلية
    # ==================================================================
    async def _guard_trigger_add_org(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if query:
            await query.answer()
            # إعادة توجيه صريحة للدالة الأصلية
            return await trigger_add_org_handler(update, context)

    # وضع الـ Guard في موضع الأولوية
    application.add_handler(
        CallbackQueryHandler(_guard_trigger_add_org, pattern="^org_add$")
    )
    # ==================================================================

    # --- [ثانياً: المعالجات الإدارية العامة] ---
    
    # [ContentManager]: معالج إدارة المحتوى (دروس، مكتبة، أقسام)
    application.add_handler(CallbackQueryHandler(content_management_handler, pattern="^(view_|manage_|add_|edit_|del_|back_to_edu_).*$"))
    
    # [System/Stats]: الوظائف الإحصائية والنسخ الاحتياطي V7.2
    application.add_handler(CallbackQueryHandler(button_callback, pattern="^(stats|refresh_cache|export_data_json|backup_to_channel|restore_from_channel|download_cache_files|tech_settings)$"))

    # [ContactHandler]: أزرار التواصل وأزرار المنصة الشاملة
    application.add_handler(CallbackQueryHandler(contact_callback_handler, pattern="^(contact_.*|schedules_lectures|discount_codes|add_discount_start|manage_group|manage_courses|manage_library|hw_view_submissions|manage_q_bank|honors_achievements|manage_control|main_menu)$"))
    

    # --- [ثالثاً: معالجات الرسائل النصية] ---
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(int(owner_id)), handle_contact_message))
    
    # [StudentAI]: رسائل الطلاب
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Chat(owner_id), handle_contact_message))

    
    # 2. إعداد مراقب التفعيل
    job_queue = application.job_queue
    if job_queue: # إضافة تحقق أمان لضمان عدم انهيار البوت
        job_queue.run_repeating(activation_monitor, interval=60, first=10)
    
    # 3. بدء تشغيل المحرك
    await application.initialize()
    await application.start()
    
    # --- [حل مشكلة Conflict وفشل الحفظ] ---
    await application.bot.delete_webhook(drop_pending_updates=True)
    # إذا كنت تستخدم create_task في الملف الرئيسي، استمر باستخدام start_polling هنا بحذر
    await application.updater.start_polling(drop_pending_updates=True)



