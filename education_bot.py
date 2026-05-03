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
from ui_keyboards import get_keyboard
logger = logging.getLogger(__name__)
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
    db_manager as dm,
    update_global_version,
    export_bot_data_to_excel,
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

# --- [ المعالجات الأساسية - أمر البداية المطوّر ] ---
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    معالجة أمر /start برسائل ترحيبية ذكية ودعم نظام الإحالة والأدوار:
    (مالك، موظف، مدرب، طالب)
    مع إضافة نظام طباعة وتتبع (Debug) لكل خطوة لضمان مطابقة "الورق".
    """

    # =========================================================
    # [ 0 ] تجهيز المتغيرات الأساسية
    # =========================================================
    user = update.effective_user
    bot_token = context.bot.token
    # استخراج الآيدي الرقمي للبوت لضمان مطابقة الفلترة في "الورق"
    bot_id_only = str(bot_token.split(':')[0]) if ':' in str(bot_token) else str(bot_token)
    
    query = update.callback_query
    message = update.message or (query.message if query else None)

    # 🟦 [تتبع]: بيانات المستخدم الذي ضغط Start
    print(f"\n--- [ 🟢 بدء تتبع عملية التمييز ] ---")
    print(f"👤 المستخدم الحالي: {user.full_name}")
    print(f"🆔 آيدي المستخدم (Current User ID): {user.id}")
    print(f"🤖 آيدي البوت الحالي (Numeric ID): {bot_id_only}")

    # حماية ai_config من None
    def ensure_dict(val):
        return val if isinstance(val, dict) else {}

    # دالة إرسال آمنة (توحد كل الإرسال)
    async def safe_send(text, reply_markup=None, use_edit=False):
        try:
            if query and use_edit:
                await query.answer()
                await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
            else:
                await message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception:
            try:
                await message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
            except Exception as e:
                print(f"❌ Send Fallback Failed: {e}")

    # =========================================================
    # [ 1 ] جلب الإعدادات وتحديد قائمة الملاك
    # =========================================================
    config = get_bot_config(bot_token)
    
    # تحسين جلب آيدي الملاك لدعم القوائم أو النصوص المنفصلة بفاصلة (مطابقة لعمود admin_ids في الورق)
    raw_admin_ids = config.get("admin_ids") or config.get("آيدي_الأدمن") or "0"
    if isinstance(raw_admin_ids, list):
        admin_list = [int(str(i).strip()) for i in raw_admin_ids if str(i).strip().isdigit()]
    else:
        admin_list = [int(i.strip()) for i in str(raw_admin_ids).split(",") if i.strip().isdigit()]

    # 🟦 [تتبع]: طباعة قائمة الملاك المعتمدة من "الورق"
    print(f"👑 قائمة الملاك من ورقة (إعدادات_المحتوى): {admin_list}")
    
    bot_owner_id = admin_list[0] if admin_list else 0
    is_owner = user.id in admin_list
    
    print(f"🧐 هل المستخدم موجود في قائمة الملاك؟ -> {is_owner}")

    ai_config = ensure_dict(get_ai_setup(bot_token))

    # =========================================================
    # [ 2 ] تصحيح اسم المؤسسة (Double Validation)
    # =========================================================
    def is_empty(val):
        return not val or str(val).strip() in ["0", "None", ""]

    if is_owner:
        if is_empty(ai_config.get('اسم_المؤسسة')):
            try:
                # محاولة الجلب باستخدام المعرف الرقمي للمطابقة مع عمود bot_id في الورق
                dm.cursor.execute(
                    'SELECT "اسم_المؤسسة" FROM "إعدادات_المحتوى" WHERE "bot_id" = ?',
                    (str(bot_id_only),)
                )
                db_row = dm.cursor.fetchone()

                if db_row and db_row[0] not in [None, "0", "", "None"]:
                    ai_config['اسم_المؤسسة'] = db_row[0]

                    if bot_token not in FACTORY_GLOBAL_CACHE["data"]:
                        FACTORY_GLOBAL_CACHE["data"][bot_token] = {}

                    FACTORY_GLOBAL_CACHE["data"][bot_token]['اسم_المؤسسة'] = db_row[0]

            except Exception as e:
                import logging
                logging.error(f"⚠️ خطأ في فحص اسم المؤسسة من القاعدة المحلية: {e}")

    # =========================================================
    # [ 3 ] التهيئة الأولى للمالك
    # =========================================================
    if is_owner:
        if is_empty(ai_config.get('اسم_المؤسسة')):
            print(f"⚙️ حالة المالك: بانتظار تهيئة اسم المنصة.")
            context.user_data['action'] = 'awaiting_institution_name'

            text = (
                "👋 <b>أهلاً بك يا دكتور!</b>\n\n"
                "قبل البدء، يرجى إرسال <b>اسم المنصة التعليمية</b> الخاصة بك:\n"
                "مثال: أكاديمية النخبة، مركز التدريب التقني..."
            )

            await safe_send(text, use_edit=True)
            return

    # =========================================================
    # [ 4 ] تحليل args مرة واحدة (Optimization)
    # =========================================================
    arg = context.args[0] if context.args and isinstance(context.args[0], str) else ""

    # =========================================================
    # [ 5 ] تسجيل الكوادر (reg_)
    # =========================================================
    if arg.startswith("reg_"):
        reg_token = arg.replace("reg_", "")
        temp_tokens = FACTORY_GLOBAL_CACHE.get("temp_registration_tokens", {})

        if reg_token in temp_tokens:
            role = temp_tokens[reg_token]
            del FACTORY_GLOBAL_CACHE["temp_registration_tokens"][reg_token]

            context.user_data['reg_role'] = role
            context.user_data['action'] = 'awaiting_reg_full_name'

            role_text = "كادرنا التعليمي (مدرب)" if role == "coach" else "كادرنا الإداري (موظف)"

            await safe_send(
                f"👋 <b>أهلاً بك!</b> نتشرف بانضمامك إلى {role_text}.\n\n"
                f"يرجى إرسال <b>اسمك الثلاثي</b> باللغة العربية لاعتماد حسابك:"
            )
            return
        else:
            await safe_send("⚠️ معذرة، هذا الرابط غير صالح أو انتهت صلاحيته.")
            return

    # =========================================================
    # [ 6 ] روابط الهدايا (gift_)
    # =========================================================
    if arg.startswith("gift_"):
        gift_code = arg.replace("gift_", "")

        try:
            from sheets import ss
            sheet_coupons = ss.worksheet("الكوبونات")
            coupon = sheet_coupons.find(gift_code, in_column=3)

            if coupon:
                coupon_data = sheet_coupons.row_values(coupon.row)

                if len(coupon_data) >= 8 and coupon_data[7] == "نشط":
                    course_id = coupon_data[10].replace("دورة_", "")
                    context.user_data['reg_flow'] = {'gift_code': gift_code}

                    await course_engine.start_registration_flow(
                        update, context, course_id, payment_method="Gift"
                    )
                    return

            await safe_send("⚠️ معذرة، هذا الرابط تم استخدامه مسبقاً أو غير موجود.")

        except Exception as e:
            print(f"Gift Link Error: {e}")
            await safe_send("⚠️ حدث خطأ أثناء معالجة رابط الهدية.")

        return

    # =========================================================
    # [ 7 ] نظام الإحالة
    # =========================================================
    inviter_id = None
    if arg.startswith("ref_"):
        potential_inviter = arg.replace("ref_", "")
        if str(potential_inviter) != str(user.id):
            inviter_id = potential_inviter

    # =========================================================
    # [ 8 ] تسجيل المستخدم (تعبئة ورقة المستخدمين)
    # =========================================================
    is_new_user = save_user(user.id, user.username, inviter_id, bot_token=bot_token)

    # =========================================================
    # [ 9 ] إشعار المالك
    # =========================================================
    if is_new_user and bot_owner_id:
        try:
            all_users = FACTORY_GLOBAL_CACHE["data"].get("المستخدمين", [])
            total_users = sum(
                1 for u in all_users if str(u.get("bot_id")) == str(bot_id_only)
            )
        except Exception as e:
            total_users = "جاري التحديث.."

        try:
            notification_text = (
                f"<b>تم دخول شخص جديد إلى المصنع الخاص بك</b> 👾\n"
                f"            -----------------------\n"
                f"• <b>معلومات العضو الجديد:</b>\n\n"
                f"• <b>الاسم:</b> {user.full_name}\n"
                f"• <b>المعرف:</b> @{user.username if user.username else 'لا يوجد'}\n"
                f"• <b>الآيدي:</b> <code>{user.id}</code>\n"
                f"            -----------------------\n"
                f"• <b>إجمالي مستخدمي البوت:</b> {total_users} مستخدم"
            )

            await context.bot.send_message(
                chat_id=bot_owner_id,
                text=notification_text,
                parse_mode="HTML"
            )

        except Exception as e:
            print(f"⚠️ فشل إرسال إشعار العضو الجديد للمالك: {e}")

    # =========================================================
    # [ 10 ] اختيار رسالة الترحيب
    # =========================================================
    from datetime import datetime
    hour = datetime.now().hour

    def fetch_valid_msg(key, fallback):
        val = config.get(key)
        if not val or str(val).strip() in ["0", "None", ""]:
            return fallback
        return val

    if 5 <= hour < 12:
        msg = fetch_valid_msg("welcome_morning", "صباح العلم والهمة.. أي مهارة سنبني اليوم؟")
    elif 12 <= hour < 17:
        msg = fetch_valid_msg("welcome_noon", "طاب يومك.. الاستمرارية هي سر النجاح، لنكمل التعلم.")
    elif 17 <= hour < 22:
        msg = fetch_valid_msg("welcome_evening", "مساء الفكر المستنير.. حان وقت الحصاد المعرفي.")
    else:
        msg = fetch_valid_msg("welcome_night", "أهلاً بالمثابر.. العظماء يصنعون مستقبلهم في هدوء الليل.")

    # =========================================================
    # [ 11 ] تحديد الدور (تصحيح منطق الفحص والمطابقة مع الورق)
    # =========================================================
    print(f"🛡️ [DEBUG STEP 11]: بدء فحص الأدوار")
    
    # فحص صلاحيات الموظفين بشكل صارم من ورقة (الهيكل_التنظيمي_والصلاحيات)
    has_perm = str(check_user_permission(bot_token, user.id, "الصلاحيات")).upper() == "TRUE"
    has_cat_perm = str(check_user_permission(bot_token, user.id, "صلاحية_الأقسام")).upper() == "TRUE"
    
    # جلب بيانات الموظفين (ID الموظف أو المدرب هو العمود 3 في الهيكل)
    # ملاحظة: نستخدم الهيكل التنظيمي لفلترة المستخدم حسب آيدي البوت الحالي (العمود 1)
    employees_data = FACTORY_GLOBAL_CACHE["data"].get("الهيكل_التنظيمي_والصلاحيات") or \
                     FACTORY_GLOBAL_CACHE["data"].get("إدارة_الموظفين", [])
    
    user_row = next(
        (row for row in employees_data if 
            (isinstance(row, list) and len(row) > 2 and str(row[2]) == str(user.id) and str(row[0]) == str(bot_id_only)) or
            (isinstance(row, dict) and str(row.get("ID_الموظف_أو_المدرب") or row.get("user_id")) == str(user.id) and str(row.get("bot_id")) == str(bot_id_only))
        ),
        None
    )

    is_staff = has_perm or has_cat_perm or (user_row is not None)
    print(f"📋 فحص الصلاحيات: has_perm={has_perm}, is_staff={is_staff}")

    if is_owner:
        print(f"✅ تم التمييز كـ: مالك (Owner)")
        final_text = (
            f"<b>مرحباً بك يا دكتور {user.first_name} في مركز قيادة منصتك</b> 🎓\n\n"
            f"{msg}\n\n"
            f"يمكنك إدارة كافة تفاصيل المنصة من الأزرار أدناه:"
        )
        reply_markup = get_keyboard(5)

    elif is_staff:
        # التحقق هل هو مدرب أم موظف (مطابقة لعمود "نوع الكادر" المعتاد في نظامك)
        is_coach = False
        if isinstance(user_row, list) and len(user_row) >= 42 and "مدرب" in str(user_row[41]):
            is_coach = True
        elif isinstance(user_row, dict) and "مدرب" in str(user_row.get("الوظيفة") or user_row.get("نوع_الكادر", "")):
            is_coach = True

        if is_coach:
            print(f"👨‍🏫 تم التوجيه: واجهة مدرب (Key 7)")
            final_text = (
                f"<b>مرحباً بك يا كابتن {user.first_name} في غرفتك الأكاديمية</b> 👨‍🏫\n\n"
                f"{msg}\n\n"
                f"يمكنك متابعة طلابك وتصحيح الواجبات من الأزرار أدناه:"
            )
            reply_markup = get_keyboard(7)
        else:
            print(f"💼 تم التوجيه: واجهة موظف (Key 6)")
            final_text = (
                f"<b>مرحباً بك يا {user.first_name} في لوحة الإدارة التعليمية</b> 💼\n\n"
                f"{msg}\n\n"
                f"لديك صلاحيات الإدارة المعتمدة، يمكنك البدء من الأزرار أدناه:"
            )
            reply_markup = get_keyboard(6)

    else:
        # فحص هل المستخدم مسجل كطالب (مطابقة لورقة المستخدمين - عمود ID المستخدم)
        all_students = FACTORY_GLOBAL_CACHE["data"].get("المستخدمين", [])
        
        # الفلترة بالمستخدم والبوت معاً لضمان عدم الخلط بين المنصات كما هو موضح في "الورق"
        is_registered_student = any(
            (str(s.get("ID المستخدم") or s.get("user_id") or s.get("id")) == str(user.id)) and 
            (str(s.get("bot_id")) == str(bot_id_only))
            for s in all_students
        )
        
        # إذا لم يكن في الكاش، نعتمد على نتيجة save_user (إذا أعادت False فهو موجود مسبقاً لهذا البوت)
        if not is_registered_student and is_new_user is False:
            is_registered_student = True
            
        print(f"🎓 هل المستخدم طالب مسجل في هذا البوت؟ -> {is_registered_student}")

        org_name = ai_config.get('اسم_المؤسسة', 'منصتنا التعليمية')
        
        if is_registered_student:
            print(f"📖 تم التوجيه: واجهة طالب مسجل (Key 8)")
            final_text = f"<b>{msg}</b>\n\nمرحباً بك في {org_name} 🎓"
            reply_markup = get_keyboard(8)
        else:
            print(f"🚪 تم التوجيه: واجهة زائر جديد (Key 9)")
            final_text = f"<b>{msg}</b>\n\nمرحباً بك في {org_name} 🎓\nنحن سعداء بزيارتك، استكشف منصتنا الآن:"
            reply_markup = get_keyboard(9)
       
    # =========================================================
    # [ 12 ] الإرسال النهائي
    # =========================================================
    print(f"🚀 إرسال الواجهة النهائية... \n--- [ 🔴 انتهاء تتبع عملية التمييز ] ---\n")
    await safe_send(final_text, reply_markup=reply_markup, use_edit=True)







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
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Chat(int(owner_id)), handle_contact_message))
    

    
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



