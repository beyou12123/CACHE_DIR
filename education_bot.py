# --- [ 1. المكتبات القياسية ونظام التشغيل ] ---
import logging
import re
import io
import os
import uuid
import json
import secrets
import importlib
import importlib.util
from datetime import datetime

# --- [ 2. مكتبات معالجة البيانات والذكاء الاصطناعي ] ---
# محرك الذكاء الاصطناعي من جوجل (مع نظام حماية ضد الفشل)
try:
    import google.generativeai as genai
    AI_ENABLED = True
except (ImportError, ModuleNotFoundError):
    genai = None  # أضف هذا السطر لتعريف المتغير كـ None ومنع خطأ NameError
    AI_ENABLED = False
    print("⚠️ تنبيه: مكتبة google-generativeai غير مثبتة في البيئة الحالية.")

import g4f  # المحرك المجاني البديل لضمان استمرارية الردود
import pandas as pd # ضروري لعمليات استيراد وتصدير الإكسل
import openpyxl    # محرك معالجة ملفات xlsx

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

import sheets
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
    get_courses_knowledge_base
)
# --- [ استيرادات الموديلات الأخرى ] ---
import educational_manager
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
import course_engine
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
import cache_manager
from cache_manager import (
    FACTORY_GLOBAL_CACHE, 
    save_cache_to_disk, 
    fetch_full_factory_data,
    export_bot_data_to_excel,
    check_excel_permission_from_cache
)

# --- [ ذاكرة المحادثات المؤقتة للطلاب ] ---
user_messages = {} 


# إعداد المفتاح الذي حصلت عليه
genai.configure(api_key="AIzaSyCkpHbxvjZNqN_PT8O1yXUAIG-dMAGZj2Y")
model = genai.GenerativeModel('gemini-1.5-flash')
# إعداد السجلات (Logging) لمراقبة أداء البوت وتتبع الأخطاء
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- [ القوائم الرئيسية للمنصة - أزرار واجهة المستخدم ] ---
def get_student_menu():
    keyboard = [
        [InlineKeyboardButton("📚 استعراض الدورات", callback_data="view_categories")],
        [InlineKeyboardButton("👤 ملفي الدراسي", callback_data="my_profile"), 
         InlineKeyboardButton("🎟 تفعيل دورة", callback_data="activate_course")],
        # --- الزر الجديد الذي طلبته ---
        [InlineKeyboardButton("💰 اربح دورات مجانية", callback_data="referral_system")],
        [InlineKeyboardButton("❓ الأسئلة الشائعة", callback_data="view_faq"),
         InlineKeyboardButton("💬 الدعم الفني", callback_data="contact_admin")]
    ]
    return InlineKeyboardMarkup(keyboard)


#لوحة الأدمن 
def get_admin_panel():
    """قائمة الأزرار الرئيسية للوحة تحكم الإدارة - النسخة المطورة بضبط الـ AI"""
    keyboard = [
        [InlineKeyboardButton("📊 الإحصائيات الذكية", callback_data="admin_stats"), 
         InlineKeyboardButton("📡 الإذاعة المستهدفة", callback_data="smart_broadcast")],
        [InlineKeyboardButton("🛠 الإعدادات العامة وتجهيز النظام", callback_data="tech_settings")], 
        [InlineKeyboardButton("معلومات تجهيز النظام", callback_data="system_setup_information"), InlineKeyboardButton("🤖 ضبط الـ AI", callback_data="setup_ai_start")],
        [InlineKeyboardButton("📥 تحميل نسخة احتياطية ", callback_data="export_data_json"),
         InlineKeyboardButton("📤 رفع نسخة بيانات", callback_data="import_data_json")],

        [InlineKeyboardButton("❌ إغلاق", callback_data="close_panel")]
    ]
    return InlineKeyboardMarkup(keyboard)


#لوحة الموظفين 
def get_employee_panel():
    """لوحة الشؤون التعليمية للموظفين والمدربين بناءً على الصلاحيات"""
    # تم إسناد النص لمتغير لاستخدامه في رسائل التعديل لاحقاً
    text = "👨‍🏫 <b>إدارة الشؤون التعليمية :</b>\nيمكنك إضافة مدربين جدد دورات جديدة او اقسام او مجموعات أو استعراض القائمة الحالية للحذف."
    
    keyboard = [
        [InlineKeyboardButton("📁 إدارة الأقسام", callback_data="manage_cats"), 
         InlineKeyboardButton("📚 إدارة الدورات", callback_data="manage_courses")],
        [InlineKeyboardButton("المكتبة الشاملة", callback_data="manage_library"),
         InlineKeyboardButton("الأوسمة والإنجازات", callback_data="honors_achievements")],
        [InlineKeyboardButton("إدارة المجموعات", callback_data="manage_group"), 
         InlineKeyboardButton("الأسئلة الشائعة", callback_data="frequently_guestions")],
        [InlineKeyboardButton("جداول المحاضرات", callback_data="schedules_lectures"), 
         InlineKeyboardButton("🎟 الكوبونات", callback_data="manage_coupons")],
        [InlineKeyboardButton("الكنترول", callback_data="manage_control")],
        [InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)


#لوحة المدرب 
def get_coach_panel():
    """لوحة التحكم الأكاديمية الخاصة بالمدربين"""
    text = "👨‍🏫 <b>غرفة الإدارة الأكاديمية (المدرب):</b>\nمرحباً بك! يمكنك إدارة مجموعاتك، متابعة طلابك، وتصحيح الواجبات من هنا."
    
    keyboard = [
        [InlineKeyboardButton("👥 مجموعاتي الدراسية", callback_data="manage_group"), 
         InlineKeyboardButton("📚 دوراتي المتاحة", callback_data="manage_courses")],
        [InlineKeyboardButton("📅 جدول المحاضرات", callback_data="schedules_lectures"), 
         InlineKeyboardButton("📖 المكتبة التعليمية", callback_data="manage_library")],
        [InlineKeyboardButton("📑 تصحيح الواجبات", callback_data="hw_view_submissions"), 
         InlineKeyboardButton("📝 بنك الأسئلة", callback_data="manage_q_bank")],
        [InlineKeyboardButton("🏆 الأوسمة والتقييمات", callback_data="honors_achievements"), 
         InlineKeyboardButton("🎮 غرفة الكنترول", callback_data="manage_control")],
        [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- [ المعالجات الأساسية - أمر البداية المطوّر ] ---
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start برسائل ترحيبية ذكية ودعم نظام الإحالة والأدوار (مالك، موظف، مدرب، طالب)"""
    from datetime import datetime

    user = update.effective_user
    bot_token = context.bot.token
    query = update.callback_query
    
    # جلب كافة الإعدادات من قاعدة البيانات
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))
    ai_config = get_ai_setup(bot_token)
    
    # --- [ 1. فحص إعدادات المالك (التهيئة الأولى) ] ---
    if user.id == bot_owner_id:
        if not ai_config or not ai_config.get('اسم_المؤسسة'):
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
        from cache_manager import FACTORY_GLOBAL_CACHE
        
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
#>>>>>>>>>>>>>>>>#>>>>>>>>>>>>>>>>
    # --- [ معالجة رابط الهدية للمستلم ] ---
    if context.args and context.args[0].startswith("gift_"):
        gift_code = context.args[0].replace("gift_", "")
        

        sheet_coupons = ss.worksheet("الكوبونات")
        coupon = sheet_coupons.find(gift_code, in_column=3)
        
        if coupon:
            coupon_data = sheet_coupons.row_values(coupon.row)
            # التأكد أن الكوبون "نشط"
            if coupon_data[7] == "نشط":
                # استخراج معرف الدورة من الملاحظات (التي خزنّاها بصيغة دورة_CRSxxxx)
                course_id = coupon_data[10].replace("دورة_", "")
                
                # تخزين كود الهدية في بيانات المستخدم لبدء التسجيل
                context.user_data['reg_flow'] = {'gift_code': gift_code}
                
                # استدعاء محرك التسجيل الموحد
                await course_engine.start_registration_flow(update, context, course_id, payment_method="Gift")
                return
            else:
                await update.message.reply_text("⚠️ معذرة، هذا الرابط تم استخدامه مسبقاً.")
                return

#>>>>>>>>>>>>>>>>#>>>>>>>>>>>>>>>>

    # --- [ 2. نظام الإحالة المتطور (للطلاب والزوار) ] ---
    inviter_id = None
    if context.args and context.args[0].startswith("ref_"):
        potential_inviter = context.args[0].replace("ref_", "")
        # التأكد أن المستخدم لا يحيل نفسه
        if str(potential_inviter) != str(user.id):
            inviter_id = potential_inviter

    # --- [ 3. تسجيل المستخدم في القاعدة (استدعاء واحد فقط) ] ---
    # نمرر inviter_id (سواء كان ID أو None) ليتم الحفظ في العمود 10 مرة واحدة
    save_user(user.id, user.username, inviter_id)
    

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

    # --- [ 4. فرز الرتب وإرسال الواجهة المناسبة - نسخة صارمة ومعززة ضد الانهيار ] ---
    
    # تحويل معرف المالك إلى رقم صحيح لضمان دقة المقارنة ومنع التداخل
    try:
        current_owner_id = int(bot_owner_id)
    except:
        current_owner_id = 0

    # أ: رتبة المالك (الأولوية المطلقة - السيادة الكاملة على النظام)
    if user.id == current_owner_id:
        final_text = (
            f"<b>مرحباً بك يا دكتور {user.first_name} في مركز قيادة منصتك</b> 🎓\n\n"
            f"{msg}\n\n"
            f"يمكنك إدارة كافة تفاصيل المنصة من الأزرار أدناه:"
        )
        reply_markup = get_admin_panel()

    # ب: رتبة الموظف أو المدرب (التحقق المترابط بنظام الفحص الآمن لمنع توقف البوت)
    elif (check_user_permission(bot_token, user.id, "الصلاحيات") == True) or \
         (check_user_permission(bot_token, user.id, "صلاحية_الأقسام") == True):
        
        # --- [ الإصلاح المستهدف: الفرز بين المدرب والموظف بناءً على العمود 42 ] ---
        from cache_manager import FACTORY_GLOBAL_CACHE
        # جلب البيانات من الكاش مباشرة (أكثر أماناً وأسرع)
        employees_data = FACTORY_GLOBAL_CACHE["data"].get("إدارة_الموظفين", [])
        
        # البحث عن صف المستخدم: المعرف موجود في العمود 3 (فهرس 2)
        user_row = next((row for row in employees_data if len(row) > 2 and str(row[2]) == str(user.id)), None)
        
        # التأكد من الرتبة في العمود 42 (فهرس 41)
        if user_row and len(user_row) >= 42 and str(user_row[41]).strip() == "مدرب":
            final_text = (
                f"<b>مرحباً بك يا كابتن {user.first_name} في غرفتك الأكاديمية</b> 👨‍🏫\n\n"
                f"{msg}\n\n"
                f"يمكنك متابعة طلابك وتصحيح الواجبات من الأزرار أدناه:"
            )
            reply_markup = get_coach_panel()
        else:
            # القيمة الافتراضية إذا لم يكن مدرباً فهو موظف إداري كما في منطقك الأصلي
            final_text = (
                f"<b>مرحباً بك يا {user.first_name} في لوحة الإدارة التعليمية</b> 💼\n\n"
                f"{msg}\n\n"
                f"لديك صلاحيات الموظفين المعتمدة، يمكنك البدء بالإدارة من الأزرار أدناه:"
            )
            reply_markup = get_employee_panel()

    # ج: رتبة الطالب (الحالة الافتراضية النهائية لمن لا تنطبق عليه الشروط أعلاه)
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
# --- [ معالج ضغطات الأزرار (Callback Query Handler) ] ---
# --------------------------------------------------------------------------
async def contact_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """التحكم في كافة عمليات الضغط على الأزرار الشفافة Inline Buttons"""
    print("--- [DEBUG]: تم ضغط زر في البوت الآن والطلب وصل للمعالج ---")    
    query = update.callback_query
    data = query.data
    
    user_id = query.from_user.id
    bot_token = context.bot.token
    config = get_bot_config(bot_token)

    bot_owner_id = int(config.get("admin_ids", 0))
    await query.answer()
# --------------------------------------------------------------------------

    # 1. معالجة جداول المحاضرات
    if data == "schedules_lectures":

        await show_lectures_logic(update, context)
        
    # 2. فتح لوحة إدارة أكواد الخصم الرئيسية
    elif data == "discount_codes":

        await show_discount_codes_logic(update, context)

    # 3. زر "إضافة كود جديد" (هذا الزر كان مفقوداً في ملفك)
    elif data == "add_discount_start":

        await add_discount_start(update, context)

    # 4. معالجة خطوات التحقق من الدورة والاستمرار
    # التعديل المطلوب لضمان الاستجابة وعدم التجمد:
    elif data.startswith("d_ch_"): # استخدمنا d_ch_ بدلاً من dsc_check_
        course_id = data.replace("d_ch_", "")

        await process_dsc_check(update, context, course_id)
    #>>>>>>>>>>>>>>>>    
# داخل contact_callback_handler (عند اختيار "أريدها لي"):
    elif data.startswith("buy_c_me_"):
        course_id = data.replace("buy_c_me_", "")
        await course_engine.start_registration_flow(update, context, course_id, payment_method="points")
#>>>>>>>>>>>>>>>>
    # معالجة أزرار الجنس والتأكيد من محرك التسجيل
    elif data.startswith("reg_gen_"):
        gender = "ذكر" if "male" in data else "أنثى"
        context.user_data['reg_flow']['gender'] = gender
        context.user_data['reg_flow']['step'] = 'awaiting_country'
        await query.message.reply_text("🌍 يرجى إرسال <b>اسم البلد</b> الحالي:")

    elif data == "confirm_reg_final":
        await course_engine.finalize_and_save(update, context)
#>>>>>>>>>>>>>>>>*
#©©©©©©©©©©
# المناداة لدالة معلومات تهيئة البوت 
    elif data == "system_setup_information":

        await show_system_setup_information(update, context)


    elif data == "dsc_continue":

        await process_dsc_ask_desc(update, context)

    # 5. عرض وإدارة الأكواد للمالك
    elif data == "list_all_discounts":
        
        await list_all_discounts_ui(update, context)

    # 6. عرض تفاصيل كود محدد
    elif data.startswith("view_disc_"):
        disc_id = data.replace("view_disc_", "")

        await view_discount_details_ui(update, context, disc_id)
#>>>>>>>>>>>>>>>>
    # 7. معالج حذف الكود
    elif data.startswith("confirm_del_disc_"):
        disc_id = data.replace("confirm_del_disc_", "")

        sheet = ss.worksheet("أكواد_الخصم")
        try:
            cell = sheet.find(disc_id, in_column=3)
            if cell:
                sheet.delete_rows(cell.row)
                await query.answer("✅ تم حذف كود الخصم بنجاح!", show_alert=True)

                await list_all_discounts_ui(update, context)
        except:
            await query.answer("❌ فشل الحذف.", show_alert=True)

    # 8. زر العودة للقائمة الرئيسية (تم تصحيح الشرط هنا لضمان تسلسل elif)
    elif data == "main_menu":
        await start_handler(update, context)
#>>>>>>>>>>>>>>>>
    # معالج اربح معنا (تم ربطه بـ elif لضمان الاستجابة)
    elif data == "referral_system":
        # ملاحظة: تم إزالة query.answer() المكررة هنا لأنها تم استدعاؤها في بداية الدالة
        user_id = query.from_user.id
        bot_token = context.bot.token
        
        # جلب يوزر البوت ديناميكياً لتوليد الرابط
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        
        # جلب إحصائيات الإحالة والرصيد من ملف sheets

        stats = get_user_referral_stats(bot_token, user_id)
        
        text = (
            f"💰 <b>نظام المكافآت والإحالة</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"مرحباً بك! يمكنك الآن الحصول على دوراتنا <b>مجاناً</b> عبر دعوة أصدقائك للمنصة.\n\n"
            f"📢 <b>كيف يعمل النظام؟</b>\n"
            f"1️⃣ انسخ رابطك الفريد أدناه.\n"
            f"2️⃣ شاركه مع أصدقائك أو في مجموعات الدراسة.\n"
            f"3️⃣ مقابل كل شخص يسجل عبرك، ستحصل على <b>نقاط رصيد</b>.\n\n"
            f"🔗 <b>رابط الإحالة الخاص بك:</b>\n"
            f"<code>{referral_link}</code>\n\n"
            f"📊 <b>إحصائياتك الحالية:</b>\n"
            f"👤 عدد الناجحين في دعوتهم: <b>{stats.get('count', 0)}</b> طالب\n"
            f"💰 رصيدك المكتسب: <b>{stats.get('balance', 0)} نقطة</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"💡 <i>يمكنك استبدال النقاط بفتح الدورات المدفوعة فور وصولك للحد المطلوب.</i>"
        )
        
        # تعديل أزرار قائمة الإحالة لتشمل المتجر وطلب السحب
        keyboard = [
            [InlineKeyboardButton("🛒 استبدال النقاط بالدورات", callback_data="redeem_store")],
            [InlineKeyboardButton("💰 سحب الأرباح (كاش)", callback_data="request_payout_start")], 
            [InlineKeyboardButton("🔄 تحديث الإحصائيات", callback_data="referral_system")],
            [InlineKeyboardButton("🔙 العودة للقائمة", callback_data="main_menu")]
        ]

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    elif data == "request_payout_start":
        bot_token = context.bot.token
        # 1. جلب الإعدادات الديناميكية لهذا البوت من ورقة الإعدادات
        currency = get_bot_setting(bot_token, "currency_unit", default="نقطة")
        min_payout = float(get_bot_setting(bot_token, "maximum_withdrawal_marketers", default=50))
        
        # 2. جلب رصيد المسوق الحالي من البيانات
        stats = get_user_referral_stats(bot_token, user_id)
        current_balance = float(stats.get('balance', 0))
        
        # 3. التحقق من الحد الأدنى للسحب
        if current_balance < min_payout:
            await query.answer(f"⚠️ رصيدك {current_balance} {currency}. الحد الأدنى للسحب هو {min_payout} {currency}.", show_alert=True)
            return

        # 4. حفظ البيانات المؤقتة في الذاكرة لبدء طلب وسيلة التحويل
        context.user_data['payout_amount'] = current_balance
        context.user_data['currency'] = currency 
        context.user_data['action'] = 'awaiting_payout_method'
        
        text = (
            f"💰 <b>طلب سحب الأرباح</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"الرصيد القابل للسحب: <b>{current_balance} {currency}</b>\n\n"
            f"يرجى إرسال <b>وسيلة التحويل</b> وبياناتك (مثلاً: رقم الحساب أو المحفظة):\n"
        )
        await query.edit_message_text(text, parse_mode="HTML")



#>>>>>>>>>>>>>>>>
    # اعتماد صرف الأرباح - (المرحلة 1: طلب صورة الإيصال)
    elif data.startswith("payout_approve_"):
        req_id = data.replace("payout_approve_", "")
        
        # جلب بيانات الطلب من البيانات لمعرفة من هو صاحب الطلب (المسوق)
        sheet_req = ss.worksheet("سجل_السحوبات")
        cell = sheet_req.find(str(req_id), in_column=4)
        if cell:
            row_data = sheet_req.row_values(cell.row)
            target_user_id = row_data[1] # العمود الثاني هو ID المسوق
            context.user_data['target_payout_user_id'] = target_user_id
            context.user_data['payout_req_id'] = req_id
            context.user_data['action'] = 'awaiting_payout_proof'
            
            await query.edit_message_text(
                f"📸 <b>يرجى إرسال صورة الإيصال للطلب {req_id}</b>\n"
                f"سيتم إرسالها فوراً للمسوق وتحديث السجل.", 
                parse_mode="HTML"
            )


    # رفض طلب السحب وإرجاع الرصيد (يبقى كما هو لأنه صحيح وآمن)
    elif data.startswith("payout_reject_"):
        req_id = data.replace("payout_reject_", "")
        try:
            sheet_req = ss.worksheet("سجل_السحوبات")
            row_cell = sheet_req.find(str(req_id), in_column=4)
            if row_cell:
                req_data = sheet_req.row_values(row_cell.row)
                target_user_id = req_data[1] 
                refund_amount = float(req_data[4])
                
                sheet_users = ss.worksheet("المستخدمين")
                u_cell = sheet_users.find(str(target_user_id), in_column=1)
                if u_cell:
                    old_bal = float(sheet_users.cell(u_cell.row, 11).value or 0)
                    sheet_users.update_cell(u_cell.row, 11, old_bal + refund_amount)
               
               
                update_withdrawal_status(bot_token, req_id, "مرفوض", admin_note="تم الرفض وإعادة الرصيد")
                await query.edit_message_text(f"❌ تم رفض الطلب <code>{req_id}</code> وإعادة {refund_amount} إلى رصيد المسوق.", parse_mode="HTML")
        except Exception as e:
            await query.answer(f"❌ خطأ في عملية الرفض: {e}")


#>>>>>>>>>>>>>>>>




    # معالج تعطيل/تفعيل الكود مؤقتاً
    elif data.startswith("dsc_tog_"):
        parts = data.split("_")
        disc_id = parts[2]
        new_action = parts[3] # on أو off
        
 
        sheet = ss.worksheet("أكواد_الخصم")
        try:
            cell = sheet.find(disc_id, in_column=3) # البحث في عمود معرف_الخصم
            if cell:
                new_status = "نشط" if new_action == "on" else "معطل"
                sheet.update_cell(cell.row, 11, new_status) # تحديث العمود 11 (الحالة)
                await query.answer(f"✅ تم تغيير حالة الكود إلى: {new_status}", show_alert=True)
                
                # إعادة تحديث الواجهة لإظهار الحالة الجديدة

                await view_discount_details_ui(update, context, disc_id)
        except Exception as e:
            await query.answer("❌ فشل تحديث الحالة.")
#>>>>>>>>>>>>>>>>
    # استبدال النقاط بالدورات - عرض المتجر
    elif data == "redeem_store":
        await query.answer()
        
        stats = get_user_referral_stats(bot_token, user_id)
        current_balance = stats.get('balance', 0)
        
        # جلب سعر الدورة الموحد من الإعدادات
        redeem_cost = get_bot_setting(bot_token, "min_points_redeem", default=100)
        
        text = (
            f"🛒 <b>متجر استبدال النقاط</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"رصيدك الحالي: 💰 <b>{current_balance} نقطة</b>\n"
            f"تكلفة فتح أي دورة: 🎫 <b>{redeem_cost} نقطة</b>\n\n"
            f"اختر الدورة التي تود فتحها برصيدك:"
        )
        
        # جلب الدورات المتاحة من البيانات
        courses_ws = ss.worksheet("الدورات_التدريبية")
        all_courses = courses_ws.get_all_records()
        
        keyboard = []
        for course in all_courses:
            if str(course.get('bot_id')) == str(bot_token):
                c_name = course.get('اسم_الدورة')
                c_id = course.get('ID_الدورة')
                
                # زر الشراء يتغير حسب الرصيد
                if float(current_balance) >= float(redeem_cost):
                    keyboard.append([InlineKeyboardButton(f"✅ فتح: {c_name}", callback_data=f"select_c_{c_id}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"🔒 {c_name} (تحتاج نقاط)", callback_data="insufficient_points")])
        
        keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data="referral_system")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # المرحلة الوسيطة: اختيار (لي أم لمشترك آخر)
    elif data.startswith("select_c_"):
        course_id = data.replace("select_c_", "")
        
        # جلب اسم الدورة للتوضيح
        courses_ws = ss.worksheet("الدورات_التدريبية")
        course_row = courses_ws.find(str(course_id), in_column=2) # نفترض ID الدورة في العمود 2
        course_name = courses_ws.cell(course_row.row, 3).value if course_row else "الدورة المختارة"

        text = (
            f"🎯 <b>تأكيد اختيار الدورة</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"الدورة المختارة هي: <b>{course_name}</b>\n\n"
            f"هل تريد التسجيل في هذه الدورة لنفسك، أم تريد إهداءها لمشترك آخر؟"
        )
        
        keyboard = [
            [InlineKeyboardButton("👤 أريدها لي", callback_data=f"buy_c_me_{course_id}")],
            [InlineKeyboardButton("🎁 أريدها لمشترك آخر", callback_data=f"buy_c_gift_{course_id}")],
            [InlineKeyboardButton("🔙 عودة للقائمة السابقة", callback_data="redeem_store")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # تنفيذ عملية الشراء "لي"
    elif data.startswith("buy_c_me_"):
        course_id = data.replace("buy_c_me_", "")
        redeem_cost = get_bot_setting(bot_token, "min_points_redeem", default=100)
        
        # استدعاء دالة الخصم والتفعيل
        success, new_balance = redeem_points_for_course(bot_token, user_id, redeem_cost)
        
        if success:
            await query.answer("🎉 مبروك! تم فتح الدورة بنجاح", show_alert=True)
            await query.edit_message_text(
                f"✅ <b>تم شراء الدورة بنجاح!</b>\n"
                f"رصيدك المتبقي: <b>{new_balance} نقطة</b>.\n\n"
                f"تم تفعيل الدورة في حسابك، يمكنك البدء الآن من القائمة الرئيسية.",
                parse_mode="HTML"
            )
        else:
            await query.answer("❌ فشلت العملية، تأكد من رصيدك.", show_alert=True)

    # تنفيذ عملية الإهداء لمشترك آخر (نظام روابط الإهداء المشفرة)
    elif data.startswith("buy_c_gift_"):
        course_id = data.replace("buy_c_gift_", "")
        bot_token = context.bot.token
        
        # 1. التحقق من وجود رابط هدية نشط لم يُستخدم بعد لهذا المسوق (القفل الذكي)

        sheet_coupons = ss.worksheet("الكوبونات")
        records = sheet_coupons.get_all_records()
        
        active_code = None
        for r in records:
            if (str(r.get("bot_id")) == str(bot_token) and 
                str(r.get("معرف_الطالب")) == str(user_id) and 
                str(r.get("حالة_الكوبون")) == "نشط"):
                active_code = r.get("معرف_الكوبون")
                break
        
        # 2. إذا وجد رابط نشط، يتم تزويد المسوق به بدلاً من توليد جديد
        if active_code:
            bot_info = await context.bot.get_me()
            old_link = f"https://t.me/{bot_info.username}?start=gift_{active_code}"
            
            text = (
                f"⚠️ <b>عذراً عزيزي المسوق!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"لديك بالفعل <b>هدية سارية المفعول</b> لم يتم استخدامها بعد. نظامنا يسمح بهدية واحدة نشطة في كل مرة لضمان دقة حساباتك.\n\n"
                f"🔗 <b>رابط الهدية الحالي:</b>\n"
                f"<code>{old_link}</code>\n\n"
                f"✨ <i>يرجى مشاركة الرابط أعلاه، وفور استخدامه ستتمكن من توليد هدية جديدة.</i>"
            )
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 العودة للمتجر", callback_data="redeem_store")]
            ]), parse_mode="HTML")
            return

        # 3. توليد كود هدية جديد وحفظه (بدون خصم نقاط في هذه المرحلة)
        import secrets
        gift_code = f"GFT{secrets.token_hex(3).upper()}"
        
        # ترتيب الأعمدة في شيت الكوبونات (11 عمود):
        # bot_id, معرف_الفرع, معرف_الكوبون, معرف_الطالب (نخزن فيه ID المهدِي), قيمة_الخصم, 
        # نوع_الخصم, الحد_الأقصى_للاستخدام, حالة_الكوبون, تاريخ_الإنشاء, تاريخ_الانتهاء, ملاحظات

        new_row = [
            str(bot_token), "1001001", gift_code, str(user_id), "100", 
            "هدية دورة", "1", "نشط", get_system_time("date"), "2026-12-31", f"دورة_{course_id}"
        ]
        sheet_coupons.append_row(new_row, value_input_option='USER_ENTERED')

        update_global_version(bot_token)

        bot_info = await context.bot.get_me()
        new_link = f"https://t.me/{bot_info.username}?start=gift_{gift_code}"
        
        success_text = (
            f"🎁 <b>تم تجهيز رابط الهدية بنجاح!</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"يمكنك الآن إرسال هذا الرابط لمن تحب. سيتم خصم النقاط من رصيدك <b>فقط</b> عندما يقوم الطرف الآخر بالتسجيل الفعلي.\n\n"
            f"🔗 <b>رابط الإهداء الخاص بك:</b>\n"
            f"<code>{new_link}</code>\n\n"
            f"📢 <i>سيصلك إشعار فوري فور تفعيل الهدية.</i>"
        )
        await query.edit_message_text(success_text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 العودة للمتجر", callback_data="redeem_store")]
        ]), parse_mode="HTML")


#>>>>>>>>>>>>>>>>
# ربط الدورات بالنقاط

    # --- [ ملفي الدراسي - عرض الدورات المشترك بها ] ---
    elif data == "my_profile":
        from cache_manager import FACTORY_GLOBAL_CACHE
        
        # جلب البيانات من الكاش لسرعة استجابة فائقة
        all_regs = FACTORY_GLOBAL_CACHE["data"].get("سجل_التسجيلات", [])
        
        # تصفية الدورات الخاصة بهذا الطالب في هذا البوت
        student_courses = [
            r for r in all_regs 
            if str(r.get("bot_id")) == str(bot_token) and str(r.get("ID_المستخدم_تيليجرام")) == str(user_id)
        ]

        if not student_courses:
            text = (
                "👤 <b>ملفك الدراسي</b>\n\n"
                "⚠️ أنت غير مشترك في أي دورة تعليمية حالياً.\n"
                "💡 يمكنك استعراض الدورات المتاحة والاشتراك بها."
            )
            keyboard = [
                [InlineKeyboardButton("📚 استعراض الدورات", callback_data="view_categories")],
                [InlineKeyboardButton("💰 اربح دورات مجانية", callback_data="referral_system")]
            ]
        else:
            text = (
                "👤 <b>ملفك الدراسي</b>\n\n"
                "إليك قائمة بالدورات التي تمتلك حق الوصول إليها:\n"
                "👇 انقر على اسم الدورة لفتح المحتوى التعليمي"
            )
            keyboard = []
            for reg in student_courses:
                c_name = reg.get('اسم_الدورة', 'دورة غير مسمى')
                c_id = reg.get('معرف_الدورة')
                keyboard.append([InlineKeyboardButton(f"📖 {c_name}", callback_data=f"open_content_{c_id}")])

        keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])
        
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        except Exception as e:
            await query.answer("جاري عرض ملفك الدراسي...")
            await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # --- [ محرك فتح محتوى الدورة ] ---
    elif data.startswith("open_content_"):
        course_id = data.replace("open_content_", "")

        # استدعاء الواجهة البرمجية لعرض الدروس (الموجودة في ملف course_engine.py)
        await show_course_content_ui(update, context, course_id)

# --------------------------------------------------------------------------
    # --- [ معالج استعراض الأقسام للطالب ] ---
    elif data == "view_categories":
        # جلب الأقسام من الكاش لسرعة الاستجابة

        categories = get_all_categories(bot_token)
        
        if not categories:
            await query.edit_message_text(
                "⚠️ <b>تنبيه:</b> لا توجد أقسام تعليمية متاحة حالياً.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]]),
                parse_mode="HTML"
            )
            return

        # بناء قائمة الأقسام كأزرار
        keyboard = []
        for cat in categories:
            keyboard.append([InlineKeyboardButton(f"📁 {cat['name']}", callback_data=f"std_view_cat_{cat['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])
        
        await query.edit_message_text(
            "📚 <b>المكتبة التعليمية:</b>\nاختر القسم الذي ترغب في استعراض دوراته:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

    # --- [ معالج عرض دورات قسم محدد للطالب ] ---
    elif data.startswith("std_view_cat_"):
        cat_id = data.replace("std_view_cat_", "")

        
        courses = get_courses_by_category(bot_token, cat_id)
        
        if not courses:
            await query.edit_message_text(
                "⚠️ لا توجد دورات متاحة في هذا القسم حالياً.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للأقسام", callback_data="view_categories")]]),
                parse_mode="HTML"
            )
            return

        keyboard = []
        for crs in courses:
            keyboard.append([InlineKeyboardButton(f"📘 {crs['name']}", callback_data=f"std_course_info_{crs['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 عودة للأقسام", callback_data="view_categories")])
        
        await query.edit_message_text(
            "📖 <b>الدورات المتاحة:</b>\nاختر دورة لمعرفة التفاصيل والتسجيل:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

# ==========================================
# الكود الجديد الذي تضعه هنا (من السطر 180 تقريباً)
# ==========================================
    # الإصلاح: معالجة الزر العام للمكتبة
    elif data == "manage_library":

        await manage_library_selector(update, context)
        
    elif data.startswith("manage_library_"):
        course_id = data.replace("manage_library_", "")
        await course_engine.show_library_menu(update, context, course_id)

    elif data.startswith("view_file_"):
        file_id = data.replace("view_file_", "")
        await course_engine.view_file_details(update, context, file_id)
# ==========================================
#~~~~~~~~~~~~~~~~
#المكتبة 
    elif data.startswith("add_lib_file_"):
        course_id = data.replace("add_lib_file_", "")
        await educational_manager.prompt_add_library_file(update, context, course_id)
#~~~~~~~~~~~~~~~~


#~~~~~~~~~~~~~~~~

    data = query.data
    await query.answer()

    # --- [ ممرات إدارة الحملات الإعلانية - الإضافة هنا ] ---
    if data == "manage_ads":
        await manage_ads_main_ui(update, context)
        return

    elif data == "ad_create_start":
        await ad_create_start(update, context)
        return

    elif data == "ad_report_view":
        await ad_report_view(update, context)
        return

    elif data.startswith("ad_set_crs_"):
        course_id = data.replace("ad_set_crs_", "")
        context.user_data['temp_ad'] = {'course_id': course_id}
        context.user_data['action'] = 'awaiting_ad_platform'
        await query.edit_message_text("🌐 <b>الخطوة 2:</b> أرسل اسم المنصة الإعلانية (مثلاً: فيسبوك):", parse_mode="HTML")
        return
    # --- [ نهاية الإضافة ] ---


#~~~~~~~~~~~~~~~~
# --------------------------------------------------------------------------
    # --- [ معالج الدعم الفني ] ---
    elif data == "contact_admin":
        # جلب إعدادات البوت لمعرفة هوية الإدارة

        config = get_bot_config(bot_token)
        
        # تحويل معرف المالك إلى نص نظيف
        admin_id = str(config.get("admin_ids", "")).split(',')[0].strip()
        
        if admin_id:
            text = (
                "💬 <b>قسم الدعم الفني:</b>\n"
                "━━━━━━━━━━━━━━\n"
                "يمكنك التواصل مباشرة مع إدارة المنصة للاستفسار عن الدورات أو حل المشكلات التقنية.\n\n"
                "👇 اضغط على الزر أدناه لبدء المحادثة:"
            )
            # إنشاء زر يحول الطالب لمحادثة خاصة مع المالك
            keyboard = [
                [InlineKeyboardButton("📨 إرسال رسالة للإدارة", url=f"tg://user?id={admin_id}")],
                [InlineKeyboardButton("🔙 العودة للقائمة", callback_data="main_menu")]
            ]
        else:
            # حالة عدم وجود معرف أدمن مسجل في البيانات
            text = "⚠️ عذراً، لم يتم ضبط حساب الدعم الفني لهذه المنصة بعد. يرجى المحاولة لاحقاً."
            keyboard = [[InlineKeyboardButton("🔙 العودة", callback_data="main_menu")]]

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --------------------------------------------------------------------------
    # --- [ معالج الأسئلة الشائعة ] ---
    elif data == "view_faq":
        from cache_manager import FACTORY_GLOBAL_CACHE
        
        # جلب بيانات الأسئلة الشائعة من الكاش (الرام)
        all_faq = FACTORY_GLOBAL_CACHE["data"].get("الأسئلة_الشائعة", [])
        bot_faq = [f for f in all_faq if str(f.get("bot_id")) == str(bot_token)]
        
        if not bot_faq:
            text = "❓ <b>الأسئلة الشائعة:</b>\n\nلا توجد أسئلة شائعة مضافة حالياً في هذا البوت."
        else:
            text = "❓ <b>الأسئلة الشائعة:</b>\n\n"
            for item in bot_faq:
                # عرض السؤال والإجابة من العمود "محتوى_السؤال_مع_الإجابة"
                text += f"📍 <b>{item.get('محتوى_السؤال_مع_الإجابة', 'سؤال غير مسمى')}</b>\n"
                text += "━━━━━━━━━━━━━━\n"

        keyboard = [[InlineKeyboardButton("🔙 العودة", callback_data="main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --------------------------------------------------------------------------
# الربط مع الأسئلة
    elif data == "add_question_bank":
        # استدعاء دالة بدء التدفق من المدير التعليمي
        await educational_manager.start_add_question_flow(update, context)
 

 
 
 

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
    # --- 1. إدارة الإحصائيات ---
    if data == "admin_stats":
        total_students = get_bot_users_count(bot_token)
        blocks = get_bot_blocks_count(bot_token)
        stats_text = (
            f"<b>📊 تقرير المنصة الحالي:</b>\n"
            f"-----------------------\n"
            f"👥 إجمالي الطلاب: {total_students}\n"
            f"🚫 عدد المحظورين: {blocks}\n"
            f"💰 مبيعات اليوم: 0.00$\n"
            f"📈 أكثر دورة طلباً: لا يوجد بيانات بعد"
        )
        await query.edit_message_text(stats_text, reply_markup=get_admin_panel(), parse_mode="HTML")

    # --- 2. إدارة الدورات التدريبية (الواجهة الرئيسية) ---

# --------------------------------------------------------------------------
    # إضافة هذا القسم للتعامل مع زر إدارة المجموعات
    elif data == "manage_group":
        # بما أن إدارة المجموعات تتطلب معرفة الدورة، سنعرض قائمة الدورات أولاً لاختيار واحدة

        all_courses = courses_sheet.get_all_records()
        bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
        
        if not bot_courses:
            await query.edit_message_text("⚠️ لا توجد دورات مضافة حالياً لإنشاء مجموعات لها.")
            return

        keyboard = [[InlineKeyboardButton(f"📖 {c['اسم_الدورة']}", callback_data=f"sel_course_groups_{c['معرف_الدورة']}")] for c in bot_courses]
        keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="main_menu")])
        
        await query.edit_message_text("🎯 **اختر الدورة المراد إدارة مجموعاتها:**", reply_markup=InlineKeyboardMarkup(keyboard))

    # معالجة اختيار الدورة للانتقال لملف المجموعات
    elif data.startswith("sel_course_groups_"):
        course_id = data.replace("sel_course_groups_", "")

        await manage_groups_main(update, context, course_id)




# --------------------------------------------------------------------------
    # --- 3. إدارة شؤون المدربين والموظفين (نسخة روابط الانضمام اللحظية) ---
    elif data == "manage_coaches":
        text = (
            "👨‍🏫 <b>إدارة الكادر التعليمي والإداري:</b>\n\n"
            "يمكنك توليد روابط انضمام فريدة صالحة لمرة واحدة لإضافة المدربين أو الموظفين آلياً إلى النظام."
        )
        keyboard = [
            [InlineKeyboardButton("➕ توليد رابط مدرب جديد", callback_data="gen_reg_coach")],
            [InlineKeyboardButton("➕ توليد رابط موظف جديد", callback_data="gen_reg_staff")],
            [InlineKeyboardButton("📋 عرض قائمة المدربين الحالية", callback_data="list_coaches")],
            [InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="tech_settings")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # منطق توليد روابط الانضمام اللحظية (مدربين وموظفين)
    elif data in ["gen_reg_coach", "gen_reg_staff"]:
        import secrets
        from cache_manager import FACTORY_GLOBAL_CACHE
        
        role = "coach" if data == "gen_reg_coach" else "staff"
        token = secrets.token_hex(4).upper() # توليد كود فريد قصير
        
        # تخزين الكود مع الرتبة في الذاكرة المركزية RAM
        FACTORY_GLOBAL_CACHE["temp_registration_tokens"][token] = role
        
        bot_info = await context.bot.get_me()
        reg_link = f"https://t.me/{bot_info.username}?start=reg_{token}"
        
        role_name = "مدرب" if role == "coach" else "موظف"
        text = (
            f"✅ <b>تم توليد رابط انضمام ({role_name}) جديد:</b>\n\n"
            f"<code>{reg_link}</code>\n\n"
            f"⚠️ <b>ملاحظة:</b> هذا الرابط صالح للاستخدام مرة واحدة فقط وسيختفي من الذاكرة بمجرد استخدامه."
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="manage_coaches")]]), parse_mode="HTML")

# --------------------------------------------------------------------------

    # 1. معالجة ضغطة "اعتماد" القادمة من المالك
    elif data.startswith("approve_reg_"):
        parts = data.split("_")
        role = parts[2]
        candidate_id = int(parts[3]) # هذا هو المعرف الذي أرسلته أنت في الزر
        
        # 🚨 النقطة الجوهرية: سحب البيانات من ذاكرة "المرشح" وليس "المالك"
        candidate_context = context.application.user_data.get(candidate_id)
        
        if candidate_context and 'reg_data' in candidate_context:
            # نقل البيانات لذاكرة المالك مؤقتاً لإتمام عملية اختيار الفرع
            context.user_data['reg_data'] = candidate_context['reg_data']
            context.user_data['pending_approve'] = {'role': role, 'id': candidate_id}
            context.user_data['candidate_username'] = candidate_context['reg_data'].get('username', 'بدون')
        else:
            # إذا لم يجد البيانات (بسبب ريستارت أو مسح الذاكرة)
            await query.answer("⚠️ عذراً، تعذر استعادة بيانات المرشح من الذاكرة. اطلب منه التسجيل مجدداً.", show_alert=True)
            return

        # 2. عرض قائمة الفروع المحدثة من الكاش (نظام 1001001)
        from cache_manager import FACTORY_GLOBAL_CACHE
        bot_branches = [r for r in FACTORY_GLOBAL_CACHE.get("data", {}).get("إدارة_الفروع", []) if str(r.get("bot_id")) == str(bot_token)]
        
        if not bot_branches:
            await query.edit_message_text("⚠️ لا توجد فروع مسجلة. أضف فرعاً أولاً من الإعدادات.")
            return

        # بناء أزرار الفروع
        keyboard = [[InlineKeyboardButton(f"🏢 {b.get('اسم_الفرع')}", callback_data=f"final_save_reg_{b.get('معرف_الفرع')}")] for b in bot_branches]
        await query.edit_message_text("🎯 <b>بيانات المرشح جاهزة:</b> اختر الفرع لإتمام الاعتماد:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data.startswith("reject_reg_"):
        candidate_id = data.replace("reject_reg_", "")
        await query.edit_message_text("❌ تم رفض الطلب بنجاح.")
        try:
            await context.bot.send_message(chat_id=candidate_id, text="⚠️ نعتذر منك، تم رفض طلب انضمامك للكادر حالياً.")
        except: pass

# --------------------------------------------------------------------------
    # التنفيذ النهائي والحفظ (المحرك الموحد المطور لـ 43 عموداً - دمج المدربين والموظفين)
    elif data.startswith("final_save_reg_"):
        branch_id_selected = data.replace("final_save_reg_", "").strip()
        pending = context.user_data.get('pending_approve')
        reg_info = context.user_data.get('reg_data') # البيانات المستعادة من الذاكرة المؤقتة
        
        # 🛡️ حماية ضد التهنيج: إذا فُقدت البيانات من الرام (بسبب ريستارت أو تأخير)
        if not pending or not reg_info:
            await query.edit_message_text(
                "⚠️ <b>انتهت صلاحية الجلسة:</b>\nعذراً، فُقدت البيانات المؤقتة. يرجى إعادة الضغط على زر (✅ اعتماد) من رسالة الطلب الأصلية لبدء العملية مجدداً.",
                parse_mode="HTML"
            )
            return

        # 🚀 الربط الآلي: جلب تفاصيل الفرع من الذاكرة المركزية RAM لضمان جلب الاسم الصحيح
        from cache_manager import FACTORY_GLOBAL_CACHE
        br_records = FACTORY_GLOBAL_CACHE.get("data", {}).get("إدارة_الفروع", [])
        # تنظيف المعرف لضمان مطابقة دقيقة للأرقام النقية (نظام 1001001)
        branch = next((b for b in br_records if str(b.get("معرف_الفرع")).replace("'", "").strip() == branch_id_selected), {})
        branch_name = branch.get('اسم_الفرع', 'الرئيسي')

        # استعادة اليوزرنيم المحفوظ أو وضع قيمة افتراضية
                # 🚨 التصحيح: سحب اليوزرنيم من قاموس بيانات المرشح المستعادة
        candidate_username = reg_info.get('username', 'بدون')
        
        # 🟢 التعبئة الآلية للرتبة: تحويل نوع الطلب لوسم عربي (مدرب/موظف) للعمود 42
        role_type_ar = "مدرب" if pending['role'] == "coach" else "موظف"


        
        # تنفيذ الحفظ الموحد في ورقة "إدارة_الموظفين" (المحرك الجديد بـ 43 عموداً)
        success = add_new_employee_advanced(
            bot_token=bot_token,
            employee_id=pending['id'],     # ID التيليجرام (العمود 3)
            name=reg_info['name'],         # الاسم الكامل (العمود 5)
            job_title=reg_info['info'],    # التخصص أو المسمى (العمود 11 و 12)
            phone=reg_info['phone'],       # الهاتف (العمود 18)
            branch_id=branch_id_selected,  # معرف الفرع الرقمي (العمود 2)
            branch_name=branch_name,       # اسم الفرع المجلوب آلياً (العمود 43)
            role_tag=role_type_ar,         # الرتبة المحددة آلياً (العمود 42)
            email=reg_info['email'],       # البريد (العمود 21)
            username=candidate_username    # ✅ اليوزرنيم الحقيقي (العمود 41)
        )


        if success:
            await query.edit_message_text(
                f"✅ تم اعتماد <b>{role_type_ar}</b> بنجاح!\n"
                f"🏢 الفرع: {branch_name}\n"
                f"🆔 تم توليد معرف مهني (100) وحفظه في قسم الموظفين.", 
                parse_mode="HTML"
            )
            
            # تحديث نبضة النظام العالمية للمزامنة اللحظية
            update_global_version(bot_token)
            
            try:
                # إشعار الكادر بالقبول النهائي وتحديد دوره وفرعه
                await context.bot.send_message(
                    chat_id=pending['id'], 
                    text=f"🎊 <b>مبروك!</b> تم قبول طلب انضمامك واعتمادك رسمياً كمـ ({role_type_ar}) في المنصة.\n🏢 الفرع المخصص: {branch_name}",
                    parse_mode="HTML"
                )
            except: pass
            
            # تنظيف الذاكرة المؤقتة بعد النجاح لضمان عدم تداخل الطلبات المستقبيلة
            context.user_data.pop('pending_approve', None)
            context.user_data.pop('reg_data', None)
        else:
            await query.answer("❌ فشل الحفظ، تأكد من تحديث هيكل قسم إدارة_الموظفين لـ 43 عموداً.", show_alert=True)
 
# --------------------------------------------------------------------------
    # عودة المالك ومدير النظام
    # تصحيح زر العودة للوحة الإدارة الرئيسية
    elif data in ["back_to_admin", "get_admin_panel"]:
        # قمنا بحذف المتغير current_msg الذي يسبب التعليق ووضعنا نصاً مباشراً
        welcome_text = "<b>مرحباً بك مجدداً في لوحة القيادة 🎓</b>\n\nاختر من الخيارات أدناه لإدارة النظام:"
        await query.edit_message_text(
            text=welcome_text, 
            reply_markup=get_admin_panel(), # استدعاء الدالة التي تحتوي على الأزرار الـ 5
            parse_mode="HTML"
        )


    # عودة الموظف
    elif data == "get_employee_panel":
        text = f"<b>{current_msg}</b>\n\n👨‍🏫 <b>قسم الشؤون التعليمية:</b> ابدأ الإدارة الآن."
        await query.edit_message_text(text, reply_markup=get_employee_panel(), parse_mode="HTML")

    # عودة المدرب
    elif data == "get_coach_panel":
        text = f"<b>{current_msg}</b>\n\n👨‍🏫 <b>الغرفة الأكاديمية:</b> مهامك بانتظارك أيها المدرب."
        await query.edit_message_text(text, reply_markup=get_coach_panel(), parse_mode="HTML")


#ضبط الذكاء الاصطناعي 
    elif data == "setup_ai_start":
        context.user_data['action'] = 'awaiting_institution_name'
        await query.edit_message_text("🤖 <b>إعداد الهوية الذكية:</b>\nيرجى إرسال اسم المنصة التعليمية الآن:",parse_mode="HTML")
# --------------------------------------------------------------------------


# المزامنة
    elif data == "manual_cache_sync":
        await query.edit_message_text("⏳ <b>جاري سحب البيانات  وتحديث الذاكرة المركزية...</b>", parse_mode="HTML")
        from cache_manager import fetch_full_factory_data
        if fetch_full_factory_data(): # استدعاء الدالة الموجودة في ملفك
            await query.message.reply_text("✅ <b>تمت المزامنة بنجاح!</b>\nالبوت الآن يقرأ أحدث البيانات  مباشرة.", parse_mode="HTML")
        else:
            await query.message.reply_text("❌ فشلت المزامنة، يرجى التحقق من سجل الأخطاء.")
 # --------------------------------------------------------------------------
    elif data == "fin_summary":
        # استدعاء المحرك التنفيذي من ملف course_engine
        await course_engine.show_financial_dashboard(update, context)
 #>>>>>>>>>>>>>>>>       
    elif data == "fin_payroll":
        await course_engine.show_payroll_management(update, context)
        
    elif data == "fin_payouts":
        await course_engine.show_marketers_payouts(update, context)
        
    elif data == "fin_settings":
        # استدعاء محرك الضبط من course_engine
        await course_engine.show_financial_settings(update, context)

    elif data == "honors_achievements":
        # عرض لوحة التحكم الموحدة للأوسمة والإنجازات

        await show_honors_main_menu(update, context)




# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
    # عرض قائمة المدربين كأزرار
    elif data == "list_coaches":

        coaches = get_all_coaches(bot_token)
        if not coaches:
            await query.edit_message_text("⚠️ لا يوجد مدربون مسجلون حالياً.", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="manage_coaches")]]))
            return

        keyboard = [[InlineKeyboardButton(f"👤 {c['name']}", callback_data=f"view_coach_{c['id']}")] for c in coaches]
        keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_coaches")])
        await query.edit_message_text("🎯 **اختر مدرباً لعرض تفاصيله أو حذفه:**", reply_markup=InlineKeyboardMarkup(keyboard))
# --------------------------------------------------------------------------
    # عرض تفاصيل مدرب محدد مع زر الحذف
    elif data.startswith("view_coach_"):
        coach_id = data.replace("view_coach_", "")

        coaches = get_all_coaches(bot_token)
        coach = next((c for c in coaches if str(c['id']) == str(coach_id)), None)
        
        if coach:
            text = f"👤 **معلومات المدرب:**\n━━━━━━━━━━━━━━\nالاسم: {coach['name']}\nID: <code>{coach['id']}</code>"
            keyboard = [
                [InlineKeyboardButton("🗑️ حذف المدرب نهائياً", callback_data=f"del_coach_{coach['id']}")],
                [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="list_coaches")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
# --------------------------------------------------------------------------
    # تنفيذ الحذف الفعلي للمدرب
    elif data.startswith("del_coach_"):
        coach_id = data.replace("del_coach_", "")

        if delete_coach_from_sheet(bot_token, coach_id):
            await query.answer("✅ تم حذف المدرب بنجاح", show_alert=True)
            await query.edit_message_text("✅ تم الحذف. هل تريد إدارة مدرب آخر؟", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 عرض القائمة", callback_data="list_coaches")]]))
        else:
            await query.answer("❌ فشل الحذف", show_alert=True)
# --------------------------------------------------------------------------
    elif data == "bulk_add_start":
        context.user_data['action'] = 'awaiting_bulk_courses'
        instruction = (
            "📥 <b>إضافة دورات دفعة واحدة:</b>\n\n"
            "يرجى إرسال الدورات بحيث يكون كل سطر دورة مستقلة بهذا التنسيق:\n"
            "<code>اسم الدورة |  الوصف وعدد الساعات | السعر | معرف المدرب | معرف القسم</code>\n\n"
            "💡 مثال:\n"
            "<code>دورة البرمجة | 40 | 150 | 873158997 | C1234</code>"
        )
        await query.edit_message_text(instruction, parse_mode="HTML")
# --------------------------------------------------------------------------
#دالة إرسال ملف للموظف وانتظار رفعه الى السيرفر 
    elif data == "excel_import_start":
        import pandas as pd
        import io
        
        # تجهيز البيانات العشوائية المترابطة (Sample Data) لضمان فهم الموظف للربط
        # لاحظ أن كلمة "البرمجة" و "أحمد علي" و "بايثون" ستتكرر للربط
        
        sheets_content = {
            "الاقسام": {
                "اسم_القسم": ["البرمجة"],
                "الوصف": ["دورات لغات البرمجة وتطوير المواقع"]
            },
            "المدربين": {
                "اسم_المدرب": ["أحمد علي"],
                "التخصص": ["بايثون"],
                "رقم_الهاتف": ["0501234567"],
                "ID_المدرب": ["873158997"] # معرف تيليجرام
            },
            "الدورات": {
                "الاسم": ["بايثون للمبتدئين"],
                "الوصف": ["دورة شاملة من الصفر - 40 ساعة"],
                "السعر": [150],
                "اسم_المدرب": ["أحمد علي"], # يطابق ورقة المدربين
                "اسم_القسم": ["البرمجة"]      # يطابق ورقة الأقسام
            },
            "المجموعات": {
                "اسم_المجموعة": ["مجموعة التميز A"],
                "اسم_الدورة": ["بايثون للمبتدئين"], # يطابق ورقة الدورات
                "الحد_الأقصى": [25]
            },
            "الموظفين": {
                "اسم_الموظف": ["سارة خالد"],
                "الوظيفة": ["مسؤولة دعم"],
                "رقم_الهاتف": ["0551112223"]
            },
            "قاعده بيانات الطلاب": {
                "الاسم_بالعربي": ["خالد محمد"],
                "رقم_الهاتف": ["0540001112"],
                "البريد_الإلكتروني": ["khaled@test.com"],
                "اسم_الدورة": ["بايثون للمبتدئين"] # يطابق ورقة الدورات
            },
            "الإختبارات الآلية": {
                "اسم_الاختبار": ["اختبار بايثون الأساسي"],
                "اسم_الدورة": ["بايثون للمبتدئين"], # يطابق ورقة الدورات
                "درجة_النجاح": [60]
            },
            "الأسئلة": {
                "اسم_الاختبار": ["اختبار بايثون الأساسي"], # يطابق ورقة الاختبارات
                "نص_السؤال": ["ما هي دالة الطباعة في بايثون؟"],
                "الخيار_1": ["print()"],
                "الخيار_2": ["echo()"],
                "الخيار_3": ["echo()"],
                "الإجابة_الصحيحة": ["print()"]
            },
            "الكوبونات": {
                "كود_الكوبون": ["PROMO2026"],
                "قيمة_الخصم": ["50"],
                "تاريخ_الانتهاء": ["2026-12-31"]
            },
            "اكود الخصم": {
                "كود_الخصم": ["SAVE20"],
                "اسم_الدورة": ["بايثون للمبتدئين"], # يطابق ورقة الدورات
                "قيمة_الخصم": ["20"]
            },
            "الأسئلة الشائعة": {
                "اسم_الدورة": ["بايثون للمبتدئين"], # يطابق ورقة الدورات
                "السؤال": ["هل يوجد شهادة؟"],
                "الإجابة": ["نعم، شهادة معتمدة عند الاجتياز"]
            }
        }

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for name, data in sheets_content.items():
                pd.DataFrame(data).to_excel(writer, index=False, sheet_name=name)
        
        output.seek(0)
        context.user_data['action'] = 'awaiting_excel_file'
        
        caption = (
            "🚀 <b>نموذج الرفع الشامل للمنصة التعليمية:</b>\n\n"
            "يرجى تعبئة الملف مع الالتزام التام بـ <b>قاعدة التطابق:</b>\n"
            "⚠️ <b>الأسماء المرتبطة</b> (مثل اسم الدورة أو القسم) يجب أن ترفع بنفس النص في كافة الأوراق دون اختلاف حرف واحد.\n\n"
            "💡 <b>مثال:</b> إذا سميت القسم 'البرمجة' في ورقة الأقسام، يجب أن تكتبه 'البرمجة' في ورقة الدورات وليس 'برمجة'.\n\n"
            "📦 الملف يحتوي على بيانات تجريبية مترابطة، امسحها وأضف بياناتك الحقيقية."
        )

        await context.bot.send_document(chat_id=query.message.chat_id, document=output, 
                                        filename="نموذج_إدارة_المنصة_الشامل.xlsx", caption=caption, parse_mode="HTML")



    elif data == "csv_import_start":
        context.user_data['action'] = 'awaiting_csv_file'
        await query.edit_message_text("📄 أرسل ملف CSV الآن:")

    elif data == "sheet_link_import":
        context.user_data['action'] = 'awaiting_sheet_link'
        await query.edit_message_text("🔗 أرسل رابط Google Sheet المفتوح للمشاركة:")

# --------------------------------------------------------------------------
# إضافة مدرب
    elif data == "start_add_coach":
        context.user_data['action'] = 'await_coach_name'
        await query.edit_message_text("✍️ <b>الخطوة 1:</b> أرسل اسم المدرب الثلاثي:", parse_mode="HTML")

    elif data == "confirm_save_coach":

        c = context.user_data.get('temp_coach')
        
        if not c:
            await query.edit_message_text("⚠️ خطأ: تعذر العثور على البيانات، يرجى إعادة المحاولة من جديد.")
            return

        success = add_new_coach_advanced(
            bot_token=bot_token,
            coach_id=c['id'],
            name=c['name'],
            specialty=c['spec'],
            phone=c['phone'],
            notes="تمت الإضافة عبر لوحة التحكم"
        )
        
        if success:
            await query.edit_message_text(
                f"✅ <b>تم تسجيل المدرب بنجاح!</b>\n\n"
                f"👤 الاسم: {c['name']}\n"
                f"🎓 التخصص: {c['spec']}\n"
                f"🆔 المعرف: <code>{c['id']}</code>",
                reply_markup=get_admin_panel(),
                parse_mode="HTML"
            )
            context.user_data.pop('temp_coach', None)
        else:
            await query.edit_message_text("❌ حدث خطأ تقني أثناء الحفظ في قاعدة البيانات.")

# --------------------------------------------------------------------------
    # --- 4. معالجة القسم المختار للدورة وبدء طلب الاسم ---
    elif data.startswith("set_crs_cat_"):
        cat_id = data.replace("set_crs_cat_", "")
        context.user_data['temp_crs_cat'] = cat_id
        context.user_data['action'] = 'awaiting_crs_name'
        await query.edit_message_text("✍️ **الخطوة 2:** أرسل اسم الدورة الآن:")

    elif data == "confirm_save_full_crs":

        import uuid
        
        # 1. جلب البيانات المؤقتة (البيانات + معرف القسم)
        d = context.user_data.get('temp_crs')
        cat_id = context.user_data.get('temp_crs_cat') # هذا هو الرابط الضروري
        
        if not d or not cat_id:
            await query.edit_message_text("⚠️ خطأ: تعذر العثور على البيانات المؤقتة، يرجى المحاولة مجدداً.")
            return

        # 2. توليد معرف فريد للدورة
        c_id = f"CRS{str(uuid.uuid4().int)[:4]}"
        
        # 3. تنفيذ الحفظ الفعلي (إرسال الـ 17 متغير بالترتيب)
        success = add_new_course(
            bot_token,          # 1
            c_id,               # 2
            d['name'],          # 3
            d['hours'],         # 4
            d['start_date'],    # 5
            "",                 # 6
            "أونلاين",          # 7
            d['price'],         # 8
            "100",              # 9
            "لا يوجد",          # 10
            "إدارة المنصة",      # 11
            "ADMIN01",          # 12
            "عام",              # 13
            d['coach_user'],    # 14
            d['coach_id'],      # 15
            d['coach_name'],    # 16
            cat_id              # 17. تم إضافة معرف القسم هنا بنجاح!
        )
        
        if success:
            await query.edit_message_text(
                f"✅ <b>تم اعتماد الدورة وحفظها بنجاح!</b>\n\n"
                f"🆔 المعرف: <code>{c_id}</code>\n"
                f"📂 القسم: <code>{cat_id}</code>\n"
                f"👨‍🏫 المدرب: {d['coach_name']}",
                reply_markup=get_admin_panel(),
                parse_mode="HTML"
            )
            # تنظيف الذاكرة بعد النجاح
            context.user_data.pop('temp_crs', None)
            context.user_data.pop('temp_crs_cat', None)
        else:
            await query.edit_message_text("❌ فشل الحفظ في قاعدة البيانات، تأكد من إعدادات دالة add_new_course.")

    # --- 5. إدارة الأقسام (عرض القائمة) ---
    # إضافة معالج زر إدارة الأقسام
    elif data == "manage_cats":

        await manage_categories_main(update, context)


        # نتحقق هل لديه صلاحية الأقسام؟
        if not check_user_permission(bot_token, user_id, "صلاحية_الأقسام"):
            await query.answer("🚫 ليس لديك صلاحية لإدارة الأقسام.", show_alert=True)
            return
            
        # إذا كان لديه صلاحية، يكمل الكود الطبيعي...
   
        categories = get_all_categories(bot_token)
        # ... بقية الكود

# --------------------------------------------------------------------------
    # --- [ 1. معالج إدارة الدورات الديناميكي بناءً على الرتبة ] ---
    elif data in ["manage_courses", "manage_courses_employee", "manage_courses_coach"]:
        keyboard = []
        # تحديد المسار لضمان العودة الصحيحة لاحقاً
        context.user_data['entry_point'] = data
        
        if data == "manage_courses":
            text = "📚 <b>إدارة واستيراد الدورات (المالك):</b>\n\nاختر الطريقة التي تفضلها لإضافة البيانات:"
            keyboard = [
                [InlineKeyboardButton("➕ إضافة دورة فردية", callback_data="start_add_course")],
                [InlineKeyboardButton("📥 نصية (|)", callback_data="bulk_add_start")],
                [InlineKeyboardButton("📄 ملف CSV", callback_data="csv_import_start"),
                 InlineKeyboardButton("🔗 رابط Google Sheet", callback_data="sheet_link_import")],
                [InlineKeyboardButton("🔙 عودة", callback_data="back_to_admin")]
            ]

        elif data == "manage_courses_employee":
            text = "👨‍🏫 <b>إدارة الدورات (الموظف):</b>\n\nيمكنك إضافة دورات جديدة أو استعراض القائمة الحالية."
            keyboard = [
                [InlineKeyboardButton("➕ إضافة دورة فردية", callback_data="start_add_course")],
                [InlineKeyboardButton("📋 عرض الدورات", callback_data="view_courses_admin")],
                [InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]
            ]

        elif data == "manage_courses_coach":
            text = "👨‍🏫 <b>غرفة المدرب الأكاديمية:</b>\n\nيمكنك استعراض دوراتك أو طلب إضافة دورة جديدة."
            keyboard = [
                [InlineKeyboardButton("📚 عرض دوراتي المتاحة", callback_data="manage_courses")], 
                [InlineKeyboardButton("📝 طلب إضافة دورة جديدة", callback_data="request_course_coach")],
                [InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]
            ]

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # --- [ 2. معالج بدء إضافة دورة (نسخة مسرعة بنظام الكاش + فرز الرتب) ] ---
    elif data == "start_add_course":
        # إرسال إشارة استجابة فورية للتطبيق لإزالة حالة الانتظار من الزر
        await query.answer("⌛ جاري تجهيز القائمة...")
        
        # استدعاء دوال المزامنة والذاكرة المؤقتة لضمان السرعة وعدم ثقل الزر

        
        # إجراء فحص المزامنة الصامت
        
        
        # سحب الأقسام من الرام (RAM) مباشرة
        records = get_bot_data_from_cache(bot_token, "الأقسام")
        categories = [{"id": r.get("معرف_القسم"), "name": r.get("اسم_القسم")} for r in records]
        
        # تحديد زر العودة بناءً على نقطة الدخول المحفوظة
        back_call = context.user_data.get('entry_point', 'manage_courses')
        
        # التحقق من الرتبة (إدمن أم لا) لتخصيص الرد
        config = get_bot_config(bot_token)
        bot_owner_id = int(config.get("admin_ids", 0))
        
        if not categories:
            if user_id == bot_owner_id:
                # رد خاص بالإدمن (يسمح له بإضافة قسم)
                text = "⚠️ <b>تنبيه:</b> لا توجد أقسام تعليمية مضافة حالياً.\nيرجى إضافة قسم أولاً لتتمكن من إدراج الدورات تحته."
                kb = [[InlineKeyboardButton("➕ إضافة قسم جديد", callback_data="add_cat_start")],
                      [InlineKeyboardButton("🔙 عودة", callback_data=back_call)]]
            else:
                # رد خاص بالموظف/المدرب (يوجهه للتواصل مع الإدارة)
                text = "⚠️ <b>عذراً:</b> لا توجد أقسام متاحة حالياً.\nيرجى التواصل مع الإدارة لتفعيل الأقسام وتحديد الصلاحيات."
                kb = [[InlineKeyboardButton("🔙 عودة", callback_data=back_call)]]
                
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
            return

        # بناء لوحة المفاتيح ديناميكياً من بيانات الكاش في حال وجود أقسام
        keyboard = [[InlineKeyboardButton(f"📁 {cat['name']}", callback_data=f"set_crs_cat_{cat['id']}")] for cat in categories]
        keyboard.append([InlineKeyboardButton("❌ إلغاء", callback_data=back_call)])
        
        await query.edit_message_text("🎯 **الخطوة 1:** اختر القسم المخصص للدورة:", 
                                     reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # --- [ 3. معالجة اختيار المدرب من القائمة (نسخة محسنة الأداء) ] ---
    elif data.startswith("sel_coach_for_crs_"):
        coach_id = data.replace("sel_coach_for_crs_", "")
        

        smart_sync_check(bot_token)
        coaches_records = get_bot_data_from_cache(bot_token, "المدربين")
        
        # البحث عن المدرب في الكاش
        coach = next((c for c in coaches_records if str(c.get('ID')) == str(coach_id)), None)
        
        if coach:
            if 'temp_crs' not in context.user_data:
                context.user_data['temp_crs'] = {}
                
            context.user_data['temp_crs'].update({
                'coach_user': "اختيار من القائمة",
                'coach_id': coach.get('ID'),
                'coach_name': coach.get('اسم_المدرب')
            })
            
            context.user_data['action'] = 'awaiting_crs_date'
            await query.edit_message_text(
                f"✅ تم اختيار المدرب: <b>{coach.get('اسم_المدرب')}</b>\n\n"
                f"🗓 <b>الخطوة 6:</b> أرسل الآن تاريخ بداية الدورة (مثلاً: 2026/05/01):",
                parse_mode="HTML"
            )
        else:
            await query.answer("⚠️ عذراً، تعذر العثور على بيانات هذا المدرب.", show_alert=True)



########


    elif data.startswith("edit_cat_"):
        cat_id = data.replace("edit_cat_", "")
        context.user_data['selected_cat_id'] = cat_id
        
        keyboard = [
            [InlineKeyboardButton("📝 تعديل اسم القسم", callback_data="rename_cat")],
            [InlineKeyboardButton("📚 عرض دورات القسم", callback_data=f"view_crs_in_{cat_id}")],
            [InlineKeyboardButton("🗑️ حذف القسم", callback_data="confirm_delete_cat")],
            [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="manage_cats")]
        ]
        await query.edit_message_text(
            f"🛠 <b>إدارة القسم:</b>\n🆔 المعرف: <code>{cat_id}</code>\n\nاختر الإجراء المطلوب:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

    elif data.startswith("view_crs_in_"):
        cat_id = data.replace("view_crs_in_", "")

        courses = get_courses_by_category(bot_token, cat_id)
        
        keyboard = []
        if courses:
            for crs in courses:
                keyboard.append([InlineKeyboardButton(f"📖 {crs['name']}", callback_data=f"manage_crs_{crs['id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 عودة للقسم", callback_data=f"edit_cat_{cat_id}")])
        
        await query.edit_message_text(
            f"📚 <b>الدورات التابعة للقسم {cat_id}:</b>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

    elif data == "add_cat_start":
        context.user_data['action'] = 'awaiting_cat_name'
        await query.edit_message_text(
            "✍️ <b>إضافة قسم جديد:</b>\n\nيرجى إرسال اسم القسم الذي تريد إنشاءه الآن:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="manage_cats")]]),
            parse_mode="HTML"
        )

    elif data == "confirm_delete_cat":
        keyboard = [
            [InlineKeyboardButton("✅ نعم، احذف", callback_data="exec_delete_cat")],
            [InlineKeyboardButton("❌ تراجع", callback_data="manage_cats")]
        ]
        await query.edit_message_text(
            "⚠️ <b>تنبيه هام!</b>\nهل أنت متأكد من حذف هذا القسم؟",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

    elif data == "exec_delete_cat":
        cat_id = context.user_data.get('selected_cat_id')
        if delete_category_by_id(bot_token, cat_id):
            await query.edit_message_text("✅ <b>تم حذف القسم بنجاح!</b>", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة للأقسام", callback_data="manage_cats")]]), parse_mode="HTML")
        else:
            await query.edit_message_text("❌ حدث خطأ أثناء محاولة الحذف.")

    elif data == "rename_cat":
        context.user_data['action'] = 'awaiting_new_cat_name'
        await query.edit_message_text(
            "📝 <b>تعديل اسم القسم:</b>\nيرجى إرسال الاسم الجديد الآن:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء", callback_data="manage_cats")]]),
            parse_mode="HTML"
        )

    elif data.startswith("manage_crs_"):
        course_id = data.replace("manage_crs_", "")
        context.user_data['selected_course_id'] = course_id
        
        keyboard = [
            [InlineKeyboardButton("🗑️ حذف هذه الدورة", callback_data="confirm_delete_crs")],
            [InlineKeyboardButton("🔙 عودة لقائمة الدورات", callback_data="manage_courses")]
        ]
        await query.edit_message_text(f"📖 <b>إدارة الدورة:</b>\n🆔 المعرف: <code>{course_id}</code>", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data == "confirm_delete_crs":
        keyboard = [
            [InlineKeyboardButton("✅ نعم، احذف الدورة", callback_data="exec_delete_crs")],
            [InlineKeyboardButton("❌ تراجع", callback_data="manage_courses")]
        ]
        await query.edit_message_text("⚠️ <b>تأكيد الحذف:</b>\nهل أنت متأكد من حذف هذه الدورة؟", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data == "exec_delete_crs":
        course_id = context.user_data.get('selected_course_id')
        if delete_course_by_id(bot_token, course_id):
            await query.edit_message_text("✅ <b>تم حذف الدورة بنجاح!</b>", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة", callback_data="manage_courses")]]), parse_mode="HTML")

    elif data == "smart_broadcast":
        await query.edit_message_text("📡 <b>الإذاعة الذكية:</b>\n\n"
                                   "💡 هذه هي الإذاعة الذكية حيث يمكنك إرسال رسائل جماعية إلى مشتركيك.\n"
                                   "📊 يمكنك إرسال رسائل إلى جميع المشتركين أو إلى مشتركي دورة معينة أو مجموعة معينة.\n"
                                   "🔍 يمكنك استخدم الإذاعة الذكية لترسل رسائل ترويجية أو إعلامية أو تعليمية إلى مشتركيك.\n\n"
                                   "🔝اختر خيارًا لإرسال رسالتك:",
            reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 للكل", callback_data="bc_all"), 
             InlineKeyboardButton("🎓 لمشتركي دورة", callback_data="bc_course"), 
             InlineKeyboardButton("🎓 لمشتركي مجموعة", callback_data="bc_group")],
            [InlineKeyboardButton("🔙 العودة", callback_data="main_menu")]
        ]), parse_mode="HTML")
#>>>>>>>>>>>>>>>>
    # --- [ لوحة المفاتيح الذكية للآدمن ] ---
    elif data == "tech_settings":
        config = get_bot_config(bot_token)
        m_status = "🔴 (نشط)" if str(config.get("maintenance_mode", "FALSE")).upper() == "TRUE" else "🟢 (متوقف)"    	
        keyboard = [
            [
                InlineKeyboardButton("📝 كليشة الترحيب", callback_data="manage_welcome_texts"),
                InlineKeyboardButton("🔄 المزامنة", callback_data="manual_cache_sync")
            ],
            [
                InlineKeyboardButton(f"🛠 وضع الصيانة {m_status}", callback_data="toggle_maintenance")
            ],
            [
                InlineKeyboardButton("إدارة الفروع", callback_data="manage_branches"),
                InlineKeyboardButton("الإدارة المالية", callback_data="manage_financial"),
                InlineKeyboardButton("الكنترول", callback_data="manage_control")
            ],
            [
               InlineKeyboardButton("📊  استيراد Excel", callback_data="excel_import_start"),
               InlineKeyboardButton("📊  تصدير Excel", callback_data="excel_export_start")
            ],
            [InlineKeyboardButton("الأوسمة والإنجازات", callback_data="honors_achievements")], 
            [
                InlineKeyboardButton("👨‍🏫 الصلاحيات", callback_data="manage_personnel"),
                InlineKeyboardButton("تكويد الكادر", callback_data="manage_coaches"), 
                InlineKeyboardButton("المهام الإدارية", callback_data="administrative_tasks")
            ],
            [
                InlineKeyboardButton("📁 إدارة الأقسام", callback_data="manage_cats"),
                InlineKeyboardButton("جداول المحاضرات", callback_data="schedules_lectures"),
                InlineKeyboardButton("📚 إدارة الدورات", callback_data="manage_courses")
            ],
            [
                InlineKeyboardButton("إدارة المجموعات", callback_data="manage_group"),
                InlineKeyboardButton("المكتبة الشاملة", callback_data="manage_library"),
                InlineKeyboardButton("الأسئلة الشائعة", callback_data="frequently_guestions")
            ],
            [
                InlineKeyboardButton("🎟 الكوبونات", callback_data="manage_coupons"),
                InlineKeyboardButton("📢 الإعلانات", callback_data="manage_ads"),
                InlineKeyboardButton("أكواد الخصم", callback_data="discount_codes")
            ],
            [
                InlineKeyboardButton("ضبط نقاط الدخول", callback_data="referral_points_settings"), 
                InlineKeyboardButton("ضبط وحدة العملة", callback_data="currency_unit")
            ],
            [
                InlineKeyboardButton(f"ضبط درجة النجاح", callback_data="passing_grade"),            
                InlineKeyboardButton("ضبط درجة الواجبات", callback_data="homework_grade")
            ],
            [
                InlineKeyboardButton("ضبط مبلغ السحب", callback_data="minimum_withdrawal_amount"),
                InlineKeyboardButton("معلومات الدفع الافتراضية", callback_data="default_payment_information"),

            ],
            [
                InlineKeyboardButton("القناة الرسمية", callback_data="public_channel_idd"),
                InlineKeyboardButton("قناة الأوسمة والإنجازات", callback_data="honors_channel_idd"),

            ],  
            [InlineKeyboardButton("ضبط عمولة المسوقين %", callback_data="percentage_marketers")],                                                
            [InlineKeyboardButton("🔙 عودة", callback_data="back_to_admin")]
        ]

        await query.edit_message_text("👨‍🏫 <b>إدارة الشؤون التعليمية :</b>\nيمكنك إضافة مدربين جدد دورات جديدة او اقسام او مجموعات أو استعراض القائمة الحالية للحذف.", 
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
#>>>>>>>>>>>>>>>
    # --- [ إضافة معالج الإكسل الناقص ] ---
    elif data == "excel_export_start":
        from cache_manager import export_bot_data_to_excel
        from datetime import datetime
        
        # استدعاء الدالة التي أضفتها أنت في ملف الكاش
        excel_file, status = export_bot_data_to_excel(bot_token)
        
        if status == "success":
            await query.answer("جاري التصدير... ✅")
            timestamp = datetime.now().strftime("%Y-%m-%d")
            await query.message.reply_document(
                document=excel_file,
                filename=f"Data_Backup_{timestamp}.xlsx",
                caption="📊 **تم تصدير نسخة البيانات كاملة من الكاش.**"
            )
        else:
            # رسالة الرفض التسويقية في حال كانت الميزة FALSE
            await query.answer("🚫 الميزة غير مفعلة", show_alert=True)
            await query.message.reply_text(f"{status}\n\n💡 تواصل مع المطور لتفعيل الميزة.")

    elif data == "excel_import_start":
        from cache_manager import check_excel_permission_from_cache
        if not check_excel_permission_from_cache(bot_token):
             await query.answer("🚫 غير مصرح", show_alert=True)
             await query.message.reply_text("⚠️ هذه الميزة مخصصة للباقات المتقدمة.")
             return
        await query.answer()
        await query.edit_message_text("📥 يرجى رفع ملف Excel الآن لبدء الاستيراد.")
        context.user_data['action'] = 'awaiting_excel_file'





    elif data == "passing_grade":
        keyboard = [
            [InlineKeyboardButton("درجة النجاح الصغرى", callback_data="minimum_passing_grade")],
            [InlineKeyboardButton("درجة النجاح الكبر", callback_data="greatest_success_grade")],
            [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
        ]
        await query.edit_message_text("💰 <b>درجات النجاح الافتراضية :</b>\nيرجى ضبط اعدادات الدراجات لإعتمادها في الاختبارات .", 
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

#>>>>>>>>>>>>>>>>#>>>>>>>>>>>>>>>>
    elif data == "manage_financial":
        keyboard = [
            [InlineKeyboardButton("📊 تقرير الأرباح والخزينة", callback_data="fin_summary")],
            [InlineKeyboardButton("👔 كشوف المرتبات (الكادر)", callback_data="fin_payroll")],
            [InlineKeyboardButton("💸 طلبات سحب المسوقين", callback_data="fin_payouts")],
            [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
        ]
        await query.edit_message_text("💰 <b>وحدة الإدارة المالية:</b>\nيرجى اختيار القسم المطلوب للمراجعة أو الصرف.", 
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


#>>>>>>>>>>>>>>>>
    elif data == "manage_welcome_texts":
        keyboard = [
            [InlineKeyboardButton("🌅 الصباحية", callback_data="edit_welcome_morning"), InlineKeyboardButton("☀️ الظهرية", callback_data="edit_welcome_noon")],
            [InlineKeyboardButton("🌆 المسائية", callback_data="edit_welcome_evening"), InlineKeyboardButton("🌃 الليلية", callback_data="edit_welcome_night")],
            [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
        ]
        await query.edit_message_text("🖼 <b>تعديل كليشات الترحيب:</b>\nاختر الفترة التي تريد تغيير رسالتها:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data.startswith("edit_welcome_"):
        period = data.replace("edit_welcome_", "")
        context.user_data['edit_period'] = period
        context.user_data['action'] = 'awaiting_new_welcome_text'
        periods_ar = {"morning": "الصباحية", "noon": "الظهرية", "evening": "المسائية", "night": "الليلية"}
        await query.edit_message_text(f"✍️ <b>تعديل الرسالة {periods_ar[period]}:</b>\n\nأرسل الآن النص الجديد (يمكنك استخدام HTML):")
# --------------------------------------------------------------------------
    elif data == "manage_branches":
        keyboard = [
            [InlineKeyboardButton("🏢 عرض قائمة الفروع", callback_data="list_branches")],
            [InlineKeyboardButton("➕ إضافة فرع جديد", callback_data="add_branch_start")],
            [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
        ]
        await query.edit_message_text(
            "🏢 <b>إدارة فروع المنصة:</b>\n\nيمكنك استعراض الفروع الحالية أو إضافة فرع جديد للنظام.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

    # بدء عملية الإضافة عند الضغط على الزر الجديد
    elif data == "add_branch_start":
        context.user_data['action'] = 'awaiting_branch_name'
        await query.edit_message_text("🏢 <b>إضافة فرع جديد:</b>\n\nيرجى إرسال <b>اسم الفرع</b> الآن:", parse_mode="HTML")

    # عرض قائمة الفروع من الكاش
    elif data == "list_branches":
        # جلب البيانات من الذاكرة المركزية RAM حصراً لسرعة الاستجابة ومنع حظر جوجل
        from cache_manager import FACTORY_GLOBAL_CACHE
        all_records = FACTORY_GLOBAL_CACHE.get("data", {}).get("إدارة_الفروع", [])
        
        # تصفية الفروع التابعة لهذا البوت فقط من مصفوفة الذاكرة
        branches = [r for r in all_records if str(r.get("bot_id")) == str(bot_token)]
        
        if not branches:
            await query.edit_message_text(
                "⚠️ لا توجد فروع مسجلة في الذاكرة حالياً.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="manage_branches")]])
            )
            return

        # بناء قائمة الأزرار باستخدام أسماء ومعرفات الفروع من الرام مباشرة
        keyboard = [[InlineKeyboardButton(f"🏢 {b.get('اسم_الفرع')}", callback_data=f"view_br_{b.get('معرف_الفرع')}")] for b in branches]
        keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_branches")])
        
        await query.edit_message_text("📋 <b>قائمة الفروع الحالية :</b>", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # عرض تفاصيل فرع محدد (بعد الضغط على اسمه من القائمة)
    elif data.startswith("view_br_"):
        branch_id = data.replace("view_br_", "")
        from cache_manager import FACTORY_GLOBAL_CACHE
        records = FACTORY_GLOBAL_CACHE.get("data", {}).get("إدارة_الفروع", [])
        branch = next((b for b in records if str(b.get("معرف_الفرع")) == branch_id and str(b.get("bot_id")) == str(bot_token)), None)
        
        if branch:
            text = (
                f"🏢 <b>تفاصيل الفرع:</b>\n━━━━━━━━━━━━━━\n"
                f"🆔 المعرف: <code>{branch.get('معرف_الفرع')}</code>\n"
                f"🏢 الاسم: {branch.get('اسم_الفرع')}\n"
                f"🌍 الدولة: {branch.get('الدولة')}\n"
                f"👤 المدير: {branch.get('المدير_المسؤول')}\n"
                f"💰 العملة: {branch.get('العملة')}\n"
            )
            keyboard = [
                [InlineKeyboardButton("📝 تعديل الاسم", callback_data=f"edit_br_name_{branch_id}")],
                [InlineKeyboardButton("🗑️ حذف الفرع", callback_data=f"conf_del_br_{branch_id}")],
                [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="list_branches")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # تأكيد الحذف
    elif data.startswith("conf_del_br_"):
        b_id = data.replace("conf_del_br_", "")
        keyboard = [
            [InlineKeyboardButton("✅ نعم، احذف نهائياً", callback_data=f"exec_del_br_{b_id}")],
            [InlineKeyboardButton("❌ تراجع", callback_data=f"view_br_{b_id}")]
        ]
        await query.edit_message_text("⚠️ <b>تحذير:</b> هل أنت متأكد من حذف هذا الفرع؟ لا يمكن التراجع.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # التنفيذ الفعلي للحذف
    elif data.startswith("exec_del_br_"):
        b_id = data.replace("exec_del_br_", "")
        if delete_branch_db(bot_token, b_id):
            await query.answer("🗑️ تم حذف الفرع بنجاح", show_alert=True)
            await start_handler(update, context) # العودة للرئيسية
        else:
            await query.answer("❌ فشل الحذف")

    # بدء تعديل الاسم
    elif data.startswith("edit_br_name_"):
        b_id = data.replace("edit_br_name_", "")
        context.user_data['edit_br_id'] = b_id
        context.user_data['action'] = 'awaiting_new_branch_name'
        await query.edit_message_text("📝 أرسل الآن <b>الاسم الجديد</b> للفرع:")

#>>>>>>>>>>>>>>>>
#إعداد ضبط نقاط الإحالة 
    elif data == "referral_points_settings":
        await query.edit_message_text(
            "أضبط نقاط الإحالة",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("ضبط نقاط الاحالة", callback_data="entry_points_settings")],
                [InlineKeyboardButton("ضبط نقاط التسجيل", callback_data="registration_points_settings")],
                [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
            ]), parse_mode="HTML"
        )
   #®®®®®®®®®®
#عند الضغط على زر معلومات الدفع
    elif data == "default_payment_information":

        await set_default_payment_flow(update, context)
#~~~~~~~~~~~~~~~~
# عنج الضغط على رز درجة الواجبات 
    elif data == "homework_grade":

        await set_homework_grade_flow(update, context)

#~~~~~~~~~~~~~~~~
# عند الضغط على رز ضبط وحدة العملة
    elif data == "currency_unit":

        await set_currency_unit_flow(update, context)
#~~~~~~~~~~~~~~~~
# عند الضغط على رز ضبط نقاط الاحالة
    elif data == "entry_points_settings":

        await set_ref_points_join_flow(update, context)
# عند الضغط على رز ضبط نقاط التسجيل 
    elif data == "registration_points_settings":

        await set_ref_points_purchase_flow(update, context)

#~~~~~~~~~~~~~~~~
# عند الضغط على رز الحد الأدنى للسحب الأرباح للمسوقين
    elif data == "minimum_withdrawal_amount":
        
        await set_min_payout_flow(update, context)

#~~~~~~~~~~~~~~~~
# ضبط درجات النجاح الصغرى والكبرى
    elif data == "minimum_passing_grade":
        
        await set_min_passing_grade_flow(update, context)

    elif data == "greatest_success_grade":
        
        await set_max_passing_grade_flow(update, context)


#~~~~~~~~~~~~~~~~
# ضبط عمولات المسوقين
    elif data == "percentage_marketers":
        
        await set_marketers_commission_flow(update, context)

#~~~~~~~~~~~~~~~~
#ضبط زر 📢 الإعلانات
#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~

# --------------------------------------------------------------------------
    # --- [ قسم إدارة الكنترول والاختبارات ] ---
    
    # 1. الدخول لغرفة الكنترول الرئيسية
    # --- [ الخطوة الثانية: معالجة الدخول والعودة الذكية ] ---

    # 1. إدارة الكنترول التعليمي (مدير، مدرب، موظف)
    if data == "manage_control":
        await manage_control_ui(update, context)
        return

    # 2. إدارة الاختبارات وبنك الأسئلة
    elif data == "manage_quizzes":
        await quiz_create_start_ui(update, context)
        return

    elif data == "manage_q_bank":
        await q_bank_manager_ui(update, context)
        return

    # 3. معالجة عمليات بنك الأسئلة (إضافة/استعراض)
    elif data == "add_q_manual":
        await start_add_question_ui(update, context)
        return

    elif data == "browse_q_bank":
        await browse_q_bank_ui(update, context)
        return

    # 4. ممرات العودة للوحات الفرعية
    elif data == "get_employee_panel":
        text = "👨‍🏫 <b>إدارة الشؤون التعليمية :</b>\nيمكنك إضافة مدربين جدد دورات جديدة او اقسام او مجموعات أو استعراض القائمة الحالية للحذف."
        await query.edit_message_text(text, reply_markup=get_employee_panel(), parse_mode="HTML")
        return

    elif data == "get_coach_panel":
        text = "👨‍🏫 <b>غرفة الإدارة الأكاديمية (المدرب):</b>\nمرحباً بك! يمكنك إدارة مجموعاتك، متابعة طلابك، وتصحيح الواجبات من هنا."
        await query.edit_message_text(text, reply_markup=get_coach_panel(), parse_mode="HTML")
        return

    elif data.startswith("q_gen_crs_"):
        course_id = data.replace("q_gen_crs_", "")

        await quiz_gen_select_groups_ui(update, context, course_id)

    elif data.startswith("q_gen_grp_"):
        parts = data.split("_")
        g_id = parts[3]
        course_id = parts[4]
        
        if g_id == "ALL":
            context.user_data['temp_quiz']['target_groups'] = ["ALL"]
        else:
            if "ALL" in context.user_data['temp_quiz']['target_groups']:
                context.user_data['temp_quiz']['target_groups'].remove("ALL")
            
            if g_id in context.user_data['temp_quiz']['target_groups']:
                context.user_data['temp_quiz']['target_groups'].remove(g_id)
            else:
                context.user_data['temp_quiz']['target_groups'].append(g_id)
        

        await quiz_gen_select_groups_ui(update, context, course_id)

    elif data == "q_gen_next_settings":
        if not context.user_data.get('temp_quiz', {}).get('target_groups'):
            await query.answer("⚠️ يرجى اختيار مجموعة واحدة على الأقل!", show_alert=True)
            return
        context.user_data['action'] = 'awaiting_quiz_title'
        await query.edit_message_text("🏷 <b>الخطوة 3:</b> أرسل <b>عنواناً للاختبار</b> (مثلاً: اختبار نهاية الفصل الأول):",parse_mode="HTML")
# --------------------------------------------------------------------------


    # 2. الدخول لبنك الأسئلة
    elif data == "manage_q_bank":

        await q_bank_manager_ui(update, context)
        #استيراد الأسئلة 
    elif data == "import_q_excel":
        import pandas as pd
        import io
        
        # 1. تجهيز بيانات النموذج الإرشادي للأسئلة
        q_sample = {
            'نص السؤال': ['مثال: ما هو عاصمة اليمن؟'],
            'A': ['صنعاء'],
            'B': ['عدن'],
            'C': ['تعز'],
            'D': ['إب'],
            'الإجابة الصحيحة': ['A'],
            'الدرجة': [1],
            'الصعوبة': ['متوسط'],
            'معرف الدورة': ['أدخل هنا ID الدورة']
        }
        
        # 2. إنشاء ملف Excel في الذاكرة (Buffer)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pd.DataFrame(q_sample).to_excel(writer, index=False, sheet_name='الأسئلة')
        output.seek(0)
        
        # 3. تحديث حالة البوت لانتظار الملف
        context.user_data['action'] = 'awaiting_q_excel'
        
        caption = (
            "📥 <b>نظام استيراد الأسئلة الذكي:</b>\n\n"
            "1️⃣ قمت بإرفاق <b>نموذج Excel</b> جاهز لك.\n"
            "2️⃣ يرجى تعبئة أسئلتك في ورقة <b>(الأسئلة)</b> بنفس الترتيب.\n"
            "3️⃣ تأكد من كتابة حرف الإجابة الصحيحة (A, B, C, D) فقط.\n\n"
            "⚠️ بعد الانتهاء، أرسل الملف هنا بصيغة <b>.xlsx</b> ليتم رفعه للبنك."
        )

        # إرسال الملف مع الشرح
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=output,
            filename="نموذج_استيراد_الأسئلة.xlsx",
            caption=caption,
            parse_mode="HTML"
        )

    elif data == "browse_q_bank":

        await browse_q_bank_ui(update, context)

    elif data.startswith("view_q_det_"):
        q_id = data.replace("view_q_det_", "")

        await view_question_details_ui(update, context, q_id)

    elif data.startswith("exec_del_q_"):
        q_id = data.replace("exec_del_q_", "")

        if delete_question_from_bank(bot_token, q_id):
            await query.answer("🗑️ تم حذف السؤال من البنك بنجاح", show_alert=True)

            await browse_q_bank_ui(update, context)
        else:
            await query.answer("❌ فشل حذف السؤال.")

#ربط  اضافة السؤال اليدوي
    elif data == "add_q_manual":

        await start_add_question_ui(update, context)

    elif data.startswith("sel_q_crs_"):
        course_id = data.replace("sel_q_crs_", "")
        # تخزين معرف الدورة لبدء تسلسل الأسئلة
        context.user_data['temp_q'] = {'course_id': course_id}
        context.user_data['action'] = 'awaiting_q_text'
        await query.edit_message_text("✍️ <b>الخطوة 2:</b> أرسل الآن <b>نص السؤال</b> الذي تود إضافته:",parse_mode="HTML")
     # معالجة اختيار الإجابة الصحيحة
    elif data.startswith("set_q_ans_"):
        ans = data.replace("set_q_ans_", "")
        context.user_data['temp_q']['correct'] = ans
        context.user_data['action'] = 'awaiting_q_grade'
        await query.edit_message_text(f"✅ تم تحديد الإجابة الصحيحة: <b>({ans})</b>\n\n🎯 <b>الخطوة 8:</b> أرسل <b>درجة السؤال</b> (أرقام فقط، مثلاً: 5):", parse_mode="HTML")

    # معالجة اختيار مستوى الصعوبة وعرض المراجعة النهائية
    elif data.startswith("set_q_lv_"):
        lv = data.replace("set_q_lv_", "")
        context.user_data['temp_q']['level'] = lv
        q = context.user_data['temp_q']
        summary = (
            f"📝 <b>مراجعة السؤال قبل الحفظ النهائي:</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"📚 الدورة: <code>{q['course_id']}</code>\n"
            f"❓ السؤال: {q['text']}\n"
            f"✅ الإجابة الصحيحة: {q['correct']}\n"
            f"🎯 الدرجة: {q['grade']} | 📊 المستوى: {lv}\n"
            f"━━━━━━━━━━━━━━\n"
            f"هل تريد تأكيد الحفظ في بنك الأسئلة؟"
        )
        keyboard = [
            [InlineKeyboardButton("✅ نعم، احفظ الآن", callback_data="exec_save_question")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="manage_q_bank")]
        ]
        await query.edit_message_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # التنفيذ الفعلي للحفظ في القاعدة
    elif data == "exec_save_question":
        import uuid
        q_data = context.user_data.get('temp_q')
        
        if not q_data:
            await query.answer("⚠️ حدث خطأ، فقدت البيانات المؤقتة.")
            return

        # توليد معرف فريد للسؤال
        q_data['q_id'] = f"Q{str(uuid.uuid4().int)[:5]}"
        q_data['creator_id'] = str(user_id)
        
        from sheets import add_question_to_bank
        if add_question_to_bank(bot_token, q_data):
            await query.answer("", show_alert=True)
            await q_bank_manager_ui(update, context)
            context.user_data.pop('temp_q', None)
        else:
            await query.answer("❌ فشل الحفظ في القاعدة.")


    elif data == "exec_create_quiz_final":
        quiz_data = context.user_data.get('temp_quiz')
        
        # تحويل القائمة لنص مفصول بفاصلة ليتناسب مع العمود 5 في الشيت
        quiz_data['target_groups_str'] = ",".join(quiz_data.get('target_groups', []))
        quiz_data['coach_id'] = str(user_id)

        from sheets import create_auto_quiz
        # الدالة الآن تعيد قيمتين (نجاح، رسالة)
        success, result = create_auto_quiz(bot_token, quiz_data)
        
        if success:
            await query.answer(f"🚀 تم إنشاء الاختبار (ID: {result}) بنجاح!", show_alert=True)
            await manage_control_ui(update, context)
            context.user_data.pop('temp_quiz', None)
        else:
            msg = "⚠️ عذراً: بنك الأسئلة لا يحتوي على عدد كافٍ من الأسئلة لهذه الدورة." if result == "نقص أسئلة" else f"❌ فشل: {result}"
            await query.answer(msg, show_alert=True)



    # 3. بدء تفعيل/إنشاء الاختبارات (اختيار الدورة)
    elif data == "manage_tests":

        await quiz_activation_start(update, context)

    # 4. اختيار المجموعات المستهدفة للاختبار
    elif data.startswith("act_q_crs_"):
        course_id = data.replace("act_q_crs_", "")

        await quiz_activation_groups(update, context, course_id)

    # 5. عرض الاختبارات المتاحة للموظف (الأرشيف)
    elif data == "manage_archiveaq":

        await employee_quiz_view(update, context)

    # 6. تبديل حالة ظهور الاختبار (TRUE/FALSE)
    # معالج تبديل رؤية الاختبار (النسخة المعتمدة والمرنة)
    elif data.startswith("q_toggle_vis_"):
        quiz_id = data.replace("q_toggle_vis_", "")

        
        # 1. تغيير الحالة في القاعدة (TRUE <-> FALSE)
        new_status = toggle_quiz_visibility(bot_token, quiz_id)
        
        # 2. إرسال تنبيه سريع للمستخدم بالحالة الجديدة
        await query.answer(f"✅ تم تغيير الحالة إلى: {new_status}")
        
        # 3. تحديث واجهة الخيارات فوراً لإظهار الأيقونة المحدثة (عين أو قفل)

        await quiz_options_ui(update, context, quiz_id)

    # 7. إدارة صلاحيات الموظف (التأسيس الصامت + عرض اللوحة)
    elif data.startswith("setup_p_perms_"):
        person_id = data.replace("setup_p_perms_", "")
        
        
        # التأكد من وجود سجل في ورقة الهيكل التنظيمي
        ensure_permission_row_exists(bot_token, person_id)
        
        # جلب الصلاحيات وعرض لوحة التحكم (الصح والخطأ)
        current_perms = get_employee_permissions(bot_token, person_id)
        await query.edit_message_text(
            f"🔐 <b>ضبط صلاحيات المستخدم ID:</b> <code>{person_id}</code>",
            reply_markup=get_permissions_keyboard(bot_token, person_id, current_perms),
            parse_mode="HTML"
        )

        
        
        
        
# --------------------------------------------------------------------------

    elif data == "administrative_tasks":
        await query.edit_message_text(
            "أهلاً في إدارة المهام الإدارية، يمكنك إضافة المهام الفردية والجماعية إلى كافة الموظفين أو المدربين.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ إضافة مهمة فردية", callback_data="single_missions"), InlineKeyboardButton("➕ إضافة مهمة لمجموعة", callback_data="single_group")],
                [InlineKeyboardButton("➕ إضافة مهمة لدورة", callback_data="single_course"), InlineKeyboardButton("➕ إضافة مهمة جماعية", callback_data="single_all")],
                [InlineKeyboardButton("عرض جميع المهام", callback_data="single_show")],
                [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
            ]), parse_mode="HTML"
        )

               
# --------------------------------------------------------------------------
# أضف هذه الحالات داخل contact_callback_handler في education_bot.py

    # استدعاء واجهة المجموعات الرئيسية لدورة معينة
    elif data.startswith("manage_group_"):
        course_id = data.replace("manage_group_", "")

        await manage_groups_main(update, context, course_id)

    # بدء إضافة مجموعة جديدة
    elif data.startswith("grp_add_start_"):
        course_id = data.replace("grp_add_start_", "")

        await start_add_group(update, context, course_id)

    # اختيار المعلم أثناء الإضافة
    elif data.startswith("sel_teacher_"):
        parts = data.split("_")
        teacher_id = parts[2]
        # جلب الاسم من دالة جلب المدربين السابقة
        ###$$$$$$############ تم حذف دالة get_all_coaches مو ملف sheets.py
        coaches = get_all_coaches(bot_token)
        teacher_name = next((c['name'] for c in coaches if str(c['id']) == str(teacher_id)), "مدرب")
        

        await confirm_group_save(update, context, teacher_id, teacher_name)

    # التنفيذ الفعلي للحفظ
    elif data == "exec_save_group":

        group_data = context.user_data.get('temp_grp')
        if save_group_to_db(bot_token, group_data):
            await query.answer("✅ تم إنشاء المجموعة بنجاح", show_alert=True)
            # العودة لواجهة المجموعات

            await manage_groups_main(update, context, group_data['course_id'])
            context.user_data.pop('temp_grp', None)
        else:
            await query.answer("❌ فشل الحفظ في قاعدة البيانات", show_alert=True)

    # عرض خيارات مجموعة معينة (تعديل/حذف)
    elif data.startswith("grp_show_"):
        group_id = data.replace("grp_show_", "")

        await group_options_ui(update, context, group_id)

    # تأكيد الحذف
    elif data.startswith("grp_confirm_del_"):
        group_id = data.replace("grp_confirm_del_", "")

        await confirm_delete_group_ui(update, context, group_id)

    # التنفيذ الفعلي للحذف
    elif data.startswith("grp_exec_del_"):
        group_id = data.replace("grp_exec_del_", "")

        if delete_group_by_id(bot_token, group_id):
            await query.answer("🗑️ تم حذف المجموعة بنجاح", show_alert=True)
            # العودة للقائمة (ستحتاج لتخزين course_id في context.user_data للعودة الصحيحة)
            await query.edit_message_text("✅ تم الحذف. يرجى العودة للقائمة الرئيسية.")
        else:
            await query.answer("❌ فشل الحذف", show_alert=True)

    # التعديلات (تغيير الحالة كمثال سريع)
    elif data.startswith("grp_edit_status_"):
        group_id = data.replace("grp_edit_status_", "")
        
        # تبديل الحالة بين نشطة ومغلقة
        update_group_field(bot_token, group_id, "حالة_المجموعة", "مغلقة")
        await query.answer("✅ تم تغيير حالة المجموعة إلى مغلقة")

        await group_options_ui(update, context, group_id)

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# --------------------------------------------------------------------------
    # --- [ جزء استعراض الطلاب للدورات ] ---
    elif data == "view_courses":
        
        categories = get_all_categories(bot_token)
        if not categories:
            await query.edit_message_text("⚠️ لا توجد أقسام تعليمية متاحة حالياً.")
            return
        
        # عرض الأقسام كأزرار للطالب
        keyboard = [[InlineKeyboardButton(f"📂 {cat['name']}", callback_data=f"std_cat_{cat['id']}")] for cat in categories]
        keyboard.append([InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="back_to_student")])
        await query.edit_message_text("🎓 <b>اختر القسم الذي تود استعراض دوراته:</b>", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data.startswith("std_cat_"):
        cat_id = data.replace("std_cat_", "")
       
        courses = get_courses_by_category(bot_token, cat_id)
        
        if not courses:
            await query.edit_message_text("⚠️ لا توجد دورات متاحة في هذا القسم حالياً.", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للأقسام", callback_data="view_courses")]]))
            return

        keyboard = [[InlineKeyboardButton(f"📘 {crs['name']}", callback_data=f"std_crs_det_{crs['id']}")] for crs in courses]
        keyboard.append([InlineKeyboardButton("🔙 عودة للأقسام", callback_data="view_courses")])
        await query.edit_message_text("📚 <b>الدورات المتاحة في هذا القسم:</b>", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    elif data == "back_to_student":
        await query.edit_message_text("مرحباً بك في القائمة الرئيسية:", reply_markup=get_student_menu())

# --------------------------------------------------------------------------
    if data.startswith("p_limit_crs_"):
        parts = data.split("_")
        emp_id = parts[3]
        target_crs_id = parts[4]
        
        
        # تحديث القائمة في القاعدة (إضافة/حذف ID الدورة)
        toggle_scope_id(bot_token, emp_id, "الدورات_المسموحة", target_crs_id)
        
        # إعادة تحديث الواجهة لإظهار الصح والخطأ الجديد
        await show_course_selector(query, context, emp_id)




# --------------------------------------------------------------------------
    # 1. عرض القائمة الرئيسية (للأوسمة)
    elif data == "honors_achievements":
        # بما أننا اتفقنا أن الملف نظيف، نستدعي المحرك فقط
        await course_engine.show_student_honors(update, context)
# دالة الأسئلة 
    elif data == "add_question_to_bank":
        await educational_manager.start_add_question_flow(update, context)

    elif data == "public_channel_idd":
        await course_engine.set_channel_id_flow(update, context, "public_channel_id")

    elif data == "honors_channel_idd":
        await course_engine.set_channel_id_flow(update, context, "honors_channel_id")



#©©©©©©©©©©©©©©©©©©©
# معالجة الأوسمة والإنجازات
    elif data == "view_all_achievements":
        await course_engine.view_all_achievements_admin(update, context)

    elif data.startswith("view_medal_"):
        record_id = data.replace("view_medal_", "")
        await course_engine.view_medal_details(update, context, record_id)
        
    elif data == "grant_medal_start":
        context.user_data['action'] = 'awaiting_medal_student_id'
        await query.edit_message_text("🆔 يرجى إرسال <b>ID التيليجرام</b> للطالب المراد تكريمه:", parse_mode="HTML")

    # 2. عرض تفاصيل إنجاز محدد للطالب (باستخدام معرف السجل)
    elif data.startswith("st_medal_"):
        record_id = data.replace("st_medal_", "")
        await course_engine.view_single_achievement(update, context, record_id)






#®®®®®®®®®®®®®®®®®®®
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# أضف هذا الجزء داخل دالة contact_callback_handler في ملف education_bot.py
    elif data == "manage_personnel":
        
        people = get_all_personnel_list(bot_token)
        
        if not people:
            await query.edit_message_text("⚠️ لا يوجد موظفون أو مدربون مسجلون حالياً.", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]]))
            return

        keyboard = []
        for p in people:
            # نضع الاسم والنوع على الزر، والـ ID مخفي في الـ callback_data
            label = f"👤 {p['name']} ({p['type']})"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"manage_perms_{p['id']}")])
        
        # خيار إضافي في حال أردت إدخال ID غير موجود بالقائمة
        keyboard.append([InlineKeyboardButton("🔍 إدخال ID يدوي", callback_data="ask_emp_id_perms")])
        keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")])
        
        await query.edit_message_text("👥 <b>اختر الموظف أو المدرب لضبط صلاحياته:</b>", 
                                     reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    # 2. عرض لوحة الصلاحيات (الصح ☑️ والخطأ ✖️) فور اختيار الاسم
    elif data.startswith("manage_perms_"):
        emp_id = data.replace("manage_perms_", "")
        
        
        # جلب الصلاحيات الحالية لعرض حالة الأزرار
        current_perms = get_employee_permissions(bot_token, emp_id)
        
        await query.edit_message_text(
            f"🔐 <b>ضبط صلاحيات المستخدم ID:</b> <code>{emp_id}</code>\n\nاضغط على المهمة لتبديل الحالة:",
            reply_markup=get_permissions_keyboard(bot_token, emp_id, current_perms),
            parse_mode="HTML"
        )

    # 3. معالجة التبديل (Toggle) للصلاحيات الوظيفية
    elif data.startswith("p_toggle_"):
        parts = data.split("_")
        emp_id = parts[2]
        col_name = "_".join(parts[3:])
        # استدعاء الدالة التي أضفتها أنت في نهاية الملف
        await handle_permission_toggle(query, bot_token, emp_id, col_name)

    # 4. فتح واجهة اختيار الدورات المسموحة
    elif data.startswith("p_limit_crs_"):
        parts = data.split("_")
        emp_id = parts[3]
        if len(parts) == 4: # فقط طلب فتح القائمة
            await show_course_selector(query, context, emp_id)
        else: # طلب تبديل دورة محددة
            target_crs_id = parts[4]

            toggle_scope_id(bot_token, emp_id, "الدورات_المسموحة", target_crs_id)
            await show_course_selector(query, context, emp_id)


    elif data == "view_courses_admin": # زر استعراض الدورات للموظف
        
        
        perms = get_employee_permissions(bot_token, user_id)
        allowed_str = perms.get("الدورات_المسموحة", "")
        
        # جلب كل الدورات وتصفيتها
        all_courses = courses_sheet.get_all_records()
        
        if allowed_str:
            allowed_list = [x.strip() for x in allowed_str.split(",") if x.strip()]
            # تصفية: أظهر فقط الدورات التي تنتمي لهذا البوت وموجودة في قائمة المسموح
            filtered = [c for c in all_courses if str(c['bot_id']) == str(bot_token) and str(c['معرف_الدورة']) in allowed_list]
        else:
            # إذا كان المالك (allowed_str فارغ غالباً)، اعرض الكل
            filtered = [c for c in all_courses if str(c['bot_id']) == str(bot_token)]
            
# --------------------------------------------------------------------------

    # معالج تصدير النسخة الاحتياطية بصيغة JSON
    elif data == "export_data_json":
        import json
        import io
        from cache_manager import FACTORY_GLOBAL_CACHE
        
        current_token = str(context.bot.token)
        await query.message.reply_text("⏳ جاري فحص  البيانات وتجميع النسخة الاحتياطية...")
        
        backup_data = {}
        all_cache = FACTORY_GLOBAL_CACHE.get("data", {})
        
        for sheet_name, records in all_cache.items():
            if not records: continue
            
            # منطق الفلترة الذكي:
            # إذا كان الجدول يحتوي على عمود bot_id نفلتر بحسبه
            if "bot_id" in records[0]:
                bot_records = [r for r in records if str(r.get("bot_id")) == current_token]
            else:
                # إذا لم يوجد bot_id (مثل جدول المستخدمين العام في المصنع) 
                # سنأخذ كافة البيانات حالياً أو يمكنك تخصيصها
                bot_records = records 
            
            if bot_records:
                backup_data[sheet_name] = bot_records
        
        if not backup_data:
            await query.message.reply_text("⚠️ لم يتم العثور على أي سجلات مطابقة لتوكن هذا البوت.")
            return

        try:
            # تحويل البيانات لملف
            json_file = io.BytesIO(json.dumps(backup_data, indent=4, ensure_ascii=False).encode('utf-8'))
            json_file.name = f"Backup_Full_{get_system_time('date')}.json"
            
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=json_file,
                caption=(
                    f"✅ **اكتمل تجميع النسخة الاحتياطية**\n\n"
                    f"📦 عدد الجداول المضمنة: {len(backup_data)}\n"
                    f"📅 التوقيت: {get_system_time('full')}\n"
                    f"🔑 معرف البوت: `{current_token[:10]}...`"
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            await query.message.reply_text(f"❌ خطأ فني أثناء إنشاء الملف: {e}")


    # معالج استيراد البيانات (تفعيل حالة الانتظار)
    elif data == "import_data_json":
        context.user_data['action'] = 'awaiting_json_backup'
        await query.edit_message_text("📥 **نظام الاستيراد الذكي:**\nمن فضلك أرسل ملف النسخة الاحتياطية بصيغة `.json` الآن.")







# --------------------------------------------------------------------------
    elif data == "close_panel":
        await query.edit_message_text("🔒 تم إغلاق لوحة التحكم.")




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

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# --- [ معالج الرسائل النصية (Message Handler) ] ---
# --------------------------------------------------------------------------
# ملاحظة هامة: يجب أن يكون السطر التالي في أعلى الملف تماماً خارج كل الدوال:
async def handle_contact_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة كافة الرسائل النصية والربط مع محرك g4f لخدمة الطلاب مع بقاء مهام المسؤول كاملة"""
    
    if not update.message: return # تم الإبقاء على فحص الرسالة الأساسي
    
    # تنظيف النص من المسافات فور وصوله
    text = update.message.text.strip() if update.message.text else ""
    user = update.effective_user
    bot_token = context.bot.token
    action = context.user_data.get('action') # الحالة الحالية للمستخدم
    
    # تصحيح: نقل جلب الإعدادات للأعلى لضمان توفر bot_owner_id لكافة الأقسام
    try:
        config = get_bot_config(bot_token)
        bot_owner_id = int(config.get("admin_ids", 0))
    except Exception as e:
        print(f"⚠️ Error getting config: {e}")
        bot_owner_id = 0 

    # 🛑 [حماية المسار]: إذا كان المستخدم في أي مرحلة تسجيل، نعالج النص هنا ثم نخرج بـ return فوراً
    registration_actions = [
        'awaiting_reg_full_name', 'awaiting_reg_phone', 
        'awaiting_reg_specialty', 'awaiting_reg_job_title', 
        'awaiting_reg_email'
    ]

#  //===========================================
    # استخراج الحالة الحالية للمستخدم لسهولة الفحص
    current_action = context.user_data.get('action')
   
    # //==============================================================
# فحص بيانت المكتبة  
    if 'awaiting_lib_file' in context.user_data:
        import educational_manager
        await educational_manager.save_library_file_logic(update, context)
        return
    # //==============================================================
    # // [1] محرك معالجة منح الأوسمة (Course Engine)
    # // اعتراض الرسائل إذا كان المالك يقوم الآن بإدخال بيانات وسام لطالب
    # //==============================================================
    medal_actions = ['awaiting_medal_student_id', 'awaiting_medal_name', 'awaiting_medal_reason']
    if current_action in medal_actions:
        await course_engine.process_grant_medal_step(update, context)
        return

    # //==============================================================
    # // [2] ضبط قنوات الإشعارات (Course Engine)
    # // اعتراض الرسائل إذا كان المالك يقوم بإرسال رابط أو ID قناة لضبط الإعدادات
    # //==============================================================
    if context.user_data.get('awaiting_setting_key'):
        await course_engine.save_channel_id_logic(update, context)
        return

    # //==============================================================
    # // [3] إضافة أسئلة لبنك الأسئلة (Educational Manager)

    # //==============================================================
    if current_action and str(current_action).startswith('awaiting_q_'):

        await educational_manager.process_q_flow(update, context)
        return

    # //==============================================================
    # // [4] محرك تسجيل الطلاب الجديد (Registration Flow)
    # // تحويل الرسالة لمحرك التسجيل إذا كان المستخدم يقوم بملء بيانات انضمامه
    # //==============================================================
    if context.user_data.get('reg_flow'):
        await course_engine.process_registration_steps(update, context)
        return



#>>>>>>>>>>>>>>>>#>>>>>>>>>>>>>>>>
    if action in registration_actions:
        # --- 1. مرحلة الاسم الكامل ---
        if action == 'awaiting_reg_full_name':
            context.user_data['reg_data'] = {'name': text}
            context.user_data['action'] = 'awaiting_reg_phone'
            await update.message.reply_text("📱 ممتاز يا أستاذ، يرجى إرسال <b>رقم الهاتف</b> للتواصل:", parse_mode="HTML")
            return # يمنع الذهاب للذكاء الاصطناعي
        elif action == 'awaiting_reg_phone':
            context.user_data['reg_data']['phone'] = text
            role = context.user_data.get('reg_role')
            if role == "coach":
                context.user_data['action'] = 'awaiting_reg_specialty'
                await update.message.reply_text("🎓 يرجى إرسال <b>مجال التخصص</b> (تخصص واحد فقط):", parse_mode="HTML")
            else:
                context.user_data['action'] = 'awaiting_reg_job_title'
                await update.message.reply_text("💼 يرجى إرسال <b>المسمى الوظيفي</b> الخاص بك:", parse_mode="HTML")
            return

        elif action in ['awaiting_reg_specialty', 'awaiting_reg_job_title']:
            context.user_data['reg_data']['info'] = text
            context.user_data['action'] = 'awaiting_reg_email'
            await update.message.reply_text("📧 وأخيراً، يرجى إرسال <b>البريد الإلكتروني</b> الرسمي:", parse_mode="HTML")
            return


        elif action == 'awaiting_reg_email':
            reg = context.user_data['reg_data']
            reg['email'] = text
            reg['username'] = user.username or "بدون" 
            role = context.user_data.get('reg_role')
            role_ar = "مدرب" if role == "coach" else "موظف"
            
            # إرسال البيانات للمالك (أنت) - المتغير bot_owner_id متاح الآن هنا
            info_msg = (
                f"🚨 <b>طلب انضمام {role_ar} جديد:</b>\n\n"
                f"👤 الاسم: {reg['name']}\n"
                f"📱 الهاتف: {reg['phone']}\n"
                f"🎓 التخصص/الوظيفة: {reg['info']}\n"
                f"📧 البريد: {reg['email']}\n"
                f"🆔 الآيدي: <code>{user.id}</code>\n"
                f"🔗 اليوزر: @{user.username or 'بدون'}\n\n"
                f"هل تريد اعتماد هذا الكادر في المؤسسة؟"
            )
            keyboard = [
                [InlineKeyboardButton("✅ نعم، اعتماد", callback_data=f"approve_reg_{role}_{user.id}"),
                 InlineKeyboardButton("❌ رفض", callback_data=f"reject_reg_{user.id}")]
            ]
            await context.bot.send_message(chat_id=bot_owner_id, text=info_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            
            await update.message.reply_text("✅ <b>تم إرسال بياناتك بنجاح.</b>\nسيتم إشعارك فور موافقة الإدارة على طلبك.",parse_mode="HTML")
            context.user_data['action'] = None
            return

#>>>>>>>>>>>>>>>>
    # معالجة المستندات (التي تحتوي على ملف النسخة الاحتياطية)
    if update.message.document:
        doc = update.message.document
        # معالجة رفع الملف (للكاش فقط)
        if action == 'awaiting_json_backup' and doc.file_name.endswith('.json'):
            import json
            from cache_manager import FACTORY_GLOBAL_CACHE, save_cache_to_disk
            
            file = await context.bot.get_file(doc.file_id)
            content = await file.download_as_bytearray()
            new_data = json.loads(content.decode('utf-8'))
            
            # 1. تحديث الرام (FACTORY_GLOBAL_CACHE)
            for sheet_name, rows in new_data.items():
                # دمج البيانات الجديدة مع الكاش الموجود أو استبداله
                FACTORY_GLOBAL_CACHE["data"][sheet_name] = rows
                
            # 2. حفظ نسخة في الهاردسك (للحماية من الريستارت)
            save_cache_to_disk() 
            
            await update.message.reply_text(
                "✅ **تم شحن الرام بالبيانات بنجاح!**\n\n"
                "⚠️ البيانات الآن تعمل في البوت (كاش).\n"
                "⏰ سيتم المزامنة الآلية مع الرام تلقائياً الساعة 12:00 ليلاً كل يوم."
            )
            context.user_data['action'] = None
            return


# --------------------------------------------------------------------------
        # --- [ معالج استيراد بنك الأسئلة المستقل - مضاف بدون تعديل القديم ] --- 
    if update.message.document:
        action = context.user_data.get('action')
        doc = update.message.document
        
        if action == 'awaiting_excel_file':
            import pandas as pd
            import os, uuid

            file = await context.bot.get_file(doc.file_id)
            file_path = f"temp_{uuid.uuid4().hex}_{doc.file_name}"
            await file.download_to_drive(file_path)
            
            try:
                xls = pd.ExcelFile(file_path)
                # --- [ مخازن الربط الذكي - القواميس ] ---
                cat_map = {}    # لربط اسم القسم بـ ID
                coach_map = {}  # لربط اسم المدرب بـ ID
                course_map = {} # لربط اسم الدورة بـ ID
                test_map = {}   # لربط اسم الاختبار بـ ID
                
                results = {"الأقسام": 0, "المدربين": 0, "الدورات": 0, "المجموعات": 0, "الطلاب": 0, "الاختبارات": 0, "الأسئلة": 0}

                # 1️⃣ معالجة الأقسام (الأساس)
                if 'الاقسام' in xls.sheet_names:
                    df = pd.read_excel(xls, 'الاقسام').fillna("")
                    for _, r in df.iterrows():
                        c_id = f"C{str(uuid.uuid4().int)[:4]}"
                        name = str(r.get('اسم_القسم', '')).strip()
                        if name and add_new_category(bot_token, c_id, name):
                            cat_map[name] = c_id
                            results["الأقسام"] += 1

                # 2️⃣ معالجة المدربين
                if 'المدربين' in xls.sheet_names:
                    df = pd.read_excel(xls, 'المدربين').fillna("")
                    for _, r in df.iterrows():
                        c_id = str(r.get('ID_المدرب', uuid.uuid4().int % 1000000000)).strip()
                        name = str(r.get('اسم_المدرب', '')).strip()
                        if name and add_new_coach_advanced(bot_token, c_id, name, str(r.get('التخصص', '')), str(r.get('رقم_الهاتف', ''))):
                            coach_map[name] = c_id
                            results["المدربين"] += 1

                # 3️⃣ معالجة الدورات (الربط بالأقسام والمدربين)
                if 'الدورات' in xls.sheet_names:
                    df = pd.read_excel(xls, 'الدورات').fillna("")
                    for _, r in df.iterrows():
                        c_id = f"CRS{str(uuid.uuid4().int)[:4]}"
                        c_name = str(r.get('الاسم', '')).strip()
                        # الربط الآلي: البحث عن ID القسم والمدرب باستخدام أسمائهم
                        cat_id = cat_map.get(str(r.get('اسم_القسم', '')).strip(), "C000")
                        coach_id = coach_map.get(str(r.get('اسم_المدرب', '')).strip(), "000")
                        
                        if add_new_course(bot_token, c_id, c_name, str(r.get('الوصف', '')), "2026-01-01", "", "أونلاين", 
                                         str(r.get('السعر', '0')), "100", "لا يوجد", "إدارة", "ADM", "رفع_شامل", 
                                         "Admin", coach_id, str(r.get('اسم_المدرب', '')), cat_id):
                            course_map[c_name] = c_id
                            results["الدورات"] += 1

                # --- تحديث الكاش المركزي بعد اكتمال الرفع الشامل ---
                from cache_manager import update_global_version
                update_global_version(bot_token)
                
                # بناء تقرير النتائج بناءً على ما تم معالجته فعلياً
                report_lines = [f"🔹 {k}: {v}" for k, v in results.items() if v > 0]
                report_text = "✅ <b>اكتمل الرفع والربط الشامل:</b>\n\n" + "\n".join(report_lines)
                report_text += "\n\n🔄 <b>حالة الكاش:</b> تمت المزامنة اللحظية بنجاح."
                
                await update.message.reply_text(report_text, parse_mode="HTML")

            except Exception as e:
                await update.message.reply_text(f"❌ خطأ حرج في المعالجة: {str(e)}")
            finally:
                if os.path.exists(file_path): 
                    os.remove(file_path)
            
            context.user_data['action'] = None
            return 

            
           
# --------------------------------------------------------------------------



# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
    if context.args:
        if context.args[0].startswith("ad_"):
            campaign_id = context.args[0].replace("ad_", "")
            context.user_data['source_campaign_id'] = campaign_id

    
# --------------------------------------------------------------------------
    # --- [ الجزء الخاص بالمسؤول - إدارة المحتوى والدورات ] ---
    if user.id == bot_owner_id:
    	
    # --- [ معالجة خطوات إضافة كود الخصم نصياً ] ---
        if action == 'awaiting_dsc_desc':

            await validate_dsc_desc(update, context)
            return

        elif action == 'awaiting_dsc_value':

            await validate_dsc_value(update, context)
            return

        elif action == 'awaiting_dsc_expiry':
        
            await validate_dsc_expiry(update, context)
            return

        elif action == 'awaiting_dsc_max':
            
            await validate_dsc_max(update, context)
            return
#~~~~~~~~~~~~~~~~
        # --- [ حفظ معلومات الدفع الافتراضية ] ---
        elif action == 'awaiting_payment_info_text':
            
            await save_payment_info_logic(update, context)
            return
#~~~~~~~~~~~~~~~~
        # --- [ حفظ درجة الواجبات ] ---
        elif action == 'awaiting_homework_grade_value':
  
            await save_homework_grade_logic(update, context)
            return



#~~~~~~~~~~~~~~~~
        # --- [ حفظ وحدة العملة ] ---
        elif action == 'awaiting_currency_unit_value':

            await save_currency_unit_logic(update, context)
            return

#~~~~~~~~~~~~~~~~
        # --- [ حفظ نقاط الإحالة عند الانضمام ] ---
        elif action == 'awaiting_ref_points_join_value':

            await save_ref_points_join_logic(update, context)
            return
#~~~~~~~~~~~~~~~~
        # --- [ حفظ نقاط الإحالة عند شراء دورة ] ---
        elif action == 'awaiting_ref_points_purchase_value':

            await save_ref_points_purchase_logic(update, context)
            return

#~~~~~~~~~~~~~~~~
        # --- [ حفظ الحد الأدنى لمبلغ السحب ] ---
        elif action == 'awaiting_min_payout_value':

            await save_min_payout_logic(update, context)
            return

#~~~~~~~~~~~~~~~~
        # --- [ حفظ درجات النجاح ] ---
        elif action == 'awaiting_min_passing_grade_value':

            await save_min_passing_grade_logic(update, context)
            return

        elif action == 'awaiting_max_passing_grade_value':

            await save_max_passing_grade_logic(update, context)
            return
#~~~~~~~~~~~~~~~~
        # --- [ حفظ نسبة عمولة المسوقين ] ---
        elif action == 'awaiting_marketers_commission_value':

            await save_marketers_commission_logic(update, context)
            return

#~~~~~~~~~~~~~~~~
        # --- [ إدارة الحملات الإعلانية ] ---
        elif action and action.startswith('awaiting_ad_'):

            await process_ad_campaign_flow(update, context)
            return

#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~





#~~~~~~~~~~~~~~~~
    
        # إضافة قسم جديد
        if action == 'awaiting_cat_name':
            import uuid
            cat_id = f"C{str(uuid.uuid4().int)[:4]}"
           
            if add_new_category(bot_token, cat_id, text):
                context.user_data['action'] = None
                await update.message.reply_text(f"✅ تم إنشاء القسم بنجاح: <b>{text}</b>", reply_markup=get_admin_panel(), parse_mode="HTML")
            return
            

        # استقبال ID الموظف لفتح لوحة صلاحياته
        # استقبال ID الموظف لفتح لوحة صلاحياته (النسخة المعتمدة والأقوى)
        elif action == 'awaiting_emp_id_for_perms':
            emp_id = text
            context.user_data['action'] = None
            
            
            # جلب الصلاحيات الحالية من القاعدة لعرض الأزرار بشكل صحيح
            current_perms = get_employee_permissions(bot_token, emp_id)
            
            await update.message.reply_text(
                f"🔐 <b>تم العثور على الموظف:</b> <code>{emp_id}</code>\n\n"
                f"قم بضبط الصلاحيات المطلوبة بالضغط على الأزرار أدناه:", 
                reply_markup=get_permissions_keyboard(bot_token, emp_id, current_perms), 
                parse_mode="HTML"
            )
            return

            
        # تعديل اسم قسم
        elif action == 'awaiting_new_cat_name':
            cat_id = context.user_data.get('selected_cat_id')
            
            if update_category_name(bot_token, cat_id, text):
                context.user_data['action'] = None
                await update.message.reply_text(f"✅ تم تحديث اسم القسم إلى: <b>{text}</b>", reply_markup=get_admin_panel(), parse_mode="HTML")
            return

        # إضافة دورة بسيطة
        elif action == 'awaiting_course_name':
            import uuid
            course_cat = context.user_data.get('temp_course_cat')
            course_id = f"CRS{str(uuid.uuid4().int)[:4]}"
            
            if add_new_course(bot_token, course_id, text, course_cat):
                context.user_data['action'] = None
                await update.message.reply_text(f"✅ تم إضافة الدورة بنجاح: <b>{text}</b>", reply_markup=get_admin_panel(), parse_mode="HTML")
            return

        # تسلسل إضافة دورة احترافي (الخطوة 2: الاسم)
        elif action == 'awaiting_crs_name':
            context.user_data['temp_crs'] = {'name': text}
            context.user_data['action'] = 'awaiting_crs_hours'
            await update.message.reply_text("⏳ <b>الخطوة 3:</b> أرسل عدد ساعات الدورة (أو وصفاً قصيراً):", parse_mode="HTML")
            return

        # الخطوة 3: الساعات
        elif action == 'awaiting_crs_hours':
            context.user_data['temp_crs']['hours'] = text
            context.user_data['action'] = 'awaiting_crs_price'
            await update.message.reply_text("💰 <b>الخطوة 4:</b> أرسل سعر الدورة (أرقام فقط):", parse_mode="HTML")
            return

        # الخطوة 4: السعر وعرض خيارات المدربين
        elif action == 'awaiting_crs_price':
            context.user_data['temp_crs']['price'] = text
            
            coaches = get_all_coaches(bot_token)
            
            msg = "👨‍🏫 <b>الخطوة 5:</b> اختر المدرب من القائمة أدناه، أو أرسل (يوزرنايم/ID) يدوي:"
            keyboard = []
            if coaches:
                for c in coaches:
                    keyboard.append([InlineKeyboardButton(f"👤 {c['name']}", callback_data=f"sel_coach_for_crs_{c['id']}")])
            
            keyboard.append([InlineKeyboardButton("❌ إلغاء العملية", callback_data="manage_courses")])
            context.user_data['action'] = 'awaiting_crs_coach'
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return

        # الخطوة 5: استقبال المدرب
        elif action == 'awaiting_crs_coach':
            input_val = text
            if input_val.isdigit():
                context.user_data['temp_crs'].update({'coach_user': "إدخال يدوي", 'coach_id': input_val, 'coach_name': f"مدرب (ID: {input_val})"})
                context.user_data['action'] = 'awaiting_crs_date'
                await update.message.reply_text(f"✅ تم قبول المعرف: <code>{input_val}</code>\n\n🗓 <b>الخطوة 6:</b> أرسل تاريخ بداية الدورة:", parse_mode="HTML")
            else:
                coach_username = input_val.replace("@", "")
                
                user_data = find_user_by_username(bot_token, coach_username)
                if user_data:
                    context.user_data['temp_crs'].update({'coach_user': f"@{coach_username}", 'coach_id': user_data['id'], 'coach_name': user_data['name']})
                else:
                    try:
                        coach_chat = await context.bot.get_chat(f"@{coach_username}")
                        context.user_data['temp_crs'].update({'coach_user': f"@{coach_username}", 'coach_id': coach_chat.id, 'coach_name': coach_chat.full_name})
                    except:
                        await update.message.reply_text("❌ لم أستطع العثور عليه. أرسل **المعرف الرقمي** للمدرب الآن:")
                        return
                context.user_data['action'] = 'awaiting_crs_date'
                await update.message.reply_text(f"✅ تم العثور على: {context.user_data['temp_crs']['coach_name']}\n\n🗓 <b>الخطوة 6:</b> أرسل تاريخ البداية:")
            return

        # الخطوة 6: التاريخ والمراجعة
        elif action == 'awaiting_crs_date':
            context.user_data['temp_crs']['start_date'] = text
            d = context.user_data['temp_crs']
            summary = (
                f"📝 <b>مراجعة بيانات الدورة:</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📂 القسم: {context.user_data.get('temp_crs_cat')}\n"
                f"📚 الاسم: {d['name']}\n"
                f"⏳ الساعات: {d['hours']}\n"
                f"💰 السعر: {d['price']}\n"
                f"👨‍🏫 المدرب: {d['coach_name']}\n"
                f"🗓 البداية: {text}\n"
                f"━━━━━━━━━━━━━━\n"
                f"<b>هل البيانات صحيحة؟</b>"
            )
            keyboard = [[InlineKeyboardButton("✅ نعم، اعتمد", callback_data="confirm_save_full_crs")], [InlineKeyboardButton("❌ إلغاء", callback_data="manage_courses")]]
            await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            context.user_data['action'] = None
            return


# --------------------------------------------------------------------------
        # --- [ محرك معالجة الإضافة الجماعية للدورات ] ---
        elif action == 'awaiting_bulk_courses':
            lines = text.split('\n')
            success_count = 0
            failed_lines = []
            
            import uuid

            for line in lines:
                if not line.strip(): continue # تخطي الأسطر الفارغة
                
                # تقسيم السطر بناءً على الفاصل الرأسي |
                parts = [p.strip() for p in line.split('|')]
                
                # التأكد من وجود الخمسة أجزاء المطلوبة حسب تعليماتك الجديدة
                if len(parts) >= 5:
                    c_id = f"CRS{str(uuid.uuid4().int)[:4]}"
                    
                    # إرسال البيانات للدالة (الترتيب مطابق للـ 17 عمود في sheets.py)
                    success = add_new_course(
                        bot_token,          # 1. bot_id
                        c_id,               # 2. معرف_الدورة
                        parts[0],           # 3. اسم_الدورة
                        parts[1],           # 4. عدد_الساعات (الوصف والساعات)
                        "2026-01-01",       # 5. تاريخ_البداية (افتراضي)
                        "",                 # 6. تاريخ_النهاية
                        "أونلاين",          # 7. نوع_الدورة
                        parts[2],           # 8. سعر_الدورة
                        "100",              # 9. الحد_الأقصى
                        "لا يوجد",          # 10. المتطلبات
                        "إدارة المنصة",      # 11. اسم_المندوب
                        "ADMIN01",          # 12. كود_المندوب
                        "عام",              # 13. الحملة_التسويقية
                        "إدخال جماعي",      # 14. معرف_المدرب (يوزر)
                        parts[3],           # 15. ID_المدرب (المعرف الرقمي)
                        "مدرب معتمد",       # 16. اسم_المدرب (افتراضي)
                        parts[4]            # 17. معرف_القسم
                    )
                    
                    if success:
                        success_count += 1
                    else:
                        failed_lines.append(line)
                else:
                    failed_lines.append(line)

            context.user_data['action'] = None
            
            # رسالة النتيجة النهائية
            result_msg = f"✅ <b>تمت العملية بنجاح!</b>\n\n📥 عدد الدورات المضافة: {success_count}"
            if failed_lines:
                result_msg += f"\n⚠️ أسطر فشلت (تأكد من التنسيق):\n" + "\n".join(failed_lines)
            
            await update.message.reply_text(result_msg, reply_markup=get_admin_panel(), parse_mode="HTML")
            return

#-----
        elif action == 'awaiting_sheet_link':
            import re, uuid
            
            
            # استخراج ID القاعدة من الرابط بدقة
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
            if not match:
                await update.message.reply_text("❌ رابط غير صحيح. أرسل رابط شيت صالح.")
                return

            try:
                external_ss = client.open_by_key(match.group(1))
                data = external_ss.get_worksheet(0).get_all_records()
                
                success_count = 0
                for r in data:
                    c_id = f"CRS{str(uuid.uuid4().int)[:4]}"
                    success = add_new_course(
                        bot_token, c_id, str(r.get('اسم_الدورة', '')), str(r.get('الوصف', '')),
                        "2026-01-01", "", "أونلاين", str(r.get('السعر', '0')), 
                        "100", "لا يوجد", "إدارة المنصة", "ADMIN01", "رابط", 
                        "Sheet", str(r.get('ID_المدرب', '')), "مدرب", str(r.get('ID_القسم', ''))
                    )
                    if success: success_count += 1
                
                await update.message.reply_text(f"✅ تم سحب {success_count} دورة من الرابط.")
            except Exception as e:
                await update.message.reply_text(f"❌ فشل الوصول للرابط: {str(e)}")
            context.user_data['action'] = None
            return





# --------------------------------------------------------------------------
# المجموعات 
# أضف هذا الجزء داخل handle_contact_message في education_bot.py

        elif action == 'awaiting_grp_name':

            await process_grp_name(update, context)
            return

        elif action == 'awaiting_grp_days':

            await process_grp_days(update, context)
            return

        elif action == 'awaiting_grp_time':

            await process_grp_time(update, context)
            return

# --------------------------------------------------------------------------
        # --- [ تابع دالة سحب الرصيد للمسوق ] ---
        elif action == 'awaiting_payout_method':
            amount = context.user_data.get('payout_amount', 0)
            currency = context.user_data.get('currency', "نقطة")
            payout_method = text  # النص الذي أرسله المسوق
            
            # تنفيذ الطلب في البيانات (سيتم خصم الرصيد تلقائياً من العمود 11)
            success, req_id = create_withdrawal_request(bot_token, user.id, user.username, amount, payout_method)
            
            if success:
                await update.message.reply_text(
                    f"✅ <b>تم تقديم طلبك بنجاح!</b>\n"
                    f"المبلغ المحجوز: <b>{amount} {currency}</b>\n"
                    f"رقم الطلب: <code>{req_id}</code>\n"
                    f"الحالة: <b>قيد الانتظار</b>",
                    parse_mode="HTML"
                )
                # إشعار مالك البوت (أنت) لاتخاذ إجراء
                admin_msg = (
                    f"🚨 <b>طلب سحب جديد:</b>\n"
                    f"👤 المسوق: {user.full_name} (@{user.username})\n"
                    f"💰 المبلغ: {amount} {currency}\n"
                    f"🏦 الوسيلة: <code>{payout_method}</code>\n"
                    f"🎫 المعرف: <code>{req_id}</code>"
                )
                keyboard = [[InlineKeyboardButton("✅ تم التحويل", callback_data=f"payout_approve_{req_id}"),
                             InlineKeyboardButton("❌ رفض", callback_data=f"payout_reject_{req_id}")]]
                await context.bot.send_message(chat_id=bot_owner_id, text=admin_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            else:
                await update.message.reply_text("❌ عذراً، رصيدك غير كافٍ أو حدث خطأ تقني.")
            
            context.user_data['action'] = None
            return  # إنهاء المعالجة لضمان صحة الصياغة البرمجية

        # معالجة استلام صورة الإيصال من الآدمن (المرحلة 2: التنفيذ الفعلي)
        elif action == 'awaiting_payout_proof' and update.message.photo:
            req_id = context.user_data.get('payout_req_id')
            target_user_id = context.user_data.get('target_payout_user_id')
            photo_file = await update.message.photo[-1].get_file()
            proof_url = photo_file.file_path
            
            if update_withdrawal_status(bot_token, req_id, "مكتمل", admin_note="تم التحويل والإثبات مرفق", proof_link=proof_url):
                await update.message.reply_text("✅ تم توثيق الإيصال وتحديث البيانات.")
                
                # إرسال الصورة للمسوق مباشرة
                if target_user_id:
                    caption = f"🎉 <b>تم تحويل أرباحك بنجاح!</b>\n🎫 رقم الطلب: <code>{req_id}</code>\n💰 الحالة: <b>مكتمل</b>"
                    await context.bot.send_photo(chat_id=target_user_id, photo=proof_url, caption=caption, parse_mode="HTML")
            
            context.user_data['action'] = None
            return  # إنهاء المعالجة ومنع حدوث Syntax Error مع الحالات التالية

        # --- [ حفظ كليشة الترحيب الجديدة - السطر 2829 الأصلي ] ---
        elif action == 'awaiting_new_welcome_text':
            period = context.user_data.get('edit_period')
            column_name = f"welcome_{period}"
            
            if update_content_setting(bot_token, column_name, text):
                await update.message.reply_text(f"✅ تم تحديث كليشة الترحيب <b>({period})</b> بنجاح!", reply_markup=get_admin_panel(), parse_mode="HTML")
                context.user_data['action'] = None
            else:
                await update.message.reply_text("❌ فشل التحديث. تأكد من إضافة الأعمدة المطلوبة.")
            return

        # --- [ حفظ كليشة الترحيب الجديدة ] ---
        elif action == 'awaiting_new_welcome_text':
            period = context.user_data.get('edit_period')
            column_name = f"welcome_{period}"
            
            if update_content_setting(bot_token, column_name, text):
                await update.message.reply_text(f"✅ تم تحديث كليشة الترحيب <b>({period})</b> بنجاح!", reply_markup=get_admin_panel(), parse_mode="HTML")
                context.user_data['action'] = None
            else:
                await update.message.reply_text("❌ فشل التحديث. تأكد من إضافة الأعمدة المطلوبة.")
            return

        # 1. استقبال اسم المؤسسة (تم دمجه في تسلسل الإدارة)
        # 1. استقبال اسم المؤسسة
        elif action == 'awaiting_institution_name':
           
            if save_ai_setup(bot_token, user.id, user.username, institution_name=text):
                context.user_data['action'] = 'awaiting_ai_instructions'
                await update.message.reply_text(f"✅ تم حفظ الاسم: <b>{text}</b>\n\nالآن أرسل <b>تعليمات الذكاء الاصطناعي</b> للمنصة:",parse_mode="HTML")
            else:
                # إذا فشل الحفظ، البوت سيخبرك بدلاً من التهنيج
                await update.message.reply_text("❌ عذراً دكتور، فشل الحفظ في القاعدة. تأكد من وجود قسم 'الذكاء_الإصطناعي'.")
            return


        # 2. استقبال تعليمات AI
        elif action == 'awaiting_ai_instructions':
            
            if save_ai_setup(bot_token, user.id, user.username, ai_instructions=text):
                context.user_data['action'] = None
                await update.message.reply_text("🎊 <b>اكتملت التهيئة!</b> تم ضبط هوية البوت بنجاح.",parse_mode="HTML", reply_markup=get_admin_panel())
            return
# --------------------------------------------------------------------------

        # --- [ تسلسل إضافة فرع جديد ] ---
        elif action == 'awaiting_branch_name':
            context.user_data['temp_br'] = {'name': text}
            context.user_data['action'] = 'awaiting_branch_country'
            await update.message.reply_text(f"🌍 تم تسجيل الاسم: <b>{text}</b>\nالآن أرسل <b>اسم الدولة</b> أو موقع الفرع:",parse_mode="HTML")
            return

        elif action == 'awaiting_branch_country':
            context.user_data['temp_br']['country'] = text
            context.user_data['action'] = 'awaiting_branch_manager'
            await update.message.reply_text(f"👤 من هو <b>المدير المسؤول</b> عن هذا الفرع؟",parse_mode="HTML")
            return

        elif action == 'awaiting_branch_manager':
            context.user_data['temp_br']['manager'] = text
            context.user_data['action'] = 'awaiting_branch_currency'
            await update.message.reply_text(f"💰 ما هي <b>العملة</b> المعتمدة للفرع؟ (مثلاً: SAR أو USD):",parse_mode="HTML")
            return

        elif action == 'awaiting_branch_currency':
            br = context.user_data.get('temp_br')
            success, b_id = add_new_branch_db(bot_token, br['name'], br['country'], br['manager'], text)
            if success:
                await update.message.reply_text(f"✅ <b>تم إنشاء الفرع بنجاح!</b>\n🆔 المعرف: <code>{b_id}</code>\n🏢 الاسم: {br['name']}\n👤 المدير: {br['manager']}", reply_markup=get_admin_panel(), parse_mode="HTML")
            else:
                await update.message.reply_text(f"❌ فشل الحفظ: {b_id}")
            context.user_data.pop('temp_br', None)
            context.user_data['action'] = None
            return
           
                  # --- [ معالجة تعديل اسم الفرع ] ---
        elif action == 'awaiting_new_branch_name':
            b_id = context.user_data.get('edit_br_id')
            if update_branch_field_db(bot_token, b_id, "اسم_الفرع", text):
                await update.message.reply_text(f"✅ تم تحديث اسم الفرع إلى: <b>{text}</b>", reply_markup=get_admin_panel(), parse_mode="HTML")
            else:
                await update.message.reply_text("❌ فشل تحديث البيانات.")
            context.user_data['action'] = None
            return

          
# --------------------------------------------------------------------------
        # تسلسل إضافة سؤال يدوي - استقبال نص السؤال
        elif action == 'awaiting_q_text':
            context.user_data['temp_q']['text'] = text
            context.user_data['action'] = 'awaiting_q_a'
            await update.message.reply_text("🔘 <b>الخطوة 3:</b> أرسل <b>الخيار (A)</b>:",parse_mode="HTML")
            return

        # استقبال الخيار A
        elif action == 'awaiting_q_a':
            context.user_data['temp_q']['a'] = text
            context.user_data['action'] = 'awaiting_q_b'
            await update.message.reply_text("🔘 <b>الخطوة 4:</b> أرسل <b>الخيار (B)</b>:",parse_mode="HTML")
            return

        # استقبال الخيار B
        elif action == 'awaiting_q_b':
            context.user_data['temp_q']['b'] = text
            context.user_data['action'] = 'awaiting_q_c'
            await update.message.reply_text("🔘 <b>الخطوة 5:</b> أرسل <b>الخيار (C)</b>:",parse_mode="HTML")
            return

        # استقبال الخيار C
        elif action == 'awaiting_q_c':
            context.user_data['temp_q']['c'] = text
            context.user_data['action'] = 'awaiting_q_d'
            await update.message.reply_text("🔘 <b>الخطوة 6:</b> أرسل <b>الخيار (D)</b>:",parse_mode="HTML")
            return

        # استقبال الخيار D وطلب الإجابة الصحيحة
        elif action == 'awaiting_q_d':
            context.user_data['temp_q']['d'] = text
            context.user_data['action'] = 'awaiting_q_correct'
            keyboard = [
                [InlineKeyboardButton("A", callback_data="set_q_ans_A"), InlineKeyboardButton("B", callback_data="set_q_ans_B")],
                [InlineKeyboardButton("C", callback_data="set_q_ans_C"), InlineKeyboardButton("D", callback_data="set_q_ans_D")]
            ]
            await update.message.reply_text(
                "✅ <b>الخطوة 7:</b> حدد <b>الإجابة الصحيحة</b> من الأزرار أدناه:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            return

        # استقبال درجة السؤال
        elif action == 'awaiting_q_grade':
            if not text.isdigit():
                await update.message.reply_text("⚠️ يرجى إرسال أرقام فقط لدرجة السؤال:")
                return
            context.user_data['temp_q']['grade'] = text
            context.user_data['action'] = 'awaiting_q_level'
            keyboard = [
                [InlineKeyboardButton("سهل", callback_data="set_q_lv_سهل"), 
                 InlineKeyboardButton("متوسط", callback_data="set_q_lv_متوسط"),
                 InlineKeyboardButton("صعب", callback_data="set_q_lv_صعب")]
            ]
            await update.message.reply_text("📊 <b>الخطوة 9:</b> اختر <b>مستوى صعوبة</b> السؤال من الأزرار:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return



        # تسلسل إعدادات الاختبار الآلي
        elif action == 'awaiting_quiz_title':
            context.user_data['temp_quiz']['quiz_id'] = text
            context.user_data['action'] = 'awaiting_quiz_q_count'
            await update.message.reply_text("🔢 <b>الخطوة 4:</b> كم <b>عدد الأسئلة</b> التي تريد سحبها من البنك لهذا الاختبار؟",parse_mode="HTML")
            return

        elif action == 'awaiting_quiz_q_count':
            if not text.isdigit():
                await update.message.reply_text("⚠️ أرسل رقماً فقط:")
                return
            context.user_data['temp_quiz']['q_count'] = text
            context.user_data['action'] = 'awaiting_quiz_pass'
            await update.message.reply_text("🎯 <b>الخطوة 5:</b> حدد <b>درجة النجاح</b> (مثلاً: 50):",parse_mode="HTML")
            return

        elif action == 'awaiting_quiz_pass':
            context.user_data['temp_quiz']['pass_score'] = text
            context.user_data['action'] = 'awaiting_quiz_time'
            await update.message.reply_text("⏱ <b>الخطوة 6:</b> حدد <b>مدة الاختبار الكلية</b> بالدقائق:",parse_mode="HTML")
            return

        elif action == 'awaiting_quiz_time':
            context.user_data['temp_quiz']['duration'] = text
            q = context.user_data['temp_quiz']
            summary = (
                f"⚙️ <b>مراجعة إعدادات الاختبار:</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📝 العنوان: {q['quiz_id']}\n"
                f"👥 المجموعات: {','.join(q['target_groups'])}\n"
                f"🔢 عدد الأسئلة: {q['q_count']}\n"
                f"🎯 النجاح من: {q['pass_score']}\n"
                f"⏱ المدة: {text} دقيقة\n"
                f"━━━━━━━━━━━━━━\n"
                f"هل تريد إنشاء الاختبار الآن؟"
            )
            keyboard = [
                [InlineKeyboardButton("✅ نعم، إنشاء", callback_data="exec_create_quiz_final")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="manage_control")]
            ]
            await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            context.user_data['action'] = None
            return


# --------------------------------------------------------------------------
    # --- [ جزء الطلاب والردود التفاعلية - g4f فقط ] ---
    
    # جلب إعدادات البوت أولاً لتعريف bot_owner_id قبل استخدامه في الشرط

    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # تنفيذ الشرط: إذا كان المرسل ليس هو المالك (أي أنه طالب)
    if user.id != bot_owner_id:
        # 1. فحص الكلمات المفتاحية (FAQ) لسرعة الرد
        faq_keywords = {
            "طريقة الدفع": "💳 يمكنك الدفع عبر (زين كاش، بايبال، أو كروت التعبئة).",
            "تفعيل": "🎟 لتفعيل الدورة، يرجى إرسال الكود الذي حصلت عليه.",
            "قائمة": "📚 يمكنك استعراض كافة الدورات المتاحة عبر الزر المخصص."
        }
        for key, response in faq_keywords.items():
            if key in text:
                await update.message.reply_text(response)
                return

        # 2. إدارة ذاكرة المحادثة وجلب البيانات من القاعدة
        global user_messages
        if user.id not in user_messages:
            user_messages[user.id] = []

        # جلب قاعدة المعرفة من القاعدة
       
        courses_knowledge = get_courses_knowledge_base(bot_token)
        
        # إضافة رسالة الطالب للذاكرة
        user_messages[user.id].append({"role": "user", "content": text})
        
        # --- [ الجزء الديناميكي الجديد: جلب الهوية من القاعدة ] ---
        
        ai_info = get_ai_setup(bot_token)
        platform = ai_info.get('اسم_المؤسسة', 'منصة الادارة التعليمية') if ai_info else "منصة الادارة التعليمية"
        rules = ai_info.get('تعليمات_AI', 'أجب بذكاء ولباقة واستخدم الرموز التعبيرية 🎓') if ai_info else "أجب بذكاء ولباقة"

        # بناء سياق المحادثة الكامل بالهوية الجديدة + الذاكرة
        messages_to_send = [
            {
                "role": "system", 
                "content": f"أنت المساعد الذكي الرسمي لـ {platform}. {rules}. إليك معلومات الدورات المتاحة حالياً:\n{courses_knowledge}"
            }
        ] + user_messages[user.id][-6:] # دمج الذاكرة لضمان استمرارية الحوار

        await update.message.reply_chat_action("typing")

        try:
            # استخدام g4f بشكل مباشر مع المزود التلقائي لضمان الاستقرار
            import g4f
            response = await g4f.ChatCompletion.create_async(
                model=g4f.models.default,
                messages=messages_to_send,
            )

            if response and len(response) > 0:
                # إضافة رد البوت للذاكرة وإرساله
                user_messages[user.id].append({"role": "assistant", "content": response})
                await update.message.reply_text(response)
                return
            else:
                raise Exception("Empty g4f Response")
            
        except Exception as e: # تصحيح الحرف الصغير هنا
            # الخطة البديلة: إرسال تنبيه للادارة في حال فشل المحرك
            print(f"❌ AI Error: {e}")
            
            # تم نقل جلب الإعدادات للأعلى لضمان توافر bot_owner_id
            info = f"📩 <b>استفسار طالب (فشل الـ AI):</b>\nالاسم: {user.full_name}\nالرسالة: {text}\nالخطأ: {str(e)}"
            
            try:
                # محاولة إرسال التنبيه للمالك إذا كان معرّفاً
                if bot_owner_id:
                    await context.bot.send_message(chat_id=bot_owner_id, text=info, parse_mode="HTML")
                
                # الرد على الطالب دائماً لضمان عدم بقاء المحادثة معلقة
                await update.message.reply_text("💡 شكراً لسؤالك! لقد استلمت استفسارك وسيقوم الادارة بالرد عليك فوراً.")
            except Exception as send_error:
                print(f"⚠️ فشل إرسال التنبيه للمالك: {send_error}")
                await update.message.reply_text("⚠️ المعذرة، هناك ضغط حالياً. يرجى المحاولة لاحقاً.")


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --- [ محرك التشغيل المتوافق مع المصنع ] ---

async def run_bot(token, owner_id):
    """هذه الدالة هي التي يستدعيها ملف main.py لتشغيل البوت ديناميكياً"""
    application = ApplicationBuilder().token(token).build()
    
    # 1. إضافة المعالجات (Handlers)
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CallbackQueryHandler(contact_callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_contact_message))
    
    # 2. إعداد مراقب التفعيل (يُوضع هنا بعد تعريف الـ application)
    # سيقوم بفحص القاعدة كل 60 ثانية وإرسال رسائل للطلاب المفعلين
    job_queue = application.job_queue
    job_queue.run_repeating(activation_monitor, interval=60, first=10)
    
    # 3. بدء تشغيل المحرك
    await application.initialize()
    await application.start()
    await application.updater.start_polling()






# --------------------------------------------------------------------------

