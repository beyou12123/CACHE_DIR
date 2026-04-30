
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
from ui_keyboards import get_coach_panel_keyboard, get_tech_settings_keyboard
import importlib.util
from datetime import datetime
from telegram.ext import CallbackQueryHandler, MessageHandler, filters

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

# --- [ 2. مكتبات معالجة البيانات والذكاء الاصطناعي ] ---
# محرك الذكاء الاصطناعي من جوجل (مع نظام حماية ضد الفشل)
try:
    import google.generativeai as genai
    AI_ENABLED = True
except (ImportError, ModuleNotFoundError):
    genai = None  # أضف هذا السطر لتعريف المتغير كـ None ومنع خطأ NameError
    AI_ENABLED = False
    print("⚠️ تنبيه: مكتبة google-generativeai غير مثبتة في البيئة الحالية.")


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
    # فحص الرد التلقائي أولاً قبل معالجة الذكاء الاصطناعي

    if await auto_reply_engine(update, context):
        return  # إذا تم إيجاد رد تلقائي، نكتفي به وننهي الدالة

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
    # معالجة المستندات (التي تحتوي على ملف النسخة الاحتياطية المشفرة)
    if update.message.document:
        doc = update.message.document
        action = context.user_data.get('action')

        # الاستبدال الاحترافي: ربط مستلم الملفات بمحرك الاستعادة الشامل
        if action == 'awaiting_json_backup' and doc.file_name.endswith('.json'):
            # استدعاء الكائن المركزي الموحد للنظام
             
            
            status_msg = await update.message.reply_text("⏳ جاري فك التشفير ومزامنة البيانات مع السحابة (V7-Restore)...")
            
            try:
                # 1. تحميل محتوى الملف
                file = await context.bot.get_file(doc.file_id)
                content = await file.download_as_bytearray()

                # 2. تنفيذ الاستعادة عبر الدالة الموجودة داخل الكلاس
                # نمرر المحتوى (content) ومعرف المستخدم (user_id) للتحقق من الصلاحية
                success = await db_manager.restore_from_telegram(
                    file_content=content, 
                    user_id=update.effective_user.id
                )
                
                if success:
                    await status_msg.edit_text(
                        "✅ **تم استعادة البيانات وشحن الرام بنجاح!**\n\n"
                        "🌐 **حالة السحابة:** تم تحديث الجداول في Google Sheets.\n"
                        "⚠️ **حالة الكاش:** البيانات تعمل الآن في البوت (RAM).\n"
                        "⏰ **المزامنة:** سيتم تحديث النسخة الاحتياطية آلياً الساعة 12:00 ليلاً."
                    )
                else:
                    await status_msg.edit_text("❌ **فشل الاستيراد:** الملف غير متوافق مع نظام التشفير أو التوكن غير صحيح.")
            
            except Exception as e:
                print(f"❌ خطأ حرج في محرك الاستعادة: {e}")
                await status_msg.edit_text("❌ حدث خطأ فني أثناء محاولة فك تشفير البيانات.")

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

    # جلب إعدادات البوت والمالك
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

        # 2. إدارة ذاكرة المحادثة (تم استبدال global بـ context.user_data لضمان فصل البوتات)
        if 'chat_history' not in context.user_data:
            context.user_data['chat_history'] = []

        # جلب قاعدة المعرفة من القاعدة
        courses_knowledge = get_courses_knowledge_base(bot_token)
        
        # إضافة رسالة الطالب للذاكرة
        context.user_data['chat_history'].append({"role": "user", "content": text})
        
        # --- [ الجزء الديناميكي الجديد: جلب الهوية من القاعدة/الكاش ] ---
        ai_info = get_ai_setup(bot_token)
        platform = ai_info.get('اسم_المؤسسة', 'منصة الادارة التعليمية') if ai_info else "منصة الادارة التعليمية"
        rules = ai_info.get('تعليمات_AI', 'أجب بذكاء ولباقة واستخدم الرموز التعبيرية 🎓') if ai_info else "أجب بذكاء ولباقة"

        # بناء سياق المحادثة الكامل بالهوية الجديدة + الذاكرة
        messages_to_send = [
            {
                "role": "system", 
                "content": f"أنت المساعد الذكي الرسمي لـ {platform}. {rules}. إليك معلومات الدورات المتاحة حالياً:\n{courses_knowledge}"
            }
        ] + context.user_data['chat_history'][-6:] # دمج الذاكرة (آخر 6 رسائل)

        await update.message.reply_chat_action("typing")

        try:
            # استخدام g4f بشكل مباشر مع المزود التلقائي كما طلبت
            
            response = await g4f.ChatCompletion.create_async(
                model=g4f.models.default,
                messages=messages_to_send,
            )

            if response and len(response) > 0:
                # إضافة رد البوت للذاكرة وإرساله
                context.user_data['chat_history'].append({"role": "assistant", "content": response})
                await update.message.reply_text(response)
                return
            else:
                raise Exception("Empty g4f Response")
            
        except Exception as e: 
            # الخطة البديلة: إرسال تنبيه للادارة في حال فشل المحرك
            print(f"❌ AI Error: {e}")
            
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
