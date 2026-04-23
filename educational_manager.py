from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from datetime import datetime
import uuid
import time
import random
import string
import re

# استيراد الدوال من ملف sheets (تأكد من مطابقة أسماء الدوال لما هو موجود في ملف sheets.py)
from sheets import (
    get_groups_by_course, 
    add_new_group, 
    ss, 
    get_all_coaches, 
    get_employee_allowed_courses,
    get_employee_allowed_groups,
    get_student_enrollment_data,
    get_lectures_by_group,
    get_active_discount_codes,
    check_user_permission, # للتأكد من هوية الموظف
    delete_group_by_id  # تم تصحيح الاسم هنا ليتطابق مع المحرك
)

async def manage_groups_main(update, context, course_id):
    """الواجهة الرئيسية لإدارة مجموعات دورة محددة"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # 1. جلب المجموعات الحالية
    groups = get_groups_by_course(bot_token, course_id)
    
    keyboard = []
    # 2. عرض المجموعات كأزرار (تعديل/عرض)
    if groups:
        for g in groups:
            # استخدام get لتجنب أخطاء المفاتيح المفقودة
            label = f"👥 {g.get('اسم_المجموعة', 'بلا اسم')} | 👤 {g.get('عدد_الطلاب_الحالي', 0)}/{g.get('سعة_المجموعة', 30)}"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"grp_show_{g.get('معرف_المجموعة')}")])
    
    # 3. أزرار التحكم الثابتة
    keyboard.append([InlineKeyboardButton("➕ إنشاء مجموعة جديدة", callback_data=f"grp_add_start_{course_id}")])
    keyboard.append([
        InlineKeyboardButton("➕ إضافة ملف للمكتبة", callback_data=f"add_lib_file_{course_id}")
    ])    
    keyboard.append([InlineKeyboardButton("🔙 عودة للدورة", callback_data=f"manage_crs_{course_id}")])
    
    text = (
        f"📂 <b>نظام إدارة المجموعات</b>\n"
        f"الدورة: <code>{course_id}</code>\n\n"
        f"يمكنك متابعة سعة المجموعات وتعيين المعلمين من هنا."
    )
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def group_details_ui(update, context, group_id):
    """واجهة خيارات المجموعة (تعديل، حذف، رابط)"""
    query = update.callback_query
    # هنا سنعرض خيارات: تعديل الوقت، تغيير المعلم، حذف المجموعة
    keyboard = [
        [InlineKeyboardButton("📝 تعديل البيانات", callback_data=f"grp_mod_{group_id}"),
         InlineKeyboardButton("🔗 رابط المجموعة", callback_data=f"grp_link_{group_id}")],
        [InlineKeyboardButton("🗑️ حذف المجموعة", callback_data=f"grp_confirm_del_{group_id}")],
        [InlineKeyboardButton("🔙 عودة للمجموعات", callback_data="manage_group_back")] 
    ]
    await query.edit_message_text(f"🛠 <b>إدارة المجموعة:</b> <code>{group_id}</code>", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# دالة الحفظ (تتم معالجتها في المحرك الرئيسي عبر callback exec_save_group)

# 1. بدء عملية إضافة مجموعة (الاسم)
async def start_add_group(update: Update, context: ContextTypes.DEFAULT_TYPE, course_id: str):
    query = update.callback_query
    context.user_data['action'] = 'awaiting_grp_name'
    context.user_data['temp_grp'] = {'course_id': course_id}
    
    await query.edit_message_text(
        f"✍️ <b>إضافة مجموعة جديدة للدورة:</b> <code>{course_id}</code>\n"
        f"الخطوة 1: أرسل <b>اسم المجموعة</b> (مثلاً: مجموعة التميز - صباحي):",
        parse_mode="HTML"
    )

# 2. طلب أيام الدراسة
async def process_grp_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    context.user_data['temp_grp']['name'] = text
    context.user_data['action'] = 'awaiting_grp_days'
    
    await update.message.reply_text(
        "📅 <b>الخطوة 2:</b> أرسل <b>أيام الدراسة</b> (مثلاً: السبت، الاثنين، الأربعاء):",
        parse_mode="HTML"
    )

# 3. طلب توقيت الدراسة
async def process_grp_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    context.user_data['temp_grp']['days'] = text
    context.user_data['action'] = 'awaiting_grp_time'
    
    await update.message.reply_text(
        "⏰ <b>الخطوة 3:</b> أرسل <b>توقيت الدراسة</b> (مثلاً: من 4:00 م إلى 6:00 م):",
        parse_mode="HTML"
    )

# 4. طلب سعة المجموعة واختيار المعلم
async def process_grp_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    context.user_data['temp_grp']['time'] = text
    bot_token = context.bot.token
    
    # جلب المدربين لاختيار أحدهم
    coaches = get_all_coaches(bot_token)
    keyboard = []
    if coaches:
        for c in coaches:
            keyboard.append([InlineKeyboardButton(f"👨‍🏫 {c['name']}", callback_data=f"sel_teacher_{c['id']}")])
    
    keyboard.append([InlineKeyboardButton("❌ إلغاء", callback_data="manage_educational")])
    
    await update.message.reply_text(
        "👨‍🏫 <b>الخطوة 4:</b> اختر <b>المعلم المسؤول</b> عن هذه المجموعة من القائمة:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

# 5. المراجعة النهائية والحفظ
async def confirm_group_save(update: Update, context: ContextTypes.DEFAULT_TYPE, teacher_id: str, teacher_name: str):
    query = update.callback_query
    d = context.user_data.get('temp_grp')
    
    # توليد ID فريد للمجموعة
    group_id = f"GRP{str(uuid.uuid4().int)[:4]}"
    
    summary = (
        f"📝 <b>مراجعة بيانات المجموعة الجديدة:</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"📚 الدورة: <code>{d['course_id']}</code>\n"
        f"👥 الاسم: {d['name']}\n"
        f"📅 الأيام: {d['days']}\n"
        f"⏰ الوقت: {d['time']}\n"
        f"👨‍🏫 المعلم: {teacher_name}\n"
        f"━━━━━━━━━━━━━━\n"
        f"هل تريد الحفظ والاعتماد؟"
    )
    
    context.user_data['temp_grp']['teacher_id'] = teacher_id
    context.user_data['temp_grp']['group_id'] = group_id
    
    keyboard = [
        [InlineKeyboardButton("✅ نعم، احفظ الآن", callback_data="exec_save_group")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="manage_educational")]
    ]
    await query.edit_message_text(summary, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
 
async def group_options_ui(update, context, group_id):
    """واجهة التحكم في مجموعة محددة (تعديل/حذف)"""
    query = update.callback_query
    # زر الحذف وزر التعديل
    keyboard = [
        [
            InlineKeyboardButton("📝 تعديل الاسم", callback_data=f"grp_edit_name_{group_id}"),
            InlineKeyboardButton("⏰ تعديل الوقت", callback_data=f"grp_edit_time_{group_id}")
        ],
        [
            InlineKeyboardButton("👨‍🏫 تغيير المعلم", callback_data=f"grp_edit_teacher_{group_id}"),
            InlineKeyboardButton("🚫 تغيير الحالة", callback_data=f"grp_edit_status_{group_id}")
        ],
        [InlineKeyboardButton("🗑️ حذف المجموعة نهائياً", callback_data=f"grp_confirm_del_{group_id}")],
        [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="manage_educational")] 
    ]
    
    await query.edit_message_text(
        f"🛠️ <b>إدارة المجموعة:</b> <code>{group_id}</code>\nاختر الإجراء المطلوب من القائمة أدناه:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

async def confirm_delete_group_ui(update, context, group_id):
    """واجهة تأكيد الحذف لمنع الحذف بالخطأ"""
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("✅ نعم، احذف", callback_data=f"grp_exec_del_{group_id}")],
        [InlineKeyboardButton("❌ تراجع", callback_data=f"grp_show_{group_id}")]
    ]
    await query.edit_message_text(
        f"⚠️ <b>تحذير:</b> هل أنت متأكد من حذف المجموعة <code>{group_id}</code>؟\nلا يمكن التراجع عن هذا الإجراء.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

 
# --------------------------------------------------------------------------
#ادارة الاختبارات والاسئلة 
async def manage_control_ui(update, context):
    """واجهة الكنترول التعليمي الرئيسية - النسخة الديناميكية المعتمدة على الكاش"""
    query = update.callback_query
    user_id = update.effective_user.id
    bot_token = context.bot.token
    
    # 1. استدعاء الكاش المركزي وإعدادات البوت
    from cache_manager import FACTORY_GLOBAL_CACHE
    from sheets import get_bot_config
    
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # 2. تحديد وجهة زر العودة الافتراضية
    back_callback = "main_menu" 

    # 3. فحص الرتبة (التسلسل الهرمي)
    if user_id == bot_owner_id:
        back_callback = "tech_settings"
    else:
        # استخراج بيانات الموظفين من الكاش (كتلة البيانات)
        employees_data = FACTORY_GLOBAL_CACHE["data"].get("إدارة_الموظفين", [])
        
        if employees_data:
            # البحث الذكي: نحول الـ ID لنص لضمان المطابقة
            # نستخدم .get() للوصول للقيم بأسماء الأعمدة لضمان الدقة
            user_row = next((row for row in employees_data if str(row.get('معرف_الموظف')) == str(user_id)), None)
            
            if user_row:
                # جلب الرتبة من العمود المخصص (الرتبة)
                user_role = str(user_row.get('الرتبة', '')).strip()
                if user_role == "مدير النظام":
                    back_callback = "get_admin_panel"
                elif user_role == "مدرب":
                    back_callback = "get_coach_panel"
                elif user_role == "موظف":
                    back_callback = "get_employee_panel"

    # 4. بناء لوحة المفاتيح
    keyboard = [
        [InlineKeyboardButton("📝 إدارة الاختبارات", callback_data="manage_quizzes"),
         InlineKeyboardButton("📚 بنك الأسئلة", callback_data="manage_q_bank")],
        [InlineKeyboardButton("📝 سجل الإجابات", callback_data="view_exam_logs"),
         InlineKeyboardButton("📑 إدارة الواجبات", callback_data="manage_homeworks")],
        [InlineKeyboardButton("🔙 عودة", callback_data=back_callback)]
    ]

    await query.edit_message_text(
        "🎮 <b>مرحباً بك في غرفة الكنترول التعليمي:</b>\n"
        "من هنا يمكنك التحكم في الاختبارات، بنك الأسئلة، وتصحيح الواجبات.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )



async def q_bank_manager_ui(update, context):
    """واجهة إدارة بنك الأسئلة (إضافة/عرض/حذف/تعديل)"""
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("➕ إضافة سؤال يدوي", callback_data="add_q_manual")],
        [InlineKeyboardButton("📥 استيراد أسئلة (Excel)", callback_data="import_q_excel")],
        [InlineKeyboardButton("🔍 استعراض وحذف الأسئلة", callback_data="browse_q_bank")],
        [InlineKeyboardButton("🔙 عودة للكنترول", callback_data="manage_control")]
    ]
    await query.edit_message_text(
        "🗄 <b>مخزن بنك الأسئلة:</b>\n"
        "يمكنك بناء قاعدة بيانات لأسئلتك واستخدامها في عدة اختبارات لاحقاً.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def question_bank_ui(update, context):
    """واجهة بنك الأسئلة"""
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("➕ إضافة سؤال يدوي", callback_data="add_q_manual")],
        [InlineKeyboardButton("📥 استيراد أسئلة (Excel)", callback_data="import_q_excel")],
        [InlineKeyboardButton("🔍 استعراض الأسئلة", callback_data="browse_questions")],
        [InlineKeyboardButton("🔙 عودة للكنترول", callback_data="manage_control")]
    ]
    await query.edit_message_text(
        "🗄 <b>مخزن بنك الأسئلة:</b>\n"
        "قم ببناء قاعدة أسئلتك لاستخدامها في الاختبارات الآلية لاحقاً.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
 


# --------------------------------------------------------------------------
async def quiz_options_ui(update, context, quiz_id):
    """واجهة التحكم في الاختبار للأدمن (مع زر الظهور/الإخفاء)"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # جلب بيانات الاختبار لمعرفة حالته الحالية
    from sheets import ss
    sheet = ss.worksheet("الاختبارات_الآلية")
    all_q = sheet.get_all_records()
    quiz = next((q for q in all_q if str(q['معرف_الاختبار']) == str(quiz_id)), {})
    
    status = str(quiz.get('حالة_الاختبار', 'FALSE')).upper()
    icon = "👁️ متاح للموظفين" if status == "TRUE" else "🚫 مخفي عن الموظفين"
    
    keyboard = [
        [InlineKeyboardButton(f"{icon}", callback_data=f"q_toggle_vis_{quiz_id}")],
        [InlineKeyboardButton("🗑️ حذف الاختبار", callback_data=f"quiz_confirm_del_{quiz_id}")],
        [InlineKeyboardButton("🔙 عودة", callback_data="manage_quizzes")]
    ]
    
    await query.edit_message_text(
        f"🛠️ <b>إدارة الاختبار:</b> <code>{quiz_id}</code>\n"
        f"الحالة الحالية: <b>{status}</b>",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
    )
    
async def manage_personnel_ui(update, context):
    """عرض القائمة الموحدة للموظفين والمدربين من دالتك الأصلية"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # استخدام دالتك الموحدة
    from sheets import get_all_personnel_list
    people = get_all_personnel_list(bot_token)
    
    if not people:
        await query.edit_message_text("⚠️ لا يوجد موظفون أو مدربون مسجلون حالياً.")
        return

    keyboard = []
    for p in people:
        label = f"👤 {p['name']} ({p['type']})"
        # عند الضغط، نرسل الـ ID لعملية التأسيس والضبط
        keyboard.append([InlineKeyboardButton(label, callback_data=f"setup_p_perms_{p['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")])
    await query.edit_message_text("👥 <b>إدارة الصلاحيات والمهام:</b>\nاختر الشخص للبدء بضبط مهامه:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
 
# --------------------------------------------------------------------------
# دالة اختيار الدورة لربط السؤال بها
async def start_add_question_ui(update, context):
    """الخطوة 1: اختيار الدورة لربط السؤال بها في بنك الأسئلة"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # جلب الدورات الخاصة بهذا البوت فقط
    from sheets import courses_sheet
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
    
    if not bot_courses:
        await query.edit_message_text("⚠️ لا توجد دورات حالياً، يجب إضافة دورة أولاً لربط الأسئلة بها.")
        return

    keyboard = [[InlineKeyboardButton(f"📘 {c['اسم_الدورة']}", callback_data=f"sel_q_crs_{c['معرف_الدورة']}")] for c in bot_courses]
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_q_bank")])
    
    await query.edit_message_text("🎯 <b>إضافة سؤال يدوي:</b>\nاختر الدورة التي يتبع لها هذا السؤال لربطها ببنك الأسئلة:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")




# --------------------------------------------------------------------------
# واجهة لعرض الأسئلة كأزرار 
async def browse_q_bank_ui(update, context):
    """عرض قائمة الأسئلة الموجودة في البنك لاختيار أحدها - تعتمد على الكاش المركزي"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # جلب البيانات من الكاش المركزي مباشرة
    from cache_manager import FACTORY_GLOBAL_CACHE
    all_questions = FACTORY_GLOBAL_CACHE["data"].get("بنك_الأسئلة", [])
    
    # تصفية الأسئلة الخاصة بهذا البوت فقط
    questions = [q for q in all_questions if str(q.get('bot_id')) == str(bot_token)]
    
    if not questions:
        await query.edit_message_text(
            "🗄 <b>بنك الأسئلة فارغ حالياً.</b>\nابدأ بإضافة أسئلة أولاً لتظهر هنا.", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="manage_q_bank")]]), 
            parse_mode="HTML"
        )
        return

    keyboard = []
    # عرض آخر 10 أسئلة مضافة (لضمان السرعة ومواكبة التحديثات)
    for q in questions[-10:]:
        q_text_val = q.get('نص_السؤال', 'بدون نص')
        q_id = q.get('معرف_السؤال')
        
        display_text = (q_text_val[:30] + '..') if len(q_text_val) > 30 else q_text_val
        keyboard.append([InlineKeyboardButton(f"❓ {display_text}", callback_data=f"view_q_det_{q_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_q_bank")])
    
    await query.edit_message_text(
        f"🔍 <b>استعراض الأسئلة ({len(questions)} سؤال):</b>\nاختر سؤالاً لعرض تفاصيله أو حذفه:", 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode="HTML"
    )

async def view_question_details_ui(update, context, q_id):
    """عرض تفاصيل السؤال مع زر الحذف - قراءة من الكاش"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # جلب البيانات من الكاش المركزي
    from cache_manager import FACTORY_GLOBAL_CACHE
    all_questions = FACTORY_GLOBAL_CACHE["data"].get("بنك_الأسئلة", [])
    
    # البحث عن السؤال المطابق لمعرف السؤال وتوكن البوت
    q = next((item for item in all_questions if str(item.get('معرف_السؤال')) == str(q_id) 
              and str(item.get('bot_id')) == str(bot_token)), None)
    
    if not q:
        await query.answer("⚠️ تعذر العثور على بيانات السؤال في الكاش.")
        return

    text = (
        f"📝 <b>تفاصيل السؤال:</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"❓ <b>السؤال:</b> {q.get('نص_السؤال')}\n"
        f"🅰️ الخيار A: {q.get('الخيار_A')}\n"
        f"🅱️ الخيار B: {q.get('الخيار_B')}\n"
        f"🆃 الخيار C: {q.get('الخيار_C')}\n"
        f"🅳 الخيار D: {q.get('الخيار_D')}\n\n"
        f"✅ <b>الإجابة الصحيحة:</b> {q.get('الإجابة_الصحيحة')}\n"
        f"🎯 الدرجة: {q.get('الدرجة')} | 📊 المستوى: {q.get('مستوى_الصعوبة')}\n"
        f"📚 الدورة: <code>{q.get('معرف_الدورة')}</code>\n"
        f"━━━━━━━━━━━━━━"
    )
    
    keyboard = [
        [InlineKeyboardButton("🗑️ حذف السؤال نهائياً", callback_data=f"exec_del_q_{q_id}")],
        [InlineKeyboardButton("🔙 عودة للقائمة", callback_data="browse_q_bank")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


# --------------------------------------------------------------------------
# بدء عملية إنشاء الاختبارات واختيار المجموعات والدورات
async def quiz_create_start_ui(update, context):
    """الخطوة 1: اختيار الدورة المراد إنشاء اختبار آلي لها"""
    query = update.callback_query
    bot_token = context.bot.token
    
    from sheets import courses_sheet
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
    
    if not bot_courses:
        await query.edit_message_text("⚠️ يجب إضافة دورة تعليمية أولاً لتتمكن من إنشاء اختبار لها.")
        return

    keyboard = [[InlineKeyboardButton(f"📘 {c['اسم_الدورة']}", callback_data=f"q_gen_crs_{c['معرف_الدورة']}")] for c in bot_courses]
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_control")])
    
    await query.edit_message_text("📝 <b>إنشاء اختبار آلي جديد:</b>\nاختر الدورة التدريبية المراد سحب الأسئلة لها:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def quiz_gen_select_groups_ui(update, context, course_id):
    """الخطوة 2: اختيار المجموعات المستهدفة (نظام الاختيار المتعدد)"""
    query = update.callback_query
    bot_token = context.bot.token
    
    from sheets import get_groups_by_course
    groups = get_groups_by_course(bot_token, course_id)
    
    if 'temp_quiz' not in context.user_data:
        context.user_data['temp_quiz'] = {'course_id': course_id, 'target_groups': []}

    keyboard = []
    # زر "الكل"
    all_icon = "✅" if "ALL" in context.user_data['temp_quiz']['target_groups'] else "⬜"
    keyboard.append([InlineKeyboardButton(f"{all_icon} كافة المجموعات", callback_data=f"q_gen_grp_ALL_{course_id}")])
    
    for g in groups:
        g_id = str(g['معرف_المجموعة'])
        icon = "✅" if g_id in context.user_data['temp_quiz']['target_groups'] else "⬜"
        keyboard.append([InlineKeyboardButton(f"{icon} {g['اسم_المجموعة']}", callback_data=f"q_gen_grp_{g_id}_{course_id}")])
    
    keyboard.append([InlineKeyboardButton("🚀 الخطوة التالية (الإعدادات)", callback_data=f"q_gen_next_settings")])
    keyboard.append([InlineKeyboardButton("🔙 تراجع", callback_data="manage_control")])
    
    await query.edit_message_text(f"👥 <b>تحديد المجموعات:</b>\nاختر المجموعات التي سيظهر لها الاختبار في لوحتهم:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")



# --------------------------------------------------------------------------
#جدول المحاضرات
# --- [ دالة عرض الجداول المحدثة بمنطق المالك المطلق ] ---

async def show_lectures_logic(update, context):
    """المنطق الذكي والمطلق لعرض المحاضرات (طالب، موظف، أو مالك)"""
    query = update.callback_query
    user_id = update.effective_user.id
    bot_token = context.bot.token
    
    # 1. جلب معرف المالك من الإعدادات لضمان الصلاحية المطلقة
    from sheets import get_bot_config, courses_sheet
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # --- [ أ: مسار المالك - الوصول الشامل بدون قيود ] ---
    if user_id == bot_owner_id:
        # جلب كافة الدورات المسجلة لهذا البوت من ورقة الدورات مباشرة
        all_courses = courses_sheet.get_all_records()
        owner_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
        
        if owner_courses:
            keyboard = []
            for course in owner_courses:
                # عرض كافة الدورات للمالك ليختار أي مجموعة يريد رؤية جدولها
                keyboard.append([InlineKeyboardButton(f"📚 {course['اسم_الدورة']}", callback_data=f"lec_course_{course['معرف_الدورة']}")])
            
            keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة", callback_data="main_menu")])
            await query.edit_message_text(
                "👑 <b>مرحباً بك يا دكتور (المالك):</b>\nتفضل باختيار الدورة لعرض جداول مجموعاتها بالكامل:", 
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
            )
        else:
            await query.answer("⚠️ لا توجد أي دورات مضافة في المنصة حالياً.", show_alert=True)
        return # إنهاء الدالة للمالك

    # --- [ ب: مسار الطالب - جلب بياناته المسجلة ] ---
    student_data = get_student_enrollment_data(bot_token, user_id)
    
    if student_data:
        lectures = get_lectures_by_group(bot_token, student_data['group_id'])
        msg = (
            f"<b>🗓 جدول محاضراتك يا {student_data['student_name']}</b>\n"
            f"📚 الدورة: {student_data['course_name']}\n"
            f"👥 المجموعة: {student_data['group_name']}\n"
            f"---------------------------\n"
        )
        keyboard = []
        if lectures:
            for lec in lectures:
                msg += (
                    f"🔹 <b>{lec.get('التاريخ')} ({lec.get('اليوم')})</b>\n"
                    f"⏰ الوقت: {lec.get('وقت_البداية')} - {lec.get('وقت_النهاية')}\n"
                    f"👨‍🏫 المدرب: {lec.get('اسم_المدرب')}\n"
                    f"📝 ملاحظة: {lec.get('ملاحظات', '-')}\n\n"
                )
                if lec.get('رابط_الحصة') and str(lec.get('رابط_الحصة')).startswith('http'):
                    keyboard.append([InlineKeyboardButton(f"🔗 انضم لمحاضرة {lec.get('التاريخ')}", url=lec.get('رابط_الحصة'))])
        else:
            msg += "⚠️ لا توجد محاضرات مجدولة حالياً لهذه المجموعة."
            
        keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data="main_menu")])
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        
    else:
        # --- [ ج: مسار الموظف - بناءً على الصلاحيات المقيدة في القاعدة ] ---
        allowed_courses = get_employee_allowed_courses(bot_token, user_id)
        
        if allowed_courses:
            keyboard = []
            for course in allowed_courses:
                keyboard.append([InlineKeyboardButton(f"📚 {course['name']}", callback_data=f"lec_course_{course['id']}")])
            
            keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data="main_menu")])
            await query.edit_message_text(
                "⚙️ <b>لوحة الموظف:</b>\nيرجى اختيار الدورة لعرض مجموعاتها المسموحة لك:", 
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
            )
        else:
            # دالة الحماية النهائية: تنبيه المستخدم غير المسجل بدلاً من الصمت
            await query.answer("⚠️ تنبيه: هويتك غير مسجلة كطالب أو موظف في قاعدة بيانات هذا البوت.", show_alert=True)

# --------------------------------------------------------------------------
#دالة إضافة الاكود 
# 1. بداية العملية وجلب الدورات
# --- [ النسخة المصححة لضمان عمل الأزرار ] ---
async def add_discount_start(update, context):
    query = update.callback_query
    from sheets import courses_sheet
    all_courses = courses_sheet.get_all_records()
    bot_token = str(context.bot.token)
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == bot_token]
    
    keyboard = []
    for c in bot_courses:
        # قمنا باختصار dsc_check_ إلى d_ch_ لتوفير مساحة للـ ID
        course_id = str(c['معرف_الدورة'])
        keyboard.append([InlineKeyboardButton(f"📚 {c['اسم_الدورة']}", callback_data=f"d_ch_{course_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 إلغاء", callback_data="discount_codes")])
    
    await query.edit_message_text(
        "🎯 <b>الخطوة 1:</b> اختر الدورة المراد إنشاء كود خصم لها:", 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode="HTML"
    )


# 2. التحقق من وجود كود سابق
async def process_dsc_check(update, context, course_id):
    query = update.callback_query
    from sheets import check_course_has_discount
    existing_code = check_course_has_discount(context.bot.token, course_id)
    
    context.user_data['temp_disc'] = {'course_id': course_id}
    
    if existing_code:
        msg = f"⚠️ <b>تنبيه:</b> يوجد كود خصم سابق لهذه الدورة وهو: <code>{existing_code}</code>\n\nهل تريد إضافة كود خصم آخر؟"
        keyboard = [
            [InlineKeyboardButton("✅ نعم", callback_data="dsc_continue"), 
             InlineKeyboardButton("❌ لا", callback_data="discount_codes")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    else:
        await process_dsc_ask_desc(update, context)

# 3. طلب الاسم الوصفي (شرط 8 حروف)
async def process_dsc_ask_desc(update, context):
    context.user_data['action'] = 'awaiting_dsc_desc'
    msg = "📝 <b>الخطوة 2:</b> أرسل الاسم الوصفي للكود (مثلاً: خصم عيد):\n⚠️ يجب ألا يزيد عن 8 حروف."
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_text(msg, parse_mode="HTML")

async def validate_dsc_desc(update, context):
    text = update.message.text.strip()
    if len(text) > 8:
        await update.message.reply_text("❌ <b>خطأ:</b> الاسم طويل جداً، أرسل اسماً لا يزيد عن 8 حروف:", parse_mode="HTML")
        return
    context.user_data['temp_disc']['desc'] = text
    context.user_data['action'] = 'awaiting_dsc_value'
    await update.message.reply_text("💰 <b>الخطوة 3:</b> أرسل قيمة الخصم كرقـم فقط (مثلاً 10 لتعني 10%):", parse_mode="HTML")

# 4. طلب التاريخ (تنسيق محدد)
async def validate_dsc_value(update, context):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ <b>خطأ:</b> يرجى إرسال أرقام فقط (مثلاً: 15):", parse_mode="HTML")
        return
    context.user_data['temp_disc']['value'] = text
    context.user_data['action'] = 'awaiting_dsc_expiry'
    await update.message.reply_text("📅 <b>الخطوة 4:</b> أرسل تاريخ انتهاء الكود بصيغة <code>YYYY-MM-DD</code>:\nمثال: <code>2026-12-31</code>", parse_mode="HTML")

# 5. طلب الحد الأقصى (أرقام إنجليزية فقط)
async def validate_dsc_expiry(update, context):
    text = update.message.text.strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        await update.message.reply_text("❌ <b>خطأ في التنسيق!</b> أرسل التاريخ بهذا الشكل <code>2026-01-01</code> حصراً:", parse_mode="HTML")
        return
    context.user_data['temp_disc']['expiry'] = text
    context.user_data['action'] = 'awaiting_dsc_max'
    await update.message.reply_text("🔢 <b>الخطوة 5:</b> أرسل <b>الحد الأقصى لاستخدام الكود</b> (أرقام إنجليزية فقط):", parse_mode="HTML")

# 6. التوليد النهائي والحفظ
async def validate_dsc_max(update, context):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ <b>خطأ:</b> أرسل أرقاماً إنجليزية فقط:", parse_mode="HTML")
        return
    
    d = context.user_data['temp_disc']
    d['max_use'] = text
    
    # توليد الكود (6 حروف كبيرة + رقمين)
    chars = ''.join(random.choices(string.ascii_uppercase, k=6))
    nums = ''.join(random.choices(string.digits, k=2))
    d['final_code'] = chars + nums
    
    from sheets import save_discount_code_full
    if save_discount_code_full(context.bot.token, d):
        summary = (
            f"✅ <b>تم إنشاء كود الخصم بنجاح!</b>\n\n"
            f"🎫 الكود: <code>{d['final_code']}</code>\n"
            f"📝 الوصف: {d['desc']}\n"
            f"💰 القيمة: {d['value']}% | 🔢 السعة: {d['max_use']}\n"
            f"📅 ينتهي في: {d['expiry']}"
        )
        await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة", callback_data="discount_codes")]]), parse_mode="HTML")
        context.user_data['action'] = None
    else:
        await update.message.reply_text("❌ حدث خطأ أثناء الحفظ في القاعدة.")


# --------------------------------------------------------------------------
#دالة عرض اكود الخصم 
# --- [ دالة إدارة وأكواد الخصم المحدثة للمالك والطلاب ] ---

async def show_discount_codes_logic(update, context):
    """إدارة أكواد الخصم: عرض للطلاب ولوحة تحكم شاملة للمالك"""
    query = update.callback_query
    user_id = update.effective_user.id
    bot_token = context.bot.token
    
    # 1. جلب معرف المالك للتحقق من الصلاحية المطلقة
    from sheets import get_bot_config, ss
    config = get_bot_config(bot_token)
    bot_owner_id = int(config.get("admin_ids", 0))

    # --- [ أ: مسار المالك - لوحة التحكم الكاملة ] ---
    if user_id == bot_owner_id:
        keyboard = [
            [InlineKeyboardButton("➕ إضافة كود جديد", callback_data="add_discount_start")],
            [InlineKeyboardButton("🔍 عرض وإدارة الأكواد", callback_data="list_all_discounts")],
            [InlineKeyboardButton("🔙 العودة للقائمة", callback_data="main_menu")]
        ]
        await query.edit_message_text(
            "🎟 <b>لوحة إدارة أكواد الخصم (المالك):</b>\n"
            "يمكنك إضافة أكواد جديدة أو تعديل وحذف الأكواد الحالية من هنا.",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
        )
        return

    # --- [ ب: مسار الطالب - العرض البسيط للأكواد النشطة فقط ] ---
    codes = get_active_discount_codes(bot_token)
    msg = "<b>🎟 أكواد الخصم المتاحة حالياً:</b>\n\n"
    if codes:
        for c in codes:
            msg += (
                f"✅ الكود: <code>{c['code']}</code>\n"
                f"💰 الخصم: <b>{c['value']}</b>\n"
                f"📖 مخصص لـ: {c['course']}\n"
                f"📅 ينتهي في: {c['expiry']}\n"
                f"---------------------------\n"
            )
    else:
        msg += "⚠️ لا توجد أكواد خصم نشطة حالياً."
        
    keyboard = [[InlineKeyboardButton("🔙 العودة", callback_data="main_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --- [ ج: دالة عرض الأكواد كأزرار بناءً على عمود الوصف ] ---

async def list_all_discounts_ui(update, context):
    """عرض كافة الأكواد للمالك كأزرار تحمل 'الوصف'"""
    query = update.callback_query
    bot_token = context.bot.token
    
    # جلب البيانات مباشرة من ورقة أكواد_الخصم
    from sheets import ss
    sheet = ss.worksheet("أكواد_الخصم")
    records = sheet.get_all_records()
    
    keyboard = []
    # تصفية الأكواد التابعة لهذا البوت فقط
    bot_codes = [r for r in records if str(r.get('bot_id')) == str(bot_token)]
    
    if bot_codes:
        for r in bot_codes:
            # استخدام عمود "الوصف" كاسم للزر كما طلبت
            btn_label = r.get('الوصف') if r.get('الوصف') else f"كود: {r.get('معرف_الخصم')}"
            keyboard.append([InlineKeyboardButton(f"🎫 {btn_label}", callback_data=f"view_disc_{r.get('معرف_الخصم')}")])
    else:
        await query.answer("⚠️ لا توجد أكواد مسجلة حالياً.", show_alert=True)
        return

    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="discount_codes")])
    await query.edit_message_text("🎯 <b>اختر الكود المراد إدارته:</b>", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --- [ د: عرض معلومات الكود التفصيلية للمالك ] ---

async def view_discount_details_ui(update, context, disc_id):
    """عرض بيانات الكود كاملة (بما فيها التاريخ) مع أزرار التحكم"""
    query = update.callback_query
    bot_token = context.bot.token
    
    from sheets import ss
    sheet = ss.worksheet("أكواد_الخصم")
    records = sheet.get_all_records()
    d = next((r for r in records if str(r.get('bot_id')) == str(bot_token) and str(r.get('معرف_الخصم')) == str(disc_id)), None)
    
    if d:
        status = d.get('الحالة')
        # إعداد زر التعطيل/التفعيل ديناميكياً
        toggle_label = "🔴 تعطيل الكود" if status == "نشط" else "🟢 تفعيل الكود"
        toggle_callback = f"dsc_tog_{disc_id}_{'off' if status == 'نشط' else 'on'}"

        # إعادة كافة التواريخ والبيانات للنص
        text = (
            f"ℹ️ <b>تفاصيل كود الخصم:</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🎫 الكود: <code>{d.get('معرف_الخصم')}</code>\n"
            f"📝 الوصف: {d.get('الوصف')}\n"
            f"💰 القيمة: {d.get('قيمة_الخصم')} ({d.get('نوع_الخصم')})\n"
            f"📊 الاستخدام: {d.get('عدد_الاستخدامات')} / {d.get('الحد_الأقصى_للاستخدام')}\n"
            f"📅 الصلاحية: من <code>{d.get('تاريخ_البداية')}</code> إلى <code>{d.get('تاريخ_الانتهاء')}</code>\n" # تم استعادة التاريخ هنا
            f"🟢 الحالة: <b>{status}</b>\n"
            f"━━━━━━━━━━━━━━"
        )
        
        keyboard = [
            [InlineKeyboardButton(toggle_label, callback_data=toggle_callback)],
            [InlineKeyboardButton("🗑️ حذف نهائي", callback_data=f"confirm_del_disc_{disc_id}")],
            [InlineKeyboardButton("🔙 رجوع للقائمة", callback_data="list_all_discounts")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    else:
        await query.answer("❌ تعذر العثور على بيانات الكود.")

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# --- [ نظام إدارة الواجبات التعليمية - الإدارة ] ---

async def manage_homeworks_main_ui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الواجهة الرئيسية لإدارة الواجبات (لوحة تحكم الأدمن)"""
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("➕ إسناد واجب جديد", callback_data="hw_add_start")],
        [InlineKeyboardButton("📥 استلام وتصحيح الواجبات", callback_data="hw_view_submissions")],
        [InlineKeyboardButton("🗄 أرشيف الواجبات", callback_data="hw_archive")],
        [InlineKeyboardButton("🔙 عودة للكنترول", callback_data="manage_control")]
    ]
    
    text = (
        "📑 <b>نظام إدارة الواجبات التعليمية:</b>\n"
        "━━━━━━━━━━━━━━\n"
        "يمكنك من هنا إسناد المهام الدراسية للمجموعات، ومتابعة تسليمات الطلاب وتقييمها."
    )
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def homework_add_select_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الخطوة 1: اختيار الدورة (مع فلترة هرمية صارمة بناءً على الصلاحيات)"""
    query = update.callback_query
    user_id = query.from_user.id
    bot_token = context.bot.token
    
    # 1. نظام التحقق من الهوية (آدمن أو موظف نشط)
    config = get_bot_config(bot_token)
    bot_owner_id = str(config.get("owner_id", "0"))
    admin_list = str(config.get("admin_ids", "0")).split(",")

    # تصحيح دمج المسافات البادئة وربط الشروط
    if str(user_id) == bot_owner_id or str(user_id) in admin_list:
        auth_access = "full"
        branch_id = "001"
    else:
        # ضبط الإعدادات الافتراضية للموظف قبل الفحص
        auth_access = "restricted"
        branch_id = "001"

        # فحص الموظف في ورقة إدارة_الموظفين
        emp_sheet = ss.worksheet("إدارة_الموظفين")
        emp_records = emp_sheet.get_all_records()
        employee = next((r for r in emp_records if str(r.get("ID_المستخدم_تيليجرام")) == str(user_id) and str(r.get("حالة_الحساب")) == "نشط"), None)
        
        if not employee:
            await query.answer("⚠️ عذراً، ليس لديك صلاحية الوصول لهذا القسم.", show_alert=True)
            return
        branch_id = employee.get("معرف_الفرع", "001")

    # 2. فلترة البيانات الهرمية
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
    
    allowed_courses = []
    if auth_access == "full":
        allowed_courses = bot_courses
    else:
        # جلب الأذونات من الهيكل التنظيمي
        perms = get_employee_permissions(bot_token, user_id)
        allowed_ids = str(perms.get("الدورات_المسموحة", "")).split(",")
        allowed_ids = [i.strip() for i in allowed_ids if i.strip()]
        allowed_courses = [c for c in bot_courses if str(c.get("معرف_الدورة")) in allowed_ids]

    if not allowed_courses:
        await query.edit_message_text("⚠️ لا توجد دورات مسموحة لك حالياً لإسناد واجبات.", 
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="manage_homeworks")]]))
        return

    keyboard = [[InlineKeyboardButton(f"📘 {c['اسم_الدورة']}", callback_data=f"hw_sel_crs_{c['معرف_الدورة']}")] for c in allowed_courses]
    keyboard.append([InlineKeyboardButton("🔙 تراجع", callback_data="manage_homeworks")])
    
    context.user_data['temp_hw'] = {'branch_id': branch_id, 'auth_access': auth_access}
    await query.edit_message_text("🎯 <b>إضافة واجب:</b> اختر الدورة التعليمية المستهدفة:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


# --------------------------------------------------------------------------
async def hw_add_select_groups_ui(update, context, course_id):
    """الخطوة 2: اختيار المجموعات (مع الفلترة الهرمية ودعم التعيين المتعدد)"""
    query = update.callback_query
    user_id = query.from_user.id
    bot_token = context.bot.token
    

    all_groups = get_groups_by_course(bot_token, course_id)
    auth = context.user_data.get('temp_hw', {})
    auth['course_id'] = course_id
    
    if auth.get('auth_access') == "restricted":

        perms = get_employee_permissions(bot_token, user_id)
        allowed_grp_ids = str(perms.get("المجموعات_المسموحة", "")).split(",")
        allowed_grp_ids = [i.strip() for i in allowed_grp_ids if i.strip()]
        filtered_groups = [g for g in all_groups if str(g.get("معرف_المجموعة")) in allowed_grp_ids]
    else:
        filtered_groups = all_groups

    if 'target_groups' not in auth: auth['target_groups'] = []

    keyboard = []
    all_ids = [str(g['معرف_المجموعة']) for g in filtered_groups]
    is_all_selected = set(all_ids).issubset(set(auth['target_groups'])) if all_ids else False
    all_icon = "✅" if is_all_selected else "⬜"
    keyboard.append([InlineKeyboardButton(f"{all_icon} تحديد كافة المجموعات المتاحة", callback_data=f"hw_grp_sel_ALL_{course_id}")])
    
    for g in filtered_groups:
        g_id = str(g['معرف_المجموعة'])
        icon = "✅" if g_id in auth['target_groups'] else "⬜"
        keyboard.append([InlineKeyboardButton(f"{icon} {g['اسم_المجموعة']}", callback_data=f"hw_grp_sel_{g_id}_{course_id}")])
    
    keyboard.append([InlineKeyboardButton("➡️ الخطوة التالية (العنوان)", callback_data="hw_gen_next_title")])
    keyboard.append([InlineKeyboardButton("🔙 تراجع", callback_data="hw_add_start")])
    
    context.user_data['temp_hw'] = auth
    await query.edit_message_text(f"👥 <b>تحديد المجموعات:</b>\nاختر المجموعات المسند إليها الواجب:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def save_homework_to_db(bot_token, data):
    """حفظ الواجب (صف لكل مجموعة) مع جلب الدرجة ديناميكياً من الإعدادات"""
    try:


        sheet = ss.worksheet("الواجبات")
        now = get_system_time("full")

        
        # جلب درجة الواجب من ورقة الإعدادات بناءً على طلبك
        hw_grade = get_bot_setting(bot_token, "homework_grade", default="10")
        
        rows_to_append = []
        for group_id in data['target_groups']:
            hw_id = f"HW{str(uuid.uuid4().int)[:5]}"
            row = [
                str(bot_token),             # bot_id
                data.get('branch_id', '001'),   # معرف_الفرع
                hw_id,                      # معرف_الواجب
                data['course_id'],          # معرف_الدورة
                group_id,                   # معرف_المجموعة
                data['title'],              # عنوان_الواجب
                data['desc'],               # وصف_الواجب
                now,                        # تاريخ_الإسناد
                data['deadline'],           # تاريخ_التسليم
                "عبر البوت",                # طريقة_التسليم
                "TRUE",                      # الحالة
                str(hw_grade),              # درجة_كاملة (مجلوية من الإعدادات)
                "",                         # ملاحظات_المعلم
                "",                         # مرفقات
                now                         # آخر_تحديث
            ]
            rows_to_append.append(row)
        
        if rows_to_append:
            sheet.append_rows(rows_to_append)
            return True
        return False
    except Exception as e:
        print(f"❌ Error in Homework Persistence: {e}")
        return False
# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# --- [ نظام استلام وتصحيح الواجبات - للمدرب ] ---


async def hw_view_submissions_course_select(update, context):
    """عرض الدورات التي تحتوي على تسليمات واجبات للمدرب"""
    query = update.callback_query
    bot_token = context.bot.token
    
    
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
    
    keyboard = [[InlineKeyboardButton(f"📘 {c['اسم_الدورة']}", callback_data=f"hw_view_subs_crs_{c['معرف_الدورة']}")] for c in bot_courses]
    keyboard.append([InlineKeyboardButton("🔙 تراجع", callback_data="manage_homeworks")])
    
    await query.edit_message_text("📥 <b>تصحيح الواجبات:</b> اختر الدورة لمشاهدة تسليمات الطلاب:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
# --------------------------------------------------------------------------
async def start_add_question_flow(update, context):
    """بدء عملية إضافة سؤال جديد للبنك"""
    query = update.callback_query
    # إضافة معرف فريد للسؤال وقصره لسهولة التعامل
    import uuid
    q_id = f"Q-{str(uuid.uuid4().int)[:6]}"
    
    context.user_data['q_temp'] = {
        'q_id': q_id,
        'creator_id': update.effective_user.id
    } 
    context.user_data['action'] = 'awaiting_q_text'
    
    await query.edit_message_text(
        f"❓ <b>إضافة سؤال جديد:</b>\n"
        f"🆔 معرف السؤال: <code>{q_id}</code>\n\n"
        f"أرسل الآن <b>نص السؤال</b> الذي تريد إضافته للبنك:",
        parse_mode="HTML"
    )

async def process_q_flow(update, context):
    """معالجة خطوات إدخال السؤال (نص، خيارات، إجابة)"""
    text = update.message.text.strip()
    action = context.user_data.get('action')
    
    if action == 'awaiting_q_text':
        context.user_data['q_temp']['text'] = text
        context.user_data['action'] = 'awaiting_q_options'
        await update.message.reply_text(
            "🔢 أرسل الآن <b>الخيارات الأربعة</b> مفصولة بفاصلة أو سطر جديد\n"
            "⚠️ الترتيب سيكون (A ثم B ثم C ثم D)\n\n"
            "مثال:\nخيار أول\nخيار ثاني\nخيار ثالث\nخيار رابع",
            parse_mode="HTML"
        )
        
    elif action == 'awaiting_q_options':
        # معالجة النص لاستخراج الخيارات بدقة
        options = [opt.strip() for opt in text.replace('\n', ',').split(',') if opt.strip()]
        
        if len(options) >= 2:
            context.user_data['q_temp']['a'] = options[0]
            context.user_data['q_temp']['b'] = options[1]
            context.user_data['q_temp']['c'] = options[2] if len(options) > 2 else '-'
            context.user_data['q_temp']['d'] = options[3] if len(options) > 3 else '-'
            
            context.user_data['action'] = 'awaiting_q_correct'
            
            msg = (
                f"📝 <b>الخيارات التي تم رصدها:</b>\n"
                f"🅰️: {context.user_data['q_temp']['a']}\n"
                f"🅱️: {context.user_data['q_temp']['b']}\n"
                f"🆂: {context.user_data['q_temp']['c']}\n"
                f"🅳: {context.user_data['q_temp']['d']}\n\n"
                f"✅ ما هي <b>الإجابة الصحيحة</b>؟\n"
                f"أرسل الرمز فقط (A أو B أو C أو D)"
            )
            await update.message.reply_text(msg, parse_mode="HTML")
        else:
            await update.message.reply_text("⚠️ يرجى إرسال خيارين على الأقل ليتم اعتباره سؤالاً!")

    elif action == 'awaiting_q_correct':
        correct_answer = text.upper()
        if correct_answer in ['A', 'B', 'C', 'D']:
            context.user_data['q_temp']['correct'] = correct_answer
            
            # استدعاء الدالة الجاهزة في sheets.py
            from sheets import add_question_to_bank
            
            # تمرير القاموس بالكامل للدالة التي تملأ الـ 21 عموداً
            success = add_question_to_bank(context.bot.token, context.user_data['q_temp'])
            
            if success:
                await update.message.reply_text(
                    f"✅ تم حفظ السؤال <code>{context.user_data['q_temp']['q_id']}</code> في بنك الأسئلة بنجاح!",
                    parse_mode="HTML"
                )
            else:
                await update.message.reply_text("❌ حدث خطأ أثناء الحفظ في الشيت. تأكد من اتصال قاعدة البيانات.")
                
            # تنظيف البيانات بعد الحفظ أو الفشل النهائي
            context.user_data.pop('q_temp', None)
            context.user_data['action'] = None
        else:
            await update.message.reply_text("⚠️ خطأ! يرجى إرسال الرمز فقط (A، B، C، أو D).")

# --------------------------------------------------------------------------

# === أضف هذا الكود في نهاية الملف تماماً ===

async def prompt_add_library_file(update, context, course_id):
    """بدء عملية طلب بيانات الملف من الأدمن"""
    query = update.callback_query
    context.user_data['awaiting_lib_file'] = course_id
    await query.edit_message_text(
        "📝 **إضافة ملف جديد للمكتبة**\n\n"
        "يرجى إرسال تفاصيل الملف بالتنسيق التالي:\n"
        "`اسم الملف | الرابط | الحالة (مجاني/مدفوع)`\n\n"
        "مثال:\n"
        "`كتاب البرمجة | https://t.me/file_link | مدفوع`",
        parse_mode="Markdown"
    )

async def save_library_file_logic(update, context):
    """استقبال الرسالة النصية وحفظها في الشيت مع ضمان تصفير الحالة"""
    # نتحقق من وجود المفتاح في user_data أو في action
    course_id = context.user_data.get('awaiting_lib_file')
    
    if course_id:
        # تصفير الحالة فوراً لمنع التكرار أو التداخل مع رسائل أخرى
        context.user_data.pop('awaiting_lib_file', None)
        if context.user_data.get('action') == 'awaiting_lib_file':
            context.user_data.pop('action', None)
            
        text = update.message.text
        bot_token = context.bot.token
        
        try:
            # تقسيم النص المدخل: الاسم | الرابط | الحالة
            parts = [t.strip() for t in text.split("|")]
            if len(parts) < 3:
                raise ValueError("Format error")
                
            name, link, status = parts[0], parts[1], parts[2]
            
            # استدعاء دالة الحفظ من ملف sheets
            from sheets import add_library_item_to_sheet
            success = add_library_item_to_sheet(
                bot_token=bot_token,
                course_id=course_id,
                file_name=name,
                file_link=link,
                status=status
            )
            
            if success:
                await update.message.reply_text(f"✅ **تمت الإضافة بنجاح!**\n\n📄 الملف: {name}\n🔗 الرابط: {link}\n🔓 الحالة: {status}")
            else:
                await update.message.reply_text("❌ حدث خطأ فني أثناء الكتابة في قاعدة البيانات.")
                
        except Exception as e:
            # في حال الخطأ، نعيد للأدمن توضيح التنسيق المطلوب
            await update.message.reply_text(
                "⚠️ **خطأ في تنسيق البيانات!**\n\n"
                "يرجى المحاولة مجدداً وإرسال البيانات بهذا الشكل حصراً:\n"
                "`اسم الملف | الرابط | الحالة`"
            )

# --------------------------------------------------------------------------
async def manage_library_selector(update, context):
    query = update.callback_query
    bot_token = context.bot.token
    from sheets import courses_sheet
    
    # جلب الدورات لاختيار المكتبة المناسبة
    all_courses = courses_sheet.get_all_records()
    bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
    
    if not bot_courses:
        await query.edit_message_text("⚠️ لا توجد دورات مضافة حالياً لفتح مكتباتها.")
        return

    keyboard = [[InlineKeyboardButton(f"📚 مكتبة: {c['اسم_الدورة']}", callback_data=f"manage_library_{c['معرف_الدورة']}")] for c in bot_courses]
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="main_menu")])
    
    await query.edit_message_text("🎯 **اختر الدورة لفتح مكتبتها الشاملة:**", reply_markup=InlineKeyboardMarkup(keyboard))


# --------------------------------------------------------------------------
    # جلب الأقسام من قاعدة البيانات
async def manage_categories_main(update, context):
    query = update.callback_query
    from sheets import get_all_categories
    

    categories = get_all_categories(bot_token) # تمرير التوكن المعرف في بداية الدالة

    
    keyboard = []
    if categories:
        for cat in categories:
            # افتراض أن cat عبارة عن dict يحتوي على 'id' و 'name'
            keyboard.append([InlineKeyboardButton(f"📂 {cat['name']}", callback_data=f"view_cat_{cat['id']}")])
    
    keyboard.append([InlineKeyboardButton("➕ إضافة قسم جديد", callback_data="add_new_cat")])
    keyboard.append([InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="main_menu")])
    
    await query.edit_message_text(
        "🛠️ **إدارة الأقسام الدراسية:**\n\nيمكنك إضافة أو تعديل أو حذف الأقسام من هنا.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# --------------------------------------------------------------------------
#دالة الاختبار

async def quiz_activation_start(update, context):
    """جلب الدورات التدريبية التي لها أسئلة في البنك لعرضها للمدرب"""
    query = update.callback_query
    bot_id = str(context.bot.id)
    
    # 1. جلب بيانات الدورات من ورقة "الدورات_التدريبية"
    courses_sheet = ss.worksheet("الدورات_التدريبية")
    all_courses = courses_sheet.get_all_records()
    
    # 2. فلترة الدورات التابعة لهذا البوت فقط
    my_courses = [c for c in all_courses if str(c.get('bot_id')) == bot_id]
    
    keyboard = []
    for course in my_courses:
        course_name = course.get('اسم_الدورة', 'دورة بدون اسم')
        course_id = course.get('معرف_الدورة')
        # إنشاء زر لكل دورة
        keyboard.append([InlineKeyboardButton(f"📘 {course_name}", callback_data=f"act_q_crs_{course_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_control")])
    
    await query.edit_message_text(
        "🎯 <b>تفعيل اختبار جديد:</b>\nاختر الدورة التي تريد تفعيل الاختبار لمجموعاتها:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

async def quiz_activation_groups(update, context, course_id):
    """عرض المجموعات التابعة للدورة المختارة"""
    query = update.callback_query
    bot_id = str(context.bot.id)
    
    # 1. جلب المجموعات من ورقة "إدارة_المجموعات"
    groups_sheet = ss.worksheet("إدارة_المجموعات")
    all_groups = groups_sheet.get_all_records()
    
    # 2. فلترة المجموعات التي تتبع نفس البوت ونفس الدورة
    relevant_groups = [
        g for g in all_groups 
        if str(g.get('bot_id')) == bot_id and str(g.get('معرف_الدورة')) == str(course_id)
    ]
    
    keyboard = []
    if not relevant_groups:
        text = "⚠️ لا توجد مجموعات مفعلة لهذه الدورة حالياً."
    else:
        text = f"👥 <b>الدورة ID: {course_id}</b>\nاختر المجموعة المستهدفة لإطلاق الاختبار لها:"
        for grp in relevant_groups:
            g_name = grp.get('اسم_المجموعة', 'مجموعة غير مسمى')
            g_id = grp.get('معرف_المجموعة')
            keyboard.append([InlineKeyboardButton(f"👥 {g_name}", callback_data=f"act_q_final_{g_id}")])
            
    keyboard.append([InlineKeyboardButton("🔙 عودة للدورات", callback_data="manage_tests")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

async def employee_quiz_view(update, context):
    """عرض أرشيف الاختبارات من ورقة الاختبارات_الآلية"""
    query = update.callback_query
    bot_id = str(context.bot.id)
    
    tests_sheet = ss.worksheet("الاختبارات_الآلية")
    all_tests = tests_sheet.get_all_records()
    
    my_tests = [t for t in all_tests if str(t.get('bot_id')) == bot_id]
    
    keyboard = []
    for test in my_tests:
        t_id = test.get('معرف_الاختبار')
        t_status = "✅" if test.get('حالة_الاختبار') == "نشط" else "❌"
        keyboard.append([InlineKeyboardButton(f"{t_status} اختبار ID: {t_id}", callback_data=f"q_toggle_vis_{t_id}")])
        
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="manage_control")])
    
    await query.edit_message_text(
        "📂 <b>أرشيف الاختبارات الآلية:</b>\nيمكنك تبديل حالة الاختبار (نشط/متوقف) بالضغط عليه:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    ) 
# --------------------------------------------------------------------------
# دالة عرض الاختبار للطالب 
async def student_quizzes_list_ui(update, context):
    """عرض الاختبارات المتاحة للطالب بناءً على مجموعته ودورته من الكاش"""
    query = update.callback_query
    user_id = update.effective_user.id
    bot_token = context.bot.token
    
    from cache_manager import FACTORY_GLOBAL_CACHE
    
    # 1. جلب بيانات الطالب لمعرفة مجموعته ودورته
    all_students = FACTORY_GLOBAL_CACHE["data"].get("قاعدة_بيانات_الطلاب", [])
    student_info = next((s for s in all_students if str(s.get("ID_المستخدم_تيليجرام")) == str(user_id)), None)
    
    if not student_info:
        await query.answer("⚠️ بياناتك غير مسجلة في قاعدة الطلاب.")
        return

    s_group = str(student_info.get("معرف_المجموعة"))
    s_course = str(student_info.get("معرف_الدورة"))

    # 2. جلب الاختبارات المتاحة لهذه المجموعة والدورة وتكون حالتها TRUE
    all_quizzes = FACTORY_GLOBAL_CACHE["data"].get("الاختبارات_الآلية", [])
    available_quizzes = [
        q for q in all_quizzes 
        if str(q.get("bot_id")) == str(bot_token) and 
        str(q.get("معرف_الدورة")) == s_course and 
        (s_group in str(q.get("المجموعات_المستهدفة")) or str(q.get("المجموعات_المستهدفة")) == "ALL") and
        str(q.get("حالة_الاختبار")).upper() == "TRUE"
    ]

    if not available_quizzes:
        await query.edit_message_text("🔔 لا توجد اختبارات متاحة لك حالياً.", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]]))
        return

    keyboard = []
    for quiz in available_quizzes:
        q_title = quiz.get("معرف_الاختبار") # أو اسم الاختبار إذا أضفته
        q_id = quiz.get("معرف_الاختبار")
        keyboard.append([InlineKeyboardButton(f"✍️ ابدأ: {q_title}", callback_data=f"start_quiz_{q_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 عودة", callback_data="main_menu")])
    
    await query.edit_message_text("📝 <b>الاختبارات المتاحة لك:</b>\nاختر الاختبار للبدء:", 
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
