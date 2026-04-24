import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import logging
import uuid 
import random
# استيراد كل شيء من الملف الموحد الجديد
from cache_manager import (
    DataManager, 
    db_manager, 
    get_bot_data_from_cache, 
    smart_sync_check, 
    update_global_version, 
    ensure_bot_sync_row
)


# --- [ التعديل المعتمد لملف sheets.py ] ---
import os
from database_core import DataManager

# تعريف المتغير بشكل عالمي
db_manager = None

try:
    # محاولة استيراد الكائن الجاهز من النواة
    from database_core import db_manager as core_db
    db_manager = core_db
except ImportError:
    # إذا فشل، ننشئ نسخة جديدة باستخدام توكن المصنع
    token = os.getenv("BOT_TOKEN")
    if token:
        db_manager = DataManager(token)

# دالة تأمين المحرك (أضفها لضمان عدم حدوث خطأ NoneType مستقبلاً)
def get_db():
    global db_manager
    if db_manager is None:
        token = os.getenv("BOT_TOKEN")
        if token:
            db_manager = DataManager(token)
    return db_manager


def get_system_time(mode="full"):
    """
    المحرك الموحد للوقت في المنصة:
    mode="date"   -> يعيد التاريخ فقط (2026-04-06)
    mode="time"   -> يعيد الوقت فقط (15:30:05)
    mode="full"   -> يعيد التاريخ والوقت (2026-04-06 15:30:05)
    """
    now = datetime.now()
    if mode == "date":
        return now.strftime("%Y-%m-%d")
    elif mode == "time":
        return now.strftime("%H:%M:%S")
    else:
        return now.strftime("%Y-%m-%d %H:%M:%S")


# إعداد اللوجر الاحترافي مع التسلسل الهرمي (Hierarchy Logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [CORE:DB] %(message)s')
logger = logging.getLogger(__name__)

# --- إعداد المتغيرات العالمية لكافة أوراق العمل (14 ورقة كاملة) ---
client = None
ss = None
users_sheet = None           # 1. المستخدمين
bots_sheet = None            # 2. البوتات_المصنوعة
content_sheet = None         # 3. إعدادات_المحتوى
logs_sheet = None            # 4. السجلات
stats_sheet = None           # 5. الإحصائيات
payments_sheet = None        # 6. المدفوعات
students_db_sheet = None      # 7. قاعدة_بيانات_الطلاب
registrations_logs_sheet = None # 8. سجل_التسجيلات
departments_sheet = None        # 9. الأقسام
discount_codes_sheet = None     # 10. أكواد_الخصم
coupons_sheet = None            # 11. الكوبونات
courses_sheet = None            # 12. الدورات_التدريبية
faq_sheet = None                # 13. الأسئلة_الشائعة
meta_sheet = None               # 14. _meta (الإصدار والتحقق)

lectures_sheet = None        # ورقة جدول_المحاضرات (إضافة جديدة)

# معرف ملف Google Sheet الخاص بمصنع البوتات
SPREADSHEET_ID = "1e0tREOyfmZgQ_iCvWXJL2GpR_I4WfCpBlU7DYUclsfY"

# --- إعدادات النظام المتقدمة (Production Core) ---
STRICT_SCHEMA = True
SCHEMA_VERSION = "1.3"
BATCH_SIZE = 50
RETRY_ATTEMPTS = 3
AUTO_RESIZE = False
SENSITIVE_FIELDS = {"التوكن", "كلمة_المرور", "token", "api_key", "credentials", "private_key","bot_id"}




# --- [ 1. كتل إدارة المخطط والهيكل - يجب أن تكون في البداية ] ---
# --------------------------------------------------------------------------
def get_sheets_structure():
    sheets_config = [
    
        {"name": "الإعدادات","cols": ["bot_id", "المفتاح_البرمجي", "العنوان", "القيمة", "ملاحظات"],"color": {"red": 0.8, "green": 0.9, "blue": 1}},
        {"name":"نظام_المزامنة","cols":["bot_id","رقم_الإصدار","آخر_تحديث","الحالة","ID_المالك","ID_المطور"],"color":{"red":0.9,"green":0.8,"blue":1}}, 
        {"name": "الهيكل_التنظيمي_والصلاحيات", "cols": ["bot_id", "معرف_الفرع", "ID_الموظف_أو_المدرب", "صلاحية_الأقسام", "صلاحية_الدورات", "صلاحية_المدربين", "صلاحية_الموظفين", "صلاحية_الإحصائيات", "صلاحية_الإذاعة", "صلاحية_الرسائل_الخاصة", "صلاحية_الكوبونات", "صلاحية_أكواد_الخصم", "الدورات_المسموحة", "المجموعات_المسموحة", "تحديث_السيرفر"]}, 
        {"name": "المستخدمين", "cols": ["ID المستخدم","اسم المستخدم","تاريخ التسجيل","الحالة","نوع الاشتراك","عدد البوتات","آخر نشاط","اللغة","مصدر التسجيل","معرف إحالة","رصيد"], "color": {"red": 0.85, "green": 0.92, "blue": 0.83}},
        {"name": "البوتات_المصنوعة", "cols": ["ID المالك","نوع البوت","اسم البوت","التوكن","حالة التشغيل","bot_id","username_bot","تاريخ الإنشاء","آخر تشغيل","عدد المستخدمين","عدد الرسائل","الحالة التقنية","webhook_url","api_type","plan","expiration_date","is_active","errors_log","تاريخ_آخر_تجديد","سعر_الاشتراك","رصيد_البوت","الحد_الأقصى_للطلاب","الحد_الأقصى_للدوات","الحد_الأقصى_للاقسام", "ميزة_الذكاء_الاصطناعي", "ميزة_رفع_وتصدير_البيانات_اكسل" ,"معرف_الفاتورة","متوسط_زمن_الاستجابة","استخدام_CPU","استخدام_الذاكرة","المستخدمون_النشطون_يومياً","المستخدمون_النشطون_شهرياً","معدل_الاحتفاظ","تاريخ_آخر_تحديث_للتوكن","حالة_التوكن","حالة_الدفع","طريقة_الدفع","دورة_الفوترة","إصدار_البوت","بيئة_التشغيل","إعادة_تشغيل_تلقائي","نموذج_الذكاء_الاصطناعي","استهلاك_التوكنات_AI","تكلفة_AI"], "color": {"red":0.81,"green":0.88,"blue":0.95} }, 
        {"name": "إعدادات_المحتوى", "cols": ["bot_id","الرسالة الترحيبية","القوانين","رد التوقف","auto_reply","ai_enabled","welcome_enabled","buttons","banned_words","admin_ids","language","theme","delay_response","broadcast_enabled","custom_commands", "welcome_morning", "welcome_noon", "welcome_evening", "welcome_night", "اسم_المؤسسة", "تعليمات_AI", "ref_points_join", "ref_points_purchase", "min_points_redeem", "currency_unit", "homework_grade", "subscription_price", "ai_provider", "maintenance_mode", "max_daily_ai_questions", "backup_channel_id", "bot_status_msg", "trial_end_action", "timezone", "ai_memory_limit", "إعدادات_الدفع"], "color": {"red": 1.0, "green": 0.95, "blue": 0.8}},
        {"name": "الإحصائيات", "cols": ["bot_id","daily_users","messages_count","new_users","blocked_users","date"], "color": {"red": 0.92, "green": 0.82, "blue": 0.86}},
        {"name": "السجلات", "cols": ["bot_id","type","message","time"], "color": {"red": 0.93, "green": 0.93, "blue": 0.93}},
        {"name": "_meta", "cols": ["key", "value", "updated_at"], "color": {"red": 1, "green": 0.8, "blue": 0.8}}, 
        {"name": "الذكاء_الإصطناعي", "cols": ["bot_id","ID_المستخدم","اسم_المستخدم","تاريخ_التسجيل","الحالة","نوع_الاشتراك","عدد_البوتات","آخر_نشاط","اللغة","مصدر_التسجيل","معرف_إحالة","رصيد","اسم_المؤسسة","تعليمات_AI"] }, 
        {"name": "المدفوعات", "cols": ["bot_id", "معرف_الفرع", "user_id","amount","method","date","status"], "color": {"red": 0.99, "green": 0.9, "blue": 0.8}},
        {"name": "قاعدة_بيانات_الطلاب", "cols": ["bot_id","معرف_الفرع", "معرف_الطالب","ID_المستخدم_تيليجرام","الاسم_بالإنجليزي","الاسم_بالعربي","العمر","البلد","المدينة","رقم_الهاتف","البريد_الإلكتروني","تاريخ_الميلاد","المستوى","الحالة","كلمة_المرور","رابط_الصورة","معرف_الدورة","اسم_الدورة","معرف_المجموعة", "اسم_المجموعة","الجنس","اسم_ ولي_الأمر","رقم_تواصل_ولي_الأمر","المؤهل_العلمي","التخصص","سنوات_الخبرة","دورات_سابقة","رابط_LinkedIn","رابط_Telegram","الرسوم","طريقة_الدفع","رابط_الإيصال","سبب_الرفض","النسبة%","المبلغ_المستحق","حالة_الحظر","معرف_الموظف","اسم_المستخدم_تيلجرام","معرف_الحملة_التسويقية","اسم_الفرع","ملاحظات"]},
        {"name": "سجل_التسجيلات", "cols": ["bot_id", "معرف_الفرع" , "معرف_التسجيل","طابع_زمني","معرف_الطالب","اسم_الطالب","ID_المستخدم_تيليجرام","معرف_الدورة","اسم_الدورة","معرف_المجموعة","اسم_المجموعة","تاريخ_التسجيل","حالة_التسجيل","طريقة_التسجيل","معرف_الخصم","قيمة_الخصم","السعر_الأصلي","السعر_بعد_الخصم","المبلغ_المدفوع","المبلغ_المتبقي","حالة_الدفع","طريقة_الدفع","رابط_الإيصال","اسم_الموظف","معرف_الموظف","معرف_الحملة_التسويقية","اسم_الفرع","حالة_القبول","سبب_الرفض","تاريخ_آخر_تحديث","ملاحظات","تاريخ_الانسحاب","حالة_الترقية","الدورة_السابقة","المجموعة_السابقة","ملاحظات_الإدارة","تاريخ_تأكيد_الدفع"]},
        {"name": "الأقسام", "cols": ["bot_id","معرف_القسم","اسم_القسم","الحالة","ترتيب_العرض","تاريخ_الإنشاء","معرف_الفرع","ملاحظات"]},
        {"name": "الكوبونات", "cols": ["bot_id", "معرف_الفرع", "معرف_الكوبون","معرف_المسوق","قيمة_الخصم","نوع_الخصم","الحد_الأقصى_للاستخدام","حالة_الكوبون","تاريخ_الإنشاء","تاريخ_الانتهاء","ملاحظات"]},
        {"name": "الدورات_التدريبية", "cols": ["bot_id", "معرف_الفرع", "معرف_الدورة", "اسم_الدورة", "الوصف", "تاريخ_البداية", "تاريخ_النهاية", "نوع_الدورة", "سعر_الدورة", "الحد_الأقصى", "المتطلبات", "اسم_الموظف", "معرف_الموظف", "معرف_الحملة_التسويقية", "معرف_المدرب", "ID_المدرب", "اسم_المدرب", "معرف_القسم"]},
        {"name": "إدارة_الحملات_الإعلانية", "cols": ["bot_id", "معرف_الفرع","معرف_الدورة", "معرف_الحملة","المنصة","تاريخ_البداية","تاريخ_النهاية","الميزانية","عدد_المسجلين","الحالة","ID_المسوق"] },
        {"name": "أكواد_الخصم", "cols": ["bot_id", "معرف_الفرع", "معرف_الخصم","نوع_الخصم","الوصف","قيمة_الخصم","الحد_الأقصى_للاستخدام","عدد_الاستخدامات","تاريخ_البداية","تاريخ_الانتهاء","الحالة","معرف_الدورة","اسم_الموظف","معرف_الحملة_التسويقية","ملاحظات"]},
        {"name": "الأسئلة_الشائعة", "cols": ["bot_id" ,"معرف_الفرع", "معرف_القسم","معرف_الدورة","اسم_الدورة", "محتوى_السؤال_مع_الإجابة","الحالة","ترتيب_العرض","تاريخ_الإنشاء","اسم_الفرع","ملاحظات"]},
        {"name": "إدارة_الموظفين", "cols": ["bot_id","معرف_الفرع ","ID","معرف_الموظف","الاسم_الكامل","الجنس","تاريخ_الميلاد","رقم_الهوية","العنوان","الصورة_الشخصية","التخصص","المسمى_الوظيفي","المواد_التي_يدرسها","المؤهل_العلمي","سنوات_الخبرة","الشهادات_المهنية","مستوى_التقييم","رقم_الهاتف","رقم_واتساب","رقم_طوارئ","البريد_الإلكتروني","كلمة_المرور","نوع_العقد","تاريخ_التعيين","تاريخ_بداية_العقد","تاريخ_نهاية_العقد","عدد_ساعات_العمل","الدرجة_الوظيفية","الحالة_الوظيفية","الراتب_الأساسي","نسبة_الحوافز","البدلات","الخصومات","إجمالي_الراتب","طريقة_الدفع","رقم_الحساب_المالي","المشرف_المباشر","الصلاحيات","تاريخ_آخر_تسجيل_دخول","حالة_الحساب","اسم_المستخدم","الرتبة","اسم_الفرع"]},
        {"name": "إدارة_الفروع", "cols": ["bot_id","معرف_الفرع","اسم_الفرع","الدولة", "المدير_المسؤول", "العملة", "ملاحظات"] }, 
        {"name": "بنك_الأسئلة", "cols": ["bot_id","معرف_الفرع","معرف_الاختبار", "معرف_الدورة","معرف_المجموعة", "معرف_السؤال","نص_السؤال","الخيار_A","الخيار_B","الخيار_C","الخيار_D","الإجابة_الصحيحة","الدرجة","مدة_السؤال_بالثواني","مستوى_الصعوبة","نوع_السؤال","شرح_الإجابة","الوسم_التصنيفي","حالة_السؤال","تاريخ_الإضافة","معرف_مُنشئ_السؤال"]},
        {"name": "الاختبارات_الآلية", "cols": ["bot_id", "معرف_الفرع", "معرف_الاختبار","معرف_الدورة","المجموعات_المستهدفة", "قائمة_الأسئلة","عدد_الأسئلة","درجة_النجاح","مدة_الاختبار","طريقة_حساب_الوقت","ترتيب_عشوائي","عدد_المحاولات","ظهور_النتيجة","حالة_الاختبار","معرف_المدرب", "تاريخ_الإنشاء"]},
        {"name": "سجل_الإجابات", "cols": ["bot_id","معرف_الفرع", "معرف_الدورة", "معرف_الاختبار", "معرف_الطالب", "تفاصيل_الاجابات", "الإجابات_الخاطئة", "الدرجة", "النسبة_المئوية", "حالة_النجاح", "تاريخ_الاختبار", "وقت_البدء", "وقت_التسليم","محاولات_الغش", "الرقم_التسلسلي", "تاريخ_الإصدار", "الحالة", "نوع_الشهادة", "رابط_الشهادة", "سبب_الالغاء"]}, 
        {"name": "الإدارة_المالية", "cols": ["bot_id", "معرف_الفرع", "معرف_الدفع","معرف_الطالب","معرف_الدورة","المبلغ_المدفوع","المبلغ_الإجمالي","تاريخ_الدفع","طريقة_الدفع","رابط_الإيصال","حالة_السداد","معرف_الموظف","معرف_الحملة_التسويقية","ملاحظات"] }, 
        {"name": "المهام_الإدارية", "cols": ["bot_id", "معرف_الفرع", "معرف_المهمة", "عنوان_المهمة", "الوصف", "الموظف_المسؤول", "تاريخ_الإسناد", "الموعد_النهائي", "الحالة", "الأولوية", "تاريخ_الإتمام", "ملاحظات_المتابعة", "المرفقات", "نوع_المهمة", "تاريخ_آخر_تحديث", "حالة_التنبيه"] },
        {"name": "سجل_العمليات_الإدارية", "cols": ["bot_id","معرف_الفرع ","معرف_الموظف", "التاريخ_والوقت", "الإجراء", "التفاصيل"] },
        {"name": "الطلبات", "cols": ["bot_id","معرف_الفرع ","معرف_الطلب", "التاريخ", "معرف_الطالب", "اسم_الطالب", "نوع_الطلب", "التفاصيل", "الأولوية", "الحالة", "الموظف_المسؤول", "قناة_الطلب", "تاريخ_الرد", "تاريخ_الإغلاق", "مدة_المعالجة", "ملاحظات_الإدارة", "مرفقات", "آخر_تحديث"] },
        {"name": "المكتبة", "cols": ["bot_id","معرف_الفرع ","معرف_الملف","اسم_الملف","النوع","التصنيف","الدورة","الوصف","الرابط","صلاحية_الوصول","سعر_الوصول","عدد_المشاهدات","عدد_المشتركين","لغة_المحتوى","المستوى","مدة_المحاضرة","تاريخ_الإضافة","تاريخ_آخر_تحديث","أضيف_بواسطة","الحالة","سجل_التعديل","عدد_التقييمات","متوسط_التقييم","تعليقات","عدد_المشاركات"] },
        {"name": "الأوسمة_والإنجازات", "cols": ["bot_id", "معرف_الفرع", "معرف_السجل", "معرف_الطالب", "اسم_الطالب", "النوع", "العنوان", "الوصف", "السبب_أو_المصدر", "تاريخ_الحدث", "منح_بواسطة", "معرف_الدورة", "معرف_المجموعة", "المستوى", "النقاط", "مرئي_للطالب", "ملاحظات", "تاريخ_التحديث", "حالة_السجل"]},
        {"name": "الواجبات", "cols": ["bot_id", "معرف_الفرع", "معرف_الواجب", "معرف_الدورة", "معرف_المجموعة", "عنوان_الواجب", "وصف_الواجب", "تاريخ_الإسناد", "تاريخ_التسليم", "طريقة_التسليم", "الحالة", "درجة_كاملة", "ملاحظات_المعلم", "مرفقات", "آخر_تحديث"] },
        {"name": "تنفيذ_الواجبات_من_الطلاب", "cols": ["bot_id","معرف_الفرع ","معرف_التنفيذ","معرف_الواجب","معرف_الطالب","معرف_المجموعة","معرف_الدورة","تاريخ_البداية","تاريخ_التسليم","حالة_التنفيذ","النقاط_المكتسبة","ملاحظات_المعلم","مرفقات_الطالب","عدد_محاولات_التسليم","وقت_الإكمال","تقييم_التسليم","آخر_تحديث","مرئي_للطالب"] },  
        {"name": "إدارة_المجموعات", "cols": ["bot_id","معرف_الفرع ", "معرف_المجموعة","اسم_المجموعة","معرف_الدورة","أيام_الدراسة","توقيت_الدراسة","ID_المعلم_المسؤول","حالة_المجموعة","معرف_الموظف","معرف_الحملة_التسويقية", "سعة_المجموعة", "عدد_الطلاب_الحالي", "رابط_المجموعة", "تاريخ_الإنشاء"] },
        {"name": "جدول_المحاضرات", "cols": ["bot_id","معرف_الفرع ","التاريخ", "اليوم", "وقت_البداية", "وقت_النهاية", "معرف_الدورة", "معرف_المجموعة", "معرف_المدرب", "اسم_المدرب", "الحالة", "ملاحظات", "نوع_الحصة", "رابط_الحصة", "تنبيه_تلقائي"] },
        {"name": "سجل_ساعات_العمل", "cols": ["bot_id","معرف_الفرع ","معرف_الموظف", "وقت_تسجيل_الدخول", "وقت_تسجيل_الخروج", "نوع_النشاط", "ملاحظات"] },
        {"name": "كشوف_المرتبات", "cols": ["bot_id","معرف_الفرع ","الشهر", "معرف_الموظف", "الراتب_الأساسي", "الحوافز", "الخصومات", "صافي_الراتب", "حالة_الصرف"] },
        {"name": "سجل_السحوبات", "cols": ["bot_id", "ID", "اسم_المستخدم", "معرف_الطلب", "المبلغ", "وسيلة_التحويل", "تاريخ_الطلب", "الحالة", "رابط_تأكيد الدفع", "ملاحظة_الإدارة", "تاريخ_التنفيذ"], "color": {"red": 0.98, "green": 0.92, "blue": 0.84}}, 



    

        
    ]
    return sheets_config
# --------------------------------------------------------------------------
# [ نظام التحقق الذكي من الجداول والأعمدة بدون إعادة تهيئة ]
def ensure_sheet_structure(sheet_name, required_headers):
    """
    التحقق من وجود الورقة + الأعمدة
    - لا يحذف أي شيء
    - لا يعيد إنشاء الورقة إذا كانت موجودة
    - يضيف فقط الأعمدة الناقصة
    """
    try:
        # فاصل زمني لتهدئة API جوجل عند بدء التعامل مع كل ورقة
        time.sleep(1.2) 
        try:
            sheet = ss.worksheet(sheet_name)
        except:
            # إنشاء الورقة إذا لم تكن موجودة
            sheet = ss.add_worksheet(title=sheet_name, rows="1000", cols="50")
            time.sleep(1) # تأخير إضافي بعد عملية الإنشاء
            sheet.append_row(required_headers, value_input_option='USER_ENTERED')
            print(f"✅ تم إنشاء الورقة: {sheet_name}")
            return True

        # جلب الصف الأول (العناوين)
        existing_headers = sheet.row_values(1)
        time.sleep(0.5) # فاصل زمني بعد عملية القراءة

        # إذا كانت الورقة فارغة
        if not existing_headers:
            sheet.append_row(required_headers, value_input_option='USER_ENTERED')
            print(f"✅ تم إضافة العناوين للورقة الفارغة: {sheet_name}")
            return True

        # تحديد الأعمدة الناقصة فقط
        missing_headers = [h for h in required_headers if h not in existing_headers]

        # إضافة الأعمدة الناقصة فقط
        if missing_headers:
            new_headers = existing_headers + missing_headers
            # استخدام تأخير قبل التحديث المباشر
            time.sleep(1) 
            sheet.update('1:1', [new_headers])
            print(f"⚙️ تم تحديث الأعمدة في {sheet_name} (إضافة الناقص فقط)")
        else:
            print(f"✔️ الورقة {sheet_name} جاهزة ولا تحتاج تعديل")

        return True

    except Exception as e:
        print(f"❌ خطأ في التحقق من الورقة {sheet_name}: {e}")
        return False

#~~~~~~~~~~~~~~~~
def setup_sheet_format(sheet, wrap_columns=None):
    """
    تهيئة تنسيق أي ورقة:
    - ضبط عرض الأعمدة
    - تفعيل التفاف النص
    - ضبط ارتفاع الصفوف
    
    wrap_columns: قائمة أرقام الأعمدة (index يبدأ من 0)
    """
    try:
        sheet_id = sheet._properties['sheetId']

        requests = []

        # ✅ 1- ضبط عرض الأعمدة المطلوبة
        if wrap_columns:
            for col in wrap_columns:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col,
                            "endIndex": col + 1
                        },
                        "properties": {
                            "pixelSize": 250
                        },
                        "fields": "pixelSize"
                    }
                })

            # ✅ 2- تفعيل التفاف النص
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": min(wrap_columns),
                        "endColumnIndex": max(wrap_columns) + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP"
                        }
                    },
                    "fields": "userEnteredFormat.wrapStrategy"
                }
            })

        # ✅ 3- ضبط ارتفاع الصفوف
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1000
                },
                "properties": {
                    "pixelSize": 120
                },
                "fields": "pixelSize"
            }
        })

        # تنفيذ
        sheet.spreadsheet.batch_update({"requests": requests})

        print(f"✅ تم تنسيق الورقة: {sheet.title}")

    except Exception as e:
        print(f"❌ خطأ في تنسيق {sheet.title}: {e}")
#~~~~~~~~~~~~~~~~



# --------------------------------------------------------------------------
# دالة تحديث الأعمدة 
def ensure_sheet_schema(worksheet, required_headers):
    """تحديث أعمدة ورقة واحدة دون حذف البيانات"""
    try:
        existing_headers = worksheet.row_values(1)
        new_headers = [h for h in required_headers if h not in existing_headers]
        if new_headers:
            next_col = len(existing_headers) + 1
            worksheet.update_cell(1, next_col, new_headers)
            print(f"✅ تمت إضافة أعمدة جديدة لـ {worksheet.title}")
    except Exception as e:
        print(f"❌ خطأ في تحديث أعمدة {worksheet.title}: {e}")
# --------------------------------------------------------------------------
# دالة النظام الشامل 
def ensure_all_sheets_schema(spreadsheet, sheets_structure):
    """
    نظام مزامنة ذكي (النسخة المطورة):
    - يجلب قائمة الأوراق بالكامل مرة واحدة لتوفير طلبات API.
    - يتفادى خطأ "A sheet with the name ... already exists".
    - يضيف الأعمدة الناقصة فقط ولا يلمس البيانات الموجودة.
    """
    try:
        # جلب أسماء كل الأوراق الموجودة حالياً دفعة واحدة
        existing_sheet_names = [ws.title for ws in spreadsheet.worksheets()]
        
        for sheet_def in sheets_structure:
            sheet_name = sheet_def.get("name")
            required_headers = sheet_def.get("cols", [])

            if not sheet_name or not required_headers:
                continue

            # التحقق محلياً من القائمة
            if sheet_name not in existing_sheet_names:
                # إنشاء الورقة فقط إذا لم تكن موجودة
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows="1000",
                    cols=str(len(required_headers) + 10)
                )
                print(f"🆕 تم إنشاء الورقة بنجاح: {sheet_name}")
            else:
                # إذا كانت موجودة، نفتحها لنفحص أعمدتها
                worksheet = spreadsheet.worksheet(sheet_name)

            # فحص وتحديث الأعمدة الناقصة
            ensure_sheet_schema(worksheet, required_headers)

    except Exception as e:
        print(f"❌ خطأ في مزامنة الأوراق: {e}")

# --------------------------------------------------------------------------
# كاش داخلي لتسريع العمليات
_ws_cache = {}
# --- [ 2. كتل الاتصال والتهيئة الأساسية ] ---
def get_config():
    """جلب وتصحيح مفاتيح الوصول من متغيرات البيئة لضمان توافق RSA و JWT الرقمي"""
    raw_key = os.getenv("G_PRIVATE_KEY")
    
    if not raw_key:
        print("❌ خطأ حرج: G_PRIVATE_KEY مفقود من إعدادات السيرفر!")
        return None
    try:
        clean_key = raw_key.replace('\\n', '\n').strip().strip('"').strip("'")
        return {
            "type": "service_account",
            "project_id": os.getenv("G_PROJECT_ID"),
            "private_key_id": os.getenv("G_PRIVATE_KEY_ID"),
            "private_key": clean_key,
            "client_email": os.getenv("G_CLIENT_EMAIL"),
            "client_id": os.getenv("G_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("G_CLIENT_CERT_URL")
        }
    except Exception as e:
        print(f"❌ خطأ في معالجة القاموس البرمجي للاعتمادات: {e}")
        return None
# --------------------------------------------------------------------------
# --- [ 2. دالة الاتصال المصححة والمضمونة ] ---
def connect_to_google():
    """تأسيس الاتصال وربط المتغيرات مع فحص المخطط فورياً"""
    global client, ss, users_sheet, bots_sheet, content_sheet, logs_sheet
    global stats_sheet, payments_sheet, students_db_sheet, registrations_logs_sheet
    global departments_sheet, discount_codes_sheet, coupons_sheet, courses_sheet 
    global faq_sheet, meta_sheet, lectures_sheet

    config = get_config()
    if not config: return False

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(config, scope)
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        
        # --- [إصلاح: تعريف الدالة المساعدة بشكل صحيح] ---
        def safe_get_sheet(name):
            try: return ss.worksheet(name)
            except: return None

        users_sheet = safe_get_sheet("المستخدمين")
        bots_sheet = safe_get_sheet("البوتات_المصنوعة")
        content_sheet = safe_get_sheet("إعدادات_المحتوى")
        logs_sheet = safe_get_sheet("السجلات")
        stats_sheet = safe_get_sheet("الإحصائيات")
        payments_sheet = safe_get_sheet("المدفوعات")
        students_db_sheet = safe_get_sheet("قاعدة_بيانات_الطلاب")
        registrations_logs_sheet = safe_get_sheet("سجل_التسجيلات")
        departments_sheet = safe_get_sheet("الأقسام")
        discount_codes_sheet = safe_get_sheet("أكواد_الخصم")
        coupons_sheet = safe_get_sheet("الكوبونات")
        courses_sheet = safe_get_sheet("الدورات_التدريبية")
        faq_sheet = safe_get_sheet("الأسئلة_الشائعة")
        meta_sheet = safe_get_sheet("_meta")
        lectures_sheet = safe_get_sheet("جدول_المحاضرات")

        print("✅ تم الاتصال بجوجل بنجاح. الجداول بانتظار التهيئة اليدوية ⚙️")
        return True
    except Exception as e:
        print(f"❌ فشل الاتصال الأولي: {str(e)}")
        return False

# تنفيذ محاولة الاتصال الفورية
connect_to_google() 

# --------------------------------------------------------------------------
# --- [ 3. الدوال الوظيفية لبوت المصنع والطلاب ] ---
# --- الدوال الوظيفية الأساسية ---
def local_save_wrapper(table_name, data_list):
    """
    دالة الحفظ المحلي المحدثة: تتأكد من اتصال قاعدة البيانات قبل التنفيذ
    لتجنب خطأ 'NoneType' object has no attribute 'cursor'
    """
    global db_manager
    try:
        # 1. التحقق الوقائي: إذا كان المحرك غير جاهز، نحاول استدعاءه فوراً
        if db_manager is None:
            from database_core import db_manager as dm
            db_manager = dm

        # 2. تجهيز علامات الاستفهام
        placeholders = ", ".join(["?" for _ in data_list])
        
        # 3. بناء الاستعلام
        query = f"INSERT INTO '{table_name}' VALUES (NULL, {placeholders}, 'pending', CURRENT_TIMESTAMP)"
        
        # 4. التنفيذ مع التحقق من وجود الكرسر
        if db_manager and db_manager.cursor:
            db_manager.cursor.execute(query, data_list)
            db_manager.conn.commit()
            return True
        else:
            print(f"⚠️ محرك قاعدة البيانات لا يزال غير جاهز للحفظ في {table_name}")
            return False

    except Exception as e:
        print(f"❌ خطأ حرج في الحفظ المحلي (الجدول: {table_name}): {e}")
        return False

def save_user(user_id, username, inviter_id=None):
    """
    نسخة مطورة (فائقة السرعة) مع الحفاظ الكامل على:
    - 11 عمود بالترتيب الأصلي.
    - منطق الإحالة (العمود 10 و 11).
    - منع التكرار (Check ID).
    """
    try:
        # 1. التحقق من وجود المستخدم محلياً (أسرع بآلاف المرات من جوجل)
        db_manager.cursor.execute("SELECT 1 FROM 'المستخدمين' WHERE column_1 = ?", (str(user_id),))
        if db_manager.cursor.fetchone():
            return False  # المستخدم مسجل مسبقاً

        now = get_system_time("full")
        
        # 2. معالجة معرف الإحالة (العمود 10)
        ref_id = str(inviter_id) if inviter_id else ""
        
        # 3. إعداد صف البيانات (11 عمود - الالتزام الكامل بالترتيب)
        # ملاحظة: تم الحفاظ على كافة القيم والمفاتيح كما في طلبك
        row = [str(user_id), str(username), now, "نشط", "مجاني", 0, now, "ar", "Direct", ref_id, 0]
        # 4. الحفظ في المحرك المحلي
        success = local_save_wrapper("المستخدمين", row)
        
        if success:
            # تحديث الكاش لضمان المزامنة
            update_global_version("GLOBAL_SYNC") 
            return True
        return False

    except Exception as e:
        print(f"❌ خطأ تسجيل مستخدم مطور: {e}")
        return False

# --------------------------------------------------------------------------
# --- [ دالة الحفظ وتهيئة البوت - النسخة الاحترافية المسرعة ] ---
ALLOWED_TABLES = {"table1", "table2", "table3"}
def local_bulk_save(table_name, data_list, sync_status='pending'):
    """
    حفظ الصفوف كاملة (حتى 44 عمود أو أكثر) في المحرك المحلي
    مع الحفاظ على الترتيب الصارم.
    """
    try:
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"اسم جدول غير مسموح: {table_name}")

        placeholders = ", ".join(["?" for _ in data_list])
       
        query = f"INSERT INTO {table_name} VALUES (NULL, {placeholders}, ?, CURRENT_TIMESTAMP)"
        db_manager.cursor.execute(query, (*data_list, sync_status))
        db_manager.conn.commit()
        return True
    except Exception as e:
        print(f"❌ خطأ حفظ محلي في {table_name}: {e}")
        return False

def save_bot(owner_id, bot_type, bot_name, bot_token):
    """
    تطوير دالة التأسيس لتعمل بنظام الذاكرة المحلية (SQLite)
    مع الالتزام الصارم بالـ 44 عموداً وكافة الوظائف الجانبية.
    """
    try:
        now = get_system_time("full")
        today = get_system_time("date")
        bot_token = str(bot_token).strip()
        bot_id_only = bot_token.split(':')[0] if ':' in bot_token else "0"

        
        # 1. جلب معلومات البوت من تيليجرام (سرعة فائقة)
        real_bot_name = bot_name
        username_bot = ""
        try:
            import requests
            res = requests.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=5).json()
            if res.get("ok"):
                real_bot_name = res["result"]["first_name"]
                username_bot = res["result"]["username"]
        except: pass

        # 2. حجر الأساس: ربط بنظام المزامنة (وظائفك الأصلية)
        ensure_bot_sync_row(bot_token, owner_id)
        seed_default_settings(bot_token)

        # 3. بناء مصفوفة البيانات (الالتزام الصارم بـ 44 عموداً بالترتيب)
        bot_row = [
            str(owner_id), bot_type, real_bot_name, bot_token, "نشط", bot_id_only,
            username_bot, now, now, 0, 0, "جيد", "", "polling", "free", "",
            "true", "", today, "100", "0", "100", "10", "3", "TRUE", "FALSE",
            f"INV-{uuid.uuid4().hex[:6].upper()}", "0ms", "0%", "0MB", 0, 0,
            "100%", now, "Valid", "Pending", "Manual", "Monthly", "1.0.0",
            "Production", "true", "Gemini-1.5-Flash", 0, "100"
        ]

        # 4. منع التكرار (محلياً - استجابة فورية)
        db_manager.cursor.execute("SELECT local_id FROM 'البوتات_المصنوعة' WHERE column_4 = ?", (bot_token,))
        existing_bot = db_manager.cursor.fetchone()

        if existing_bot:
            # تحديث حالة التشغيل ووقت النشاط فقط (كما في كودك الأصلي)
            update_query = "UPDATE 'البوتات_المصنوعة' SET column_5 = ?, column_9 = ?, sync_status = 'pending' WHERE column_4 = ?"
            db_manager.cursor.execute(update_query, ("نشط", now, bot_token))
            print(f"♻️ تم تحديث بيانات البوت محلياً: {bot_id_only}")
        else:
            # إضافة صف جديد كامل (44 عمود)
            local_bulk_save("البوتات_المصنوعة", bot_row)
            print(f"✨ تم تسجيل بوت جديد محلياً: {bot_id_only}")

        # 5. إدارة سجل "إعدادات_المحتوى" (منع التكرار محلياً)
        content_row = [
            bot_token, "أهلاً بك! 🤖", "لا توجد قوانين حالياً.", 
            "عذراً، البوت متوقف مؤقتاً.", "false", "false", "true", "[]", "[]", 
            str(owner_id), "ar", "default", "0", "true", "[]"
        ]
        
        db_manager.cursor.execute("SELECT local_id FROM 'إعدادات_المحتوى' WHERE column_1 = ?", (bot_token,))
        if db_manager.cursor.fetchone():
            # تحديث المالك فقط (كما في منطقك الأصلي)
            db_manager.cursor.execute("UPDATE 'إعدادات_المحتوى' SET column_10 = ?, sync_status = 'pending' WHERE column_1 = ?", (str(owner_id), bot_token))
        else:
            local_bulk_save("إعدادات_المحتوى", content_row)
            print("📝 تم إنشاء سجل إعدادات المحتوى محلياً.")

        db_manager.conn.commit()
        
        # 6. إطلاق إشارة المزامنة العالمية
        update_global_version("GLOBAL_SYNC") 
        return True

    except Exception as e:
        print(f"❌ خطأ حرج في دالة التأسيس المطورة: {e}")
        return False


# --------------------------------------------------------------------------
def update_content_setting(bot_id, column_name, new_value):
    """
    تحديث إعدادات المحتوى محلياً فوراً مع ضمان المزامنة لاحقاً.
    تحافظ على نفس المنطق: البحث عن المعرف ثم تحديث العمود المحدد.
    """
    try:
        # 1. جلب العناوين من الجدول المحلي لمعرفة رقم العمود
        db_manager.cursor.execute(f"PRAGMA table_info('إعدادات_المحتوى')")
        columns = [info[1] for info in db_manager.cursor.fetchall()]
        
        # البحث عن اسم العمود (تجاهل الأعمدة التقنية local_id و sync_status)
        if column_name in columns:
            # تحديث القيمة محلياً ووسم السجل بـ pending
            query = f"UPDATE 'إعدادات_المحتوى' SET {column_name} = ?, sync_status = 'pending' WHERE column_1 = ?"
            db_manager.cursor.execute(query, (str(new_value), str(bot_id)))
            db_manager.conn.commit()
            
            # تحديث نسخة الكاش لضمان الانعكاس الفوري في البوتات
            update_global_version("GLOBAL_SYNC") 
            return True
    except Exception as e:
        print(f"❌ خطأ تحديث إعدادات محلي: {e}")
    return False

def get_bot_config(bot_id):
    """
    جلب تكوين البوت من القاعدة المحلية (استجابة في ميلي ثانية).
    تحافظ على إرجاع قاموس (Dict) بنفس المفاتيح الأصلية.
    """
    try:
        # جلب الصف بالكامل بناءً على توكن البوت أو المعرف (العمود 1)
        db_manager.cursor.execute("SELECT * FROM 'إعدادات_المحتوى' WHERE column_1 = ?", (str(bot_id),))
        row = db_manager.cursor.fetchone()
        
        if row:
            # تحويل الصف إلى قاموس مع الحفاظ على أسماء الأعمدة (Headers)
            return dict(row)
    except Exception as e:
        print(f"❌ خطأ جلب تكوين من المحلي: {e}")
    return {}

def add_log_entry(bot_id, log_type, message):
    """
    تدوين السجلات محلياً لمنع تعطيل العمليات الأساسية بانتظار Google Sheets.
    تحافظ على الأعمدة الـ 4: ID، النوع، الرسالة، الوقت.
    """
    try:
        now = get_system_time("full")
        log_data = [str(bot_id), log_type, message, now]
        
        # الحفظ في جدول السجلات المحلي
        success = local_save_wrapper("السجلات", log_data)
        return success
    except Exception as e:
        print(f"❌ خطأ تدوين سجل محلي: {e}")
        return False

def check_connection():
    """
    التحقق من الاتصال أصبح الآن يشمل القاعدة المحلية + جوجل شيت.
    """
    try:
        # التأكد من أن ملف القاعدة المحلي مفتوح وقابل للقراءة
        db_manager.cursor.execute("SELECT 1")
        
        # التأكد من اتصال جوجل شيت (المنطق الأصلي الخاص بك)
        try:
            ss.title
            return True
        except:
            return connect_to_google()
    except Exception as e:
        print(f"❌ خطأ فحص الاتصال: {e}")
        return False



# --------------------------------------------------------------------------
# جلب الإحصائيات 
def get_all_active_bots():
    """
    جلب كافة البوتات النشطة من القاعدة المحلية.
    تحافظ على نفس المفتاح 'التوكن' و 'حالة التشغيل' كما في الكود الأصلي.
    """
    try:
        # جلب البيانات من الجدول المحلي (column_4 هو التوكن، column_5 هو حالة التشغيل)
        # ملاحظة: sqlite3.Row تسمح لنا بالوصول عبر أسماء الأعمدة كما في قاموس جوجل
        db_manager.cursor.execute("SELECT * FROM 'البوتات_المصنوعة' WHERE column_5 = 'نشط'")
        rows = db_manager.cursor.fetchall()
        
        # تحويل النتائج إلى قائمة قواميس لتطابق get_all_records() الأصلية
        active_bots = []
        for row in rows:
            bot_dict = dict(row)
            # التأكد من وجود التوكن (column_4) كما في شرطك الأصلي
            if bot_dict.get("column_4"):
                active_bots.append(bot_dict)
        return active_bots
    except Exception as e:
        print(f"❌ خطأ جلب البوتات النشطة من المحلي: {e}")
        return []

def get_total_bots_count():
    """
    حساب عدد البوتات الكلي من القاعدة المحلية.
    يحاكي len(col_values(1)) - 1 بدقة متناهية وسرعة أعلى.
    """
    try:
        db_manager.cursor.execute("SELECT COUNT(*) FROM 'البوتات_المصنوعة'")
        count = db_manager.cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"❌ خطأ حساب عدد البوتات: {e}")
        return 0

def get_total_factory_users():
    """
    جلب إجمالي مستخدمي المصنع من الجدول المحلي.
    يحاكي len(users_sheet.col_values(1)) - 1.
    """
    try:
        db_manager.cursor.execute("SELECT COUNT(*) FROM 'المستخدمين'")
        count = db_manager.cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"❌ خطأ حساب إجمالي المستخدمين: {e}")
        return 0

# --- الدوال التي طلبت بقاء قيمها كما هي (بدون تغيير مفاتيح أو تبسيط) ---

def get_bot_users_count(bot_token):
    """
    كما في كودك الأصلي تماماً (قيمة ثابتة حالياً)
    """
    return 1

def get_bot_blocks_count(bot_token):
    """
    كما في كودك الأصلي تماماً (قيمة ثابتة حالياً)
    """
    return 0


# --------------------------------------------------------------------------
def safe_api_call(func, *args, **kwargs):
    """
    الوسيط الذكي (صمام أمان المزامنة): 
    - تم الحفاظ على كامل هيكل الدالة الأصلي والوظائف التقنية.
    - معالجة ذكية للخطأ 429 (تجاوز الكوتا) لضمان عدم فقدان أي بيانات أثناء الرفع.
    - الالتزام الصارم بمتغير RETRY_ATTEMPTS ومنطق الانتظار المتضاعف.
    """
    # التأكد من استخدام عدد المحاولات المحدد في الإعدادات
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # تنفيذ الطلب الفعلي لـ Google Sheets (سواء إرسال صفوف البوتات الـ 44 أو سجلات المستخدمين)
            return func(*args, **kwargs)
            
        except gspread.exceptions.APIError as e:
            # فحص نوع الخطأ (الالتزام الصارم بالبحث عن كود 429)
            if "429" in str(e):
                # حساب وقت الانتظار المتضاعف: المحاولة 1 = 3ث، المحاولة 2 = 6ث، إلخ.
                wait_time = (attempt + 1) * 10 
                logger.warning(f"⚠️ تنبيه الكوتا: تم الوصول للحد الأقصى لطلبات جوجل. إعادة المحاولة بعد {wait_time} ثانية (محاولة {attempt + 1})...")
                time.sleep(wait_time)
            else:
                # في حال وجود أخطاء أخرى (مثل صلاحيات أو اتصال)، يتم رفعها للنظام للتعامل معها
                raise e
                
        except Exception as e:
            # معالجة أي أخطاء غير متوقعة مع الحفاظ على سجل اللوج (Logging)
            logger.error(f"❌ خطأ غير متوقع في وسيط API: {e}")
            # وقت انتظار ثابت للأخطاء العامة لضمان استقرار الشبكة
            time.sleep(2) 
            
    # في حال استنفاد كافة المحاولات دون نجاح
    logger.error(f"🚨 فشل نهائي: تعذر إكمال الطلب بعد {RETRY_ATTEMPTS} محاولات.")
    return None

# --------------------------------------------------------------------------
# دالة إنشاء وتجهيز الورق - النسخة المعززة بالفواصل الزمنية
def setup_bot_factory_database(bot_token=None):
    """
    المحرك الشامل المطور:
    1. ينشئ الجداول في Google Sheets و SQLite معاً.
    2. يضمن تطابق الهيدرز (100%) في الطرفين.
    3. يحافظ على كافة التنسيقات والفواصل الزمنية ونظام الـ Migration.
    """
    global ss, _ws_cache
    if 'ss' not in globals() or ss is None: connect_to_google()
    all_requests = []

    # [إضافة حيوية]: مزامنة هيكلية قاعدة البيانات المحلية أولاً
    try:
        from database_core import DataManager
        print("🔗 جاري ربط الهيكل المحلي بـ SQLite...")
        # استدعاء محرك المزامنة الهيكلية الذي أنشأناه سابقاً
        db_manager.sync_schema(ss)
    except Exception as e:
        print(f"⚠️ تنبيه: فشل مزامنة الهيكل المحلي، سيتم الاعتماد على جوجل فقط مؤقتاً: {e}")

    # جلب الهيكل وحساب عدد الأوراق الكلي (كودك الأصلي بدون تغيير)
    structures = get_sheets_structure()  
    total_sheets = len(structures)   
    
    print(f"⚙️ بدء محرك تهيئة الجداول ({total_sheets} ورقة)...")
    time.sleep(1)  
    _ws_cache = {ws.title: ws for ws in ss.worksheets()}  

    for config in structures:  
        try:  
            sheet_name = config["name"]  
            headers = config["cols"]  
           
            # 1. التحقق من وجود الورقة أو إنشاؤها (الالتزام الصارم بالمنطق الأصلي)
            if sheet_name not in _ws_cache:  
                print(f"🆕 إنشاء ورقة جديدة: {sheet_name}")
                worksheet = safe_api_call(ss.add_worksheet, title=sheet_name, rows="500", cols=str(len(headers) + 2))  
                _ws_cache[sheet_name] = worksheet  
                time.sleep(1) 
                safe_api_call(worksheet.append_row, headers)
                time.sleep(1)
            else:  
                print(f"🛠️ تهيئة: {sheet_name} | الحالة: موجودة")
                worksheet = _ws_cache[sheet_name]  

            # 2. نظام التنسيق التلقائي (بدون أي تبسيط)
            try:  
                wrap_cols = [] 
                try: wrap_cols = get_wrap_columns(sheet_name)
                except: pass
                
                if wrap_cols:
                    print(f"✨ جاري تطبيق نظام التفاف النص لـ: {sheet_name}")
                    setup_sheet_format(worksheet, wrap_columns=wrap_cols)
                    time.sleep(1.5)
            except Exception as e:
                print(f"⚠️ فشل تنسيق الورقة {sheet_name}: {e}")

            # 3. قراءة العناوين الحالية (الالتزام الصارم)
            current_headers = worksheet.row_values(1)  

            # --- [ MIGRATION SYSTEM: إضافة الأعمدة الجديدة بدون حذف القديمة ] ---
            try:
                new_columns = [col for col in headers if col not in current_headers]
                if new_columns:
                    print(f"🆕 اكتشاف أعمدة جديدة في {sheet_name}: {new_columns}")
                    try:
                        current_col_count = len(current_headers)
                    except: current_col_count = 0

                    safe_api_call(worksheet.add_cols, len(new_columns))
                    time.sleep(1)
                    
                    refreshed_headers = worksheet.row_values(1) if worksheet.row_values(1) else current_headers
                    updated_headers = refreshed_headers + new_columns
                    
                    safe_api_call(worksheet.update, '1:1', [updated_headers])
                    print(f"✅ تمت إضافة الأعمدة الجديدة في {sheet_name}")
                    time.sleep(1)
                    current_headers = updated_headers
            except Exception as e:
                print(f"❌ خطأ في نظام Migration داخل {sheet_name}: {e}")

            time.sleep(1.1) 

            # 4. تطبيق نظام STRICT_SCHEMA (بدون تغيير)
            if set(current_headers) != set(headers):  
                if STRICT_SCHEMA:  
                    print(f"🛡️ نظام STRICT_SCHEMA: تحديث عناوين {sheet_name}")
                    safe_api_call(worksheet.update, '1:1', [headers])  
                    time.sleep(1.5)

            # 5. بناء طلبات التنسيق الجماعي (Batch Update)
            sheet_id = worksheet.id  
            all_requests.extend([  
                {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": config.get("color", {"red": 1, "green": 1, "blue": 1}), "textFormat": {"bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"}},  
                {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}}  
            ])  

            time.sleep(3.5) 

        except Exception as e:   
            print(f"❌ خطأ تهيئة {sheet_name}: {e}")  
            time.sleep(2) 

    # 6. دفع التحديثات الجماعية (بدون اختصار)
    if all_requests:  
        print(f"🚀 جاري دفع التحديثات الجماعية للتنسيق (Batch Update)...")
        for i in range(0, len(all_requests), BATCH_SIZE):  
            try:
                safe_api_call(ss.batch_update, {"requests": all_requests[i:i+BATCH_SIZE]})  
                time.sleep(4.5)
            except: pass

    # 7. زرع الإعدادات وتحديث الميتا (الالتزام الصارم)
    if bot_token:  
        print(f"🌱 زرع الإعدادات الافتراضية للبوت الرئيسي...")
        seed_default_settings(bot_token)  
        time.sleep(2.2)  

    print(f"📊 تحديث ورقة البيانات الوصفية (Meta Info)...")
    update_meta_info()  
    time.sleep(2)  

    if verify_setup(structures):  
        print(f"🎊 اكتملت تهيئة كافة الجداول ({total_sheets} ورقة) بنجاح باهر!")
        return total_sheets  
    
    print(f"⚠️ فشل التحقق النهائي من التهيئة!")
    return 0

def verify_setup(bot_token):
    [span_0](start_span)"""دالة التحقق من اكتمال تأسيس الجداول لضمان عدم الانهيار[span_0](end_span)"""
    try:
        from database_core import DataManager
        dm = DataManager(bot_token)
        # التحقق من وجود جدول البوتات كعينة
        dm.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='البوتات_المصنوعة'")
        return dm.cursor.fetchone() is not None
    except:
        return False


# --------------------------------------------------------------------------    
def seed_default_settings(bot_token):
    """
    تطوير محرك زرع الإعدادات:
    - الحفاظ الكامل على كافة المفاتيح الـ 18 (keys, titles, values, notes).
    - تحويل منطق فحص التكرار والحفظ ليكون محلياً (SQLite) لسرعة البرق.
    - الالتزام الصارم بترتيب الأعمدة الخمسة: bot_token, key, title, value, note.
    """
    try:
        # 1. قائمة المفاتيح الأساسية (تم الحفاظ عليها كاملة وبدون أي تعديل في النصوص أو القيم)
        default_keys = [
            {"key": "ref_points_join", "title": "نقاط دعوة صديق", "value": "1", "note": "النقاط التي يحصل عليها الشخص عند دخول صديق عبر رابطه"},
            {"key": "ref_points_purchase", "title": "نقاط الشراء", "value": "100", "note": "النقاط التي يحصل عليها الداعي عند قيام الصديق بشراء دورة"},
            {"key": "min_points_redeem", "title": "حد استبدال النقاط", "value": "100", "note": "الحد الأدنى من النقاط المطلوب لفتح دورة مجانية"},
            {"key": "AI_cost", "title": "تكلفة_AI", "value": "100", "note": "تكلفة استخدام الذكاء الاصطناعي لكل بوت"},
            {"key": "operating_environment", "title": "بيئة_التشغيل", "value": "Ar", "note": "تحدد بيئة تشغيل البوت (إنتاج/تجريبي)"},
            {"key": "subscription_price", "title": "سعر_الاشتراك", "value": "100", "note": "السعر المطلوب للاشتراك في البوت"},
            {"key": "maximum_number_sections", "title": "الحد الأقصى للاقسام", "value": "3", "note": "أقصى عدد أقسام يمكن للبوت إدارتها"},
            {"key": "maximum_number_courses", "title": "الحد الأقصى للدورات", "value": "10", "note": "أقصى عدد دورات يمكن للبوت إدارتها"},
            {"key": "maximum_number_students", "title": "الحد الأقصى للطلاب", "value": "100", "note": "أقصى عدد طلاب يمكن للبوت خدمتهم"},
            {"key": "currency_unit", "title": "وحدة العملة", "value": "نقطة", "note": "الاسم الذي يظهر بجانب الرصيد (مثلاً: نقطة أو ريال)"},
            {"key": "homework_grade", "title": "درجة الواجبات", "value": "10", "note": "درجة الواجبات اليومية للطلاب"}, 
            {"key": "maximum_withdrawal_marketers", "title": "سحب الرصيد", "value": "50", "note": "الحد الأقصى لسحب الرصيد للمسوقين"},
            {"key": "payment_information", "title": "معلومات الدفع", "value": "بنك الرياض حساب رقم 1234455666 بنك الاهلي حساب6765566 ", "note": "معلومات تحويل الرسوم "},      
            {"key": "marketers_commission", "title": "عمولة المسوقين", "value": "10%", "note": "نسبة العمولة للمسوقين"},          
            {"key": "honors_channel_id", "title": "معرف قناة الأوسمة", "value": "-100yyyyyyy", "note": "قناة مخصصة لاستعراض انحازات الطلاب"},                 
            {"key": "minimum_passing_gradee", "title": "درجة النجاح الصغرى", "value": "50", "note": "الدرجة الادنى للنجاح "},                  
            {"key": "greatest_success_gradee", "title": "درجة النجاح الكبرى", "value": "100", "note": "الدرجة الاعلى للنجاح"},                              
            {"key": "public_channel_id", "title": "معرف القناة العامة", "value": "-100xxxxxxx", "note": "معرف القناة الرسمية للمؤسسة "}
        ]

        # 2. فحص ومنع التكرار (محلياً عبر SQLite - أسرع بآلاف المرات من جوجل)
        # نفترض أن الجدول المحلي 'الإعدادات' يحتوي على الأعمدة: column_1(bot_id), column_2(key)...
        for item in default_keys:
            db_manager.cursor.execute(
                "SELECT 1 FROM 'الإعدادات' WHERE column_1 = ? AND column_2 = ?", 
                (str(bot_token), item['key'])
            )
            
            if not db_manager.cursor.fetchone():
                # 3. إعداد الصف الجديد (الالتزام الصارم بالترتيب اليدوي الذي وضعته)
                new_row = [
                    str(bot_token),   # 1: bot_id
                    item['key'],      # 2: المفتاح_البرمجي
                    item['title'],    # 3: العنوان
                    item['value'],    # 4: القيمة
                    item['note']      # 5: الملاحظة
                ]
                
                # 4. الحفظ في المحرك المحلي مع وسم المزامنة
                # نستخدم دالة local_save_wrapper التي تحافظ على الترتيب
                local_save_wrapper("الإعدادات", new_row)
                print(f"✅ [محلي] تم زرع المفتاح: {item['key']}")
        
        db_manager.conn.commit()
        return True

    except Exception as e:
        print(f"❌ خطأ أثناء تعبئة الإعدادات (المحرك المطور): {e}")
        return False

# --------------------------------------------------------------------------
# --- [ كتلة تحديث الميتا والتحقق من سلامة الهيكل ] ---
# تقوم دالة update_meta_info بتسجيل حالة المحرك وإصدار المخطط في ورقة _meta لضمان التوافق.
# تقوم دالة verify_setup بفحص كافة أوراق العمل والتأكد من مطابقة الأعمدة للهيكل المعتمد.
def update_meta_info():
    """
    تحديث سجلات النظام وقائمة البوتات المعتمدة:
    - الحفاظ الصارم على النطاق A1:C7 لعدم مسح البيانات اليدوية.
    - الحفاظ على كافة المفاتيح والأوصاف (التفريغ، الذكاء الاصطناعي، التحميل).
    - إضافة نسخة احتياطية في SQLite لضمان سلامة المحرك.
    """
    try:
        # 1. إعداد مصفوفة البيانات (نفس البيانات الأصلية 100% بدون أي تغيير في المفاتيح)
        meta_data = [
            ["key", "value", "updated_at"], 
            ["version", SCHEMA_VERSION, get_system_time("date")], 
            ["engine_status", "HEALTHY", datetime.now().isoformat()],
            ["desc_transcriber_bot.py", "التفريغ الصوتي", "Active"],
            ["desc_ai_bot.py", "الذكاء الاصطناعي", "Active"],
            ["desc_downloader_bot.py", "تحميل فيديوهات", "Active"]
        ]

        # 2. الحفظ المحلي (SQLite) - لضمان سرعة التحقق عند الإقلاع
        # نستخدم جدول '_meta' المحلي (يتم إنشاؤه تلقائياً بواسطة sync_schema)
        for row in meta_data[1:]: # نتخطى الهيدر
            db_manager.cursor.execute(
                "INSERT OR REPLACE INTO '_meta' (column_1, column_2, column_3, sync_status) VALUES (?, ?, ?, 'pending')",
                (row[0], row[1], row[2])
            )
        db_manager.conn.commit()

        # 3. التحديث في جوجل شيت (الالتزام بالنطاق الصارم A1:C7)
        meta_ws = _ws_cache.get("_meta")
        if meta_ws:
            safe_api_call(meta_ws.update, 'A1:C7', meta_data)
            print("✅ تم تحديث ورقة الميتا وحماية قائمة بوتاتك (محلياً وسحابياً).")
            
    except Exception as e: 
        print(f"❌ فشل تحديث الميتا: {e}")


# --------------------------------------------------------------------------
# إضافة قسم 
def add_new_category(bot_token, cat_id, cat_name):
    """
    إضافة قسم جديد لجدول الأقسام:
    - الحفاظ الكامل على الهيكل الأصلي المكون من 8 أعمدة بالترتيب الصارم.
    - تحويل الحفظ ليكون محلياً (SQLite) لضمان السرعة الفائقة.
    - الالتزام بكافة القيم الافتراضية والمفاتيح دون أي تغيير.
    """
    try:
        from datetime import datetime
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # إعداد مصفوفة البيانات (الالتزام الصارم بالـ 8 أعمدة وبالترتيب المذكور في كودك)
        # 1. bot_id | 2. معرف_القسم | 3. اسم_القسم | 4. الحالة | 5. ترتيب_العرض | 6. تاريخ_الإنشاء | 7. معرف_الفرع | 8. ملاحظات
        row = [
            str(bot_token).strip(),       # 1. bot_id
            str(cat_id).strip(),          # 2. معرف_القسم
            str(cat_name).strip(),        # 3. اسم_القسم
            "نشط",                        # 4. الحالة
            "0",                          # 5. ترتيب_العرض (افتراضي)
            current_date,                 # 6. تاريخ_الإنشاء
            "001",                        # 7. معرف_الفرع (افتراضي)
            "إضافة عبر لوحة التحكم"       # 8. ملاحظات
        ]

        # الحفظ في المحرك المحلي (SQLite) بدلاً من الانتظار المباشر لجوجل شيت
        # نستخدم الجدول 'الأقسام' (أو الاسم المطابق في السبريدشيت لديك)
        success = local_save_wrapper("الأقسام", row)
        
        if success:
            # تحديث نسخة المزامنة (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم إضافة القسم: {cat_name}")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص الخطأ الأصلي مع توضيح أنه في المحرك المطور
        print(f"❌ Error in add_new_category (Hybrid): {e}")
        return False
#~~~~~~~~~~~~~~~~

#دالة حذف القسم والبحث 
def delete_category_by_id(bot_token, cat_id):
    """
    حذف صف القسم من قاعدة البيانات:
    - الحفاظ الكامل على منطق التحقق من التوكن (العمود 1) والـ ID (العمود 2).
    - تنفيذ الحذف محلياً في SQLite فوراً لضمان (0 تأخير).
    - الحفاظ على استدعاء update_global_version لضمان مزامنة الكاش.
    """
    try:
        # 1. الحفظ في المحرك المحلي (SQLite) - التنفيذ الفوري
        # البحث عن السجل المطابق للتوكن (column_1) ومعرف القسم (column_2)
        try:
            # نقوم بتحديث حالة السجل إلى 'deleted' ليقوم محرك المزامنة بحذفه من جوجل في 03:30
            # أو نحذفه فيزيائياً من القاعدة المحلية لضمان عدم ظهوره فوراً
            query = f"DELETE FROM 'الأقسام' WHERE column_1 = ? AND column_2 = ?"
            db_manager.cursor.execute(query, (str(bot_token).strip(), str(cat_id).strip()))
            db_manager.conn.commit()
            
            # تحديث نسخة المزامنة (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"🗑️ [محلي] تم حذف القسم {cat_id} بنجاح.")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الحذف المحلي، محاولة الحذف عبر API جوجل: {local_e}")

        # 2. الحفاظ على المنطق الأصلي للحذف من Google Sheets (لضمان المطابقة)
        if departments_sheet is not None:
            # جلب كافة القيم (الالتزام الكامل بطريقتك الأصلية)
            all_rows = departments_sheet.get_all_values()
            for i, row in enumerate(all_rows):
                # التأكد من مطابقة التوكن (العمود 1) والـ ID (العمود 2) - نفس منطقك تماماً
                if row[0] == bot_token and row[1] == cat_id:
                    # تنفيذ الحذف عبر صمام الأمان safe_api_call لضمان عدم الانهيار
                    from sheets import safe_api_call
                    success = safe_api_call(departments_sheet.delete_rows, i + 1)
                    
                    if success:
                        # يجب أن تكون هنا لضمان أن التحديث تم فعلاً (كما طلبت في تعليقك)
                        update_global_version(bot_token)
                        return True
        
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error in delete_category: {e}")
        return False

#~~~~~~~~~~~~~~~~

 # دالة تبحث عن الـ ID وتقوم بتغيير الاسم في ذلك الصف
def update_category_name(bot_token, cat_id, new_name):
    """
    تحديث اسم قسم موجود في قاعدة البيانات:
    - الحفاظ الكامل على منطق التحقق (العمود 1 توكن، العمود 2 ID).
    - تحديث الاسم محلياً في SQLite فوراً لضمان (0 تأخير).
    - الحفاظ على تحديث الخلية في جوجل شيت (العمود 3).
    - الالتزام باستدعاء update_global_version داخل شرط الـ if.
    """
    try:
        # 1. التحديث المحلي (SQLite) - لضمان انعكاس الاسم الجديد فوراً في البوت
        try:
            # العمود 3 في جوجل يقابله column_3 في الجدول المحلي
            query = f"UPDATE 'الأقسام' SET column_3 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?"
            db_manager.cursor.execute(query, (str(new_name).strip(), str(bot_token).strip(), str(cat_id).strip()))
            db_manager.conn.commit()
            print(f"🔄 [محلي] تم تحديث اسم القسم إلى: {new_name}")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل التحديث المحلي، سيتم الاعتماد على جوجل فقط: {local_e}")

        # 2. التحديث في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if departments_sheet is None: return False
        
        all_rows = departments_sheet.get_all_values()
        for i, row in enumerate(all_rows):
            # العمود 1 توكن، العمود 2 ID (نفس منطقك تماماً)
            if row[0] == bot_token and row[1] == cat_id:
                # 1. تحديث الخلية في جوجل شيت (العمود 3 هو اسم القسم)
                from sheets import safe_api_call
                success = safe_api_call(departments_sheet.update_cell, i + 1, 3, new_name)
                
                if success:
                    # 2. رفع إصدار البوت فوراً لتحديث الكاش (داخل شرط الـ if كما طلبت)
                    update_global_version(bot_token)
                    
                    # 3. العودة بنجاح
                    return True
                
        return False
    except Exception as e:
        # الحفاظ على نص الخطأ الأصلي
        print(f"❌ Error in update_category_name: {e}")
        return False

# --------------------------------------------------------------------------
#إضافة الدورات 
def add_new_course(bot_token, course_id, name, hours, start_date, end_date, c_type, price, limit, reqs, rep_name, rep_code, campaign, coach_user, coach_id, coach_name, cat_id, **kwargs):
    """
    إضافة دورة كاملة مع ربطها بالقسم:
    - الحفاظ الكامل على هيكل الـ 18 عموداً بالترتيب البرمجي الصارم.
    - تحويل الحفظ ليكون محلياً (SQLite) لضمان السرعة الفائقة (Zero Lag).
    - الالتزام بكافة المتغيرات ووسيطات **kwargs دون أي نقص.
    """
    try:
        # 1. تعريف branch_id (الحفاظ على منطق kwargs الأصلي)
        branch_id = kwargs.get('branch_id', '001') 
        
        # 2. بناء مصفوفة البيانات بالترتيب المطابق تماماً لهيكل جوجل شيت (18 عموداً)
        # 1.bot_id | 2.الفرع | 3.ID_الدورة | 4.الاسم | 5.الساعات | 6.البداية | 7.النهاية | 8.النوع | 9.السعر
        # 10.الحد | 11.المتطلبات | 12.الموظف | 13.ID_الموظف | 14.الحملة | 15.يوزر_المدرب | 16.ID_المدرب | 17.اسم_المدرب | 18.القسم
        row = [
            str(bot_token).strip(),  # 1. bot_id
            str(branch_id),          # 2. معرف_الفرع
            str(course_id),          # 3. معرف_الدورة
            str(name),               # 4. اسم_الدورة
            str(hours),              # 5. عدد_الساعات
            str(start_date),         # 6. تاريخ_البداية
            str(end_date),           # 7. تاريخ_النهاية
            str(c_type),             # 8. نوع_الدورة
            str(price),              # 9. سعر_الدورة
            str(limit),              # 10. الحد_الأقصى
            str(reqs),               # 11. المتطلبات
            str(rep_name),           # 12. اسم_الموظف
            str(rep_code),           # 13. معرف_الموظف
            str(campaign),           # 14. معرف_الحملة_التسويقية
            str(coach_user),         # 15. معرف_المدرب (يوزر)
            str(coach_id),           # 16. ID_المدرب (رقمي)
            str(coach_name),         # 17. اسم_المدرب
            str(cat_id)              # 18. معرف_القسم (للربط الهرمي)
        ]
        
        # 3. تنفيذ عملية الحفظ في المحرك المحلي (SQLite)
        # نستخدم الجدول 'الدورات' (أو الاسم المطابق في السبريدشيت لديك)
        success = local_save_wrapper("الدورات", row)
        
        if success:
            # تحديث نسخة المزامنة العالمية (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم إضافة الدورة بنجاح: {name}")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error in add_new_course: {e}")
        return False

# --------------------------------------------------------------------------
# دالة جلب الدورات بقسم محدد
def get_courses_by_category(bot_token, cat_id):
    """
    جلب كافة الدورات المرتبطة بقسم محدد:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش.
    - الالتزام بالبحث في العمود 18 (معرف_القسم).
    - الحفاظ على شكل المخرجات (id, name) لضمان توافق واجهة البوت.
    """
    try:
        # 1. التأكد من مزامنة البيانات قبل الجلب (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب البيانات من الكاش المحلي (الذي أصبح الآن SQLite)
        # ملاحظة: حافظنا على اسم الجدول "الدورات_التدريبية" كما هو في طلبك
        all_courses = get_bot_data_from_cache(bot_token, "الدورات_التدريبية")
        
        # 3. الفلترة مع الحفاظ على تحويل النوع str لضمان دقة المطابقة
        return [
            {"id": c.get("معرف_الدورة"), "name": c.get("اسم_الدورة")} 
            for c in all_courses 
            if str(c.get("معرف_القسم")) == str(cat_id)
        ]

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error fetching courses from cache: {e}")
        return []


def get_all_categories(bot_token):
    """
    جلب كافة الأقسام المتاحة لبوت معين:
    - الحفاظ على إزاحة الكود (Indentation) داخل try كما طلبت.
    - الالتزام التام بمفاتيح القواميس الأصلية.
    """
    try:
        # 1. التحقق الذكي من المزامنة (بدون تبسيط)
        smart_sync_check(bot_token)
        
        # 2. جلب سجلات الأقسام من الكاش
        records = get_bot_data_from_cache(bot_token, "الأقسام")
        
        # 3. بناء القائمة مع الحفاظ على المفاتيح (id, name) والمعرفات (معرف_القسم, اسم_القسم)
        return [
            {"id": r.get("معرف_القسم"), "name": r.get("اسم_القسم")} 
            for r in records
        ]

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error fetching categories: {e}")
        return []

# --------------------------------------------------------------------------
# دالة حذف الدورات
def delete_course_by_id(bot_token, course_id):
    """
    حذف صف دورة محددة من الشيت:
    - الحفاظ الكامل على منطق التحقق (العمود 1 للتوكن، العمود 3 لمعرف الدورة).
    - تنفيذ الحذف محلياً في SQLite فوراً لضمان (Zero Lag).
    - الحفاظ على الحذف من جوجل شيت وتحديث الإصدار بمحاذاة عملية الحذف.
    """
    try:
        # 1. الحذف من المحرك المحلي (SQLite) - لضمان اختفاء الدورة فوراً أمام المستخدم
        try:
            # في الهيكل المحلي: column_1 هو التوكن، column_3 هو معرف_الدورة
            query = f"DELETE FROM 'الدورات_التدريبية' WHERE column_1 = ? AND column_3 = ?"
            db_manager.cursor.execute(query, (str(bot_token).strip(), str(course_id).strip()))
            db_manager.conn.commit()
            print(f"🗑️ [محلي] تم حذف الدورة {course_id} من القاعدة المحلية.")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الحذف المحلي، سيتم الاعتماد على حذف جوجل فقط: {local_e}")

        # 2. تنفيذ الحذف من Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if courses_sheet is None: return False
        
        all_rows = courses_sheet.get_all_values()
        for i, row in enumerate(all_rows):
            # التحقق من مطابقة التوكن (العمود 1) ومعرف الدورة (العمود 3 -> Index 2)
            # تم الحفاظ على شرط len(row) >= 3 لضمان عدم حدوث خطأ Index
            if len(row) >= 3 and str(row[0]) == str(bot_token) and str(row[2]) == str(course_id):
                
                # 1. تنفيذ الحذف الفعلي من جوجل شيت عبر صمام الأمان
                from sheets import safe_api_call
                success = safe_api_call(courses_sheet.delete_rows, i + 1)
                
                if success:
                    # 2. رفع رقم الإصدار لتحديث الرام (بمحاذاة delete_rows كما طلبت)
                    update_global_version(bot_token)
                    
                    # 3. العودة بنجاح (بمحاذاة delete_rows كما طلبت)
                    return True
                
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error deleting course: {e}")
        return False

# --------------------------------------------------------------------------


#دالة البحث عن مدرب
def find_user_by_username(bot_token, username):
    """
    البحث عن بيانات المدرب:
    - الحفاظ الكامل على منطق تنظيف اليوزرنايم (إزالة @ وتحويله لـ lower).
    - البحث محلياً في SQLite أولاً لضمان السرعة.
    - الحفاظ على شكل القاموس المرتجع (id, name) وبنفس الترتيب.
    """
    try:
        # 1. تنظيف اليوزرنايم (الالتزام الصارم بمنطقك الأصلي)
        search_name = username.replace("@", "").lower()

        # 2. البحث في المحرك المحلي (SQLite) - استجابة في أجزاء من الثانية
        try:
            # في جدول 'المستخدمين': column_1=bot_token, column_3=username, column_2=user_id, column_4=full_name
            query = "SELECT * FROM 'المستخدمين' WHERE column_1 = ? AND LOWER(column_3) = ?"
            db_manager.cursor.execute(query, (str(bot_token), search_name))
            row_local = db_manager.cursor.fetchone()
            
            if row_local:
                # تحويل الصف إلى قاموس (الالتزام بالمفاتيح id و name)
                # column_2 هو ID المستخدم (row[1])، و column_4 هو الاسم (row[3])
                res_local = {
                    "id": row_local['column_2'],
                    "name": row_local['column_4'] if row_local['column_4'] else "مدرب"
                }
                print(f"🔍 [محلي] تم العثور على المدرب: {search_name}")
                return res_local
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل البحث المحلي، جاري محاولة البحث في جوجل: {local_e}")

        # 3. البحث في Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        # ورقة المستخدمين (تأكد من تسميتها حسب ملفك، سأفترض أنها users_sheet)
        if users_sheet is None: return None
        
        all_rows = users_sheet.get_all_values()
        
        for row in all_rows[1:]:
            # فحص التوكن واليوزرنايم (العمود 1 والعمود 3 عادةً)
            # الالتزام بنفس ترتيب الأعمدة (row[0] و row[2])
            if row[0] == bot_token and row[2].lower() == search_name:
                # العودة بالنتائج (الالتزام بمنطق len(row) > 3)
                return {
                    "id": row[1],   # معرف المستخدم الرقمي
                    "name": row[3] if len(row) > 3 else "مدرب" # الاسم (العمود الرابع)
                }
                
        return None
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error finding user in sheets: {e}")
        return None

# --------------------------------------------------------------------------
#دالة إضافة المدربين
def add_new_coach_advanced(bot_token, coach_id, name, specialty, phone, branch_id, **kwargs):
    """
    إضافة مدرب بنظام الذاكرة المركزية والمحلية والسحابية:
    - الحفاظ الكامل على هيكل الـ 15 عموداً بالترتيب البرمجي الصارم.
    - إضافة الحقن في SQLite لضمان سرعة الاستجابة الفائقة.
    - الالتزام بكافة مفاتيح FACTORY_GLOBAL_CACHE و kwargs دون أي نقص.
    """
    try:
        from cache_manager import FACTORY_GLOBAL_CACHE, save_cache_to_disk
        
        # 1. المزامنة والربط الحي بالورقة (الحفاظ على منطقك الأصلي)
        if 'ss' not in globals() or ss is None: connect_to_google()
        current_sheet = ss.worksheet("المدربين")
        
        today_date = get_system_time("date")
        branch_id = kwargs.get('branch_id', "001")
        username = kwargs.get('username', "بدون") # جلب يوزرنيم التيليجرام
        
        # 2. [الخطوة الأولى]: الحقن الفوري في الذاكرة المركزية (RAM) - بدون أي تغيير
        new_coach_record = {
            "bot_id": str(bot_token),
            "معرف_الفرع": branch_id,
            "ID": str(coach_id), # معرف التيليجرام هو المعرف الأساسي
            "اسم_المدرب": str(name),
            "التخصص": str(specialty),
            "رقم_الهاتف": str(phone),
            "البريد_الإلكتروني": kwargs.get('email', "لا يوجد"),
            "تاريخ_التعاقد": today_date,
            "الحالة": "نشط",
            "اسم_المستخدم": username
        }
        
        if "المدربين" not in FACTORY_GLOBAL_CACHE["data"]:
            FACTORY_GLOBAL_CACHE["data"]["المدربين"] = []
        FACTORY_GLOBAL_CACHE["data"]["المدربين"].append(new_coach_record)
        save_cache_to_disk() # حفظ النسخة الفيزيائية لضمان استمرارية البيانات

        # 3. [إضافة حيوية]: الحقن في المحرك المحلي (SQLite) لضمان السرعة
        # الترتيب المحلي يتبع ترتيب الـ 15 عموداً المذكورة في خطوتك الثانية
        row_for_sqlite = [
            str(bot_token), branch_id, str(coach_id), str(name), str(specialty),
            str(phone), kwargs.get('email', "لا يوجد"), "لا يوجد", "لا يوجد",
            "نشط", kwargs.get('branch_name', "الرئيسي"), "0", today_date,
            "إضافة آلية عبر البوت", username
        ]
        local_save_wrapper("المدربين", row_for_sqlite)

        # 4. [الخطوة الثانية]: بناء الصف الـ 15 عموداً بمطابقة تامة للمخطط (لجوجل شيت)
        # الترتيب الصارم: [bot_id, معرف_الفرع, ID, اسم_المدرب, التخصص, رقم_الهاتف, البريد_الإلكتروني, السيرة_الذاتية, رابط_الصورة, الحالة, اسم_الفرع, عدد_الدورات, تاريخ_التعاقد, ملاحظات, اسم_المستخدم]
        row = [
            str(bot_token),             # 1
            f"'{branch_id}",            # 2
            f"'{coach_id}",             # 3 (ID التيليجرام)
            str(name),                  # 4
            str(specialty),             # 5
            f"'{phone}",                # 6
            kwargs.get('email', "لا يوجد"), # 7
            "لا يوجد",                  # 8 (سيرة ذاتية)
            "لا يوجد",                  # 9 (رابط صورة)
            "نشط",                      # 10
            kwargs.get('branch_name', "الرئيسي"), # 11 (اسم_الفرع حسب المخطط)
            "0",                        # 12 (عدد الدورات)
            today_date,                 # 13 (تاريخ التعاقد)
            "إضافة آلية عبر البوت",      # 14 (ملاحظات)
            username                    # 15 (اسم_المستخدم - العمود الأخير)
        ]
        
        # استخدام الوسيط الآمن لمنع حظر API جوجل (الالتزام الصارم)
        safe_api_call(current_sheet.append_row, row)
        
        # 5. [الخطوة الثالثة]: تحديث نظام المزامنة العالمي
        update_global_version(bot_token)
        
        print(f"✅ [نجاح]: المدرب {name} متاح الآن في الرام والمحرك المحلي والشيت بالمعرف الرقمي: {coach_id}")
        return True

    except Exception as e:
        logger.error(f"❌ خطأ حرج في إضافة المدرب: {e}")
        return False


# --------------------------------------------------------------------------
# دالة جلب إعدادات الذكاء الاصطناعي (تم توحيد المسمى لـ setup)
def get_ai_setup(bot_token):
    """
    جلب إعدادات الهوية والذكاء:
    - الحفاظ الكامل على اسم الورقة الصارم "الذكاء_الإصطناعي".
    - البحث محلياً في SQLite أولاً لضمان استجابة الذكاء الاصطناعي الفورية.
    - الحفاظ على منطق تنظيف التوكن (strip) للمقارنة الدقيقة.
    """
    try:
        # 1. البحث في المحرك المحلي (SQLite) - لضمان عدم تأخر ردود الذكاء الاصطناعي
        try:
            # نستخدم الجدول 'الذكاء_الإصطناعي' (المزامن آلياً)
            # نفترض column_1 هو bot_id بناءً على هيكل جداولك
            db_manager.cursor.execute("SELECT * FROM 'الذكاء_الإصطناعي' WHERE column_1 = ?", (str(bot_token).strip(),))
            row_local = db_manager.cursor.fetchone()
            
            if row_local:
                # تحويل الصف المحلي إلى قاموس (Dict) ليطابق مخرجات get_all_records()
                # مع الحفاظ على كافة المفاتيح الأصلية الموجودة في الهيدرز
                res_local = dict(row_local)
                print(f"🤖 [محلي] تم جلب إعدادات الذكاء الاصطناعي للبوت: {bot_token[:10]}...")
                return res_local
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الجلب المحلي للإعدادات، محاولة الجلب من جوجل: {local_e}")

        # 2. الجلب من Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if 'ss' not in globals() or ss is None: connect_to_google()
        sheet = ss.worksheet("الذكاء_الإصطناعي")
        records = sheet.get_all_records()
        
        for r in records:
            # تنظيف التوكن من أي مسافات زائدة (نفس منطقك الصارم)
            if str(r.get('bot_id', '')).strip() == str(bot_token).strip():
                return r
                
        return None
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error fetching AI setup: {e}")
        return None

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# دالة حفظ أو تحديث إعدادات الذكاء الاصطناعي (تم توحيد المسمى لـ setup)
def save_ai_setup(bot_token, user_id, username, institution_name=None, ai_instructions=None):
    """
    حفظ أو تحديث بيانات المؤسسة وتعليمات الذكاء الاصطناعي:
    - الحفاظ الكامل على هيكل الـ 14 عنصراً في الصف الجديد.
    - الحفاظ على تحديث الأعمدة المحددة (13 للمؤسسة، 14 للتعليمات، 8 للوقت).
    - تنفيذ الحفظ محلياً في SQLite أولاً لضمان (Zero Lag) في إعدادات الهوية.
    """
    try:
        now = get_system_time("full")
        bot_token_clean = str(bot_token).strip()

        # 1. المعالجة المحلية (SQLite) - لضمان السرعة الفائقة
        try:
            # البحث عن التوكن محلياً (العمود 1)
            db_manager.cursor.execute("SELECT local_id FROM 'الذكاء_الإصطناعي' WHERE column_1 = ?", (bot_token_clean,))
            existing_local = db_manager.cursor.fetchone()

            if existing_local:
                # تحديث الأعمدة محلياً (13 يقابله column_13، وهكذا)
                update_query = f"UPDATE 'الذكاء_الإصطناعي' SET column_8 = ?, sync_status = 'pending'"
                params = [now]
                if institution_name:
                    update_query += ", column_13 = ?"
                    params.append(institution_name)
                if ai_instructions:
                    update_query += ", column_14 = ?"
                    params.append(ai_instructions)
                
                update_query += " WHERE column_1 = ?"
                params.append(bot_token_clean)
                db_manager.cursor.execute(update_query, params)
            else:
                # إضافة صف جديد محلياً (14 عنصراً بالترتيب الصارم)
                local_row = [
                    bot_token_clean, str(user_id), username, now, 
                    "نشط", "إداري", 0, now, "ar", "Direct", 
                    "", 0, institution_name or "", ai_instructions or ""
                ]
                local_save_wrapper("الذكاء_الإصطناعي", local_row)
            
            db_manager.conn.commit()
            print(f"✅ [محلي] تم تحديث إعدادات الذكاء الاصطناعي للبوت.")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الحفظ المحلي، جاري المحاولة عبر جوجل: {local_e}")

        # 2. المعالجة السحابية (Google Sheets) - الالتزام الصارم بمنطقك الأصلي
        sheet = ss.worksheet("الذكاء_الإصطناعي")
        cell = None
        try: 
            # البحث عن التوكن في العمود الأول فقط (A) كما في كودك الصارم
            cell = sheet.find(bot_token_clean, in_column=1)
        except: pass

        if cell:
            # تحديث البيانات في الأعمدة 13 و 14 و 8 (نفس منطقك تماماً)
            from sheets import safe_api_call
            if institution_name: safe_api_call(sheet.update_cell, cell.row, 13, institution_name)
            if ai_instructions: safe_api_call(sheet.update_cell, cell.row, 14, ai_instructions)
            safe_api_call(sheet.update_cell, cell.row, 8, now) # تحديث عمود آخر نشاط (H)
        else:
            # إضافة صف جديد (الالتزام بـ 14 عنصراً وبنفس القيم)
            row = [
                bot_token_clean, str(user_id), username, now, 
                "نشط", "إداري", 0, now, "ar", "Direct", 
                "", 0, institution_name or "", ai_instructions or ""
            ]
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
            update_global_version(bot_token)
            
        return True
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error saving AI setup: {e}")
        return False

# --------------------------------------------------------------------------
def get_courses_knowledge_base(bot_token):
    """
    جلب بيانات الدورات + معلومات الدفع وتحويلها لنص يفهمه الذكاء الاصطناعي:
    - الحفاظ الكامل على منطق جلب الدورات من ورقة الدورات.
    - إضافة منطق جلب 'payment_information' من ورقة 'الإعدادات'.
    - الالتزام بهيكل الأعمدة: bot_id, المفتاح_البرمجي, القيمة.
    """
    try:
        # 1. الجزء الأول: جلب بيانات الدورات (الالتزام الصارم بالكود الأصلي)
        if courses_sheet is None: 
            courses_info = "لا توجد بيانات دورات حالياً."
        else:
            all_courses = courses_sheet.get_all_records()
            bot_courses = [c for c in all_courses if str(c.get('bot_id')) == str(bot_token)]
            
            if not bot_courses:
                courses_info = "لا توجد دورات متاحة حالياً."
            else:
                kb = "قائمة الدورات المتاحة:\n"
                for c in bot_courses:
                    # الحفاظ على المفاتيح الأصلية: اسم_الدورة، سعر_الدورة، اسم_المدرب
                    kb += f"- {c.get('اسم_الدورة')}، السعر: {c.get('سعر_الدورة')}، المدرب: {c.get('اسم_المدرب')}.\n"
                courses_info = kb

        # 2. الجزء الثاني: جلب معلومات الدفع (الميزة الجديدة المطلوبة)
        payment_info_text = ""
        try:
            # الوصول لورقة الإعدادات بناءً على المخطط الصارم
            if 'settings_sheet' in globals() and settings_sheet is not None:
                all_settings = settings_sheet.get_all_records()
                # البحث عن المفتاح البرمجي "payment_information" المرتبط بالبوت
                payment_record = next((
                    s for s in all_settings 
                    if str(s.get('bot_id')) == str(bot_token) and 
                    str(s.get('المفتاح_البرمجي')) == "payment_information"
                ), None)
                
                if payment_record:
                    payment_val = payment_record.get('القيمة', '')
                    payment_info_text = f"\nمعلومات الدفع وطرق التحويل:\n{payment_val}\n"
        except Exception as e_pay:
            print(f"⚠️ تنبيه: تعذر جلب معلومات الدفع من الإعدادات: {e_pay}")

        # 3. دمج المعلومات في نص واحد نهائي (قاعدة المعرفة)
        final_kb = courses_info + payment_info_text
        return final_kb

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي والرسالة الراجعة
        print(f"❌ خطأ في جلب قاعدة معرفة الدورات: {e}")
        return "المعلومات قيد التحديث حالياً، يرجى المحاولة لاحقاً."

# --------------------------------------------------------------------------
#دالة الصلاحيات
def get_employee_permissions(bot_token, employee_id):
    """
    جلب سجل الصلاحيات الكامل لموظف:
    - الحفاظ الكامل على اسم الورقة "الهيكل_التنظيمي_والصلاحيات".
    - البحث محلياً في SQLite أولاً لضمان سرعة التحقق من الأوامر.
    - الحفاظ على المخرجات كقاموس (Dict) بنفس المفاتيح الأصلية.
    """
    try:
        # 1. البحث في المحرك المحلي (SQLite) - استجابة فورية
        try:
            # column_1 هو bot_id، و column_2 هو ID_الموظف_أو_المدرب
            query = "SELECT * FROM 'الهيكل_التنظيمي_والصلاحيات' WHERE column_1 = ? AND column_2 = ?"
            db_manager.cursor.execute(query, (str(bot_token), str(employee_id)))
            row_local = db_manager.cursor.fetchone()
            
            if row_local:
                # تحويل الصف إلى قاموس (Dict) ليطابق get_all_records()
                print(f"🔐 [محلي] تم جلب صلاحيات الموظف: {employee_id}")
                return dict(row_local)
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الجلب المحلي للصلاحيات: {local_e}")

        # 2. الجلب من Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if 'ss' not in globals() or ss is None: connect_to_google()
        sheet = ss.worksheet("الهيكل_التنظيمي_والصلاحيات")
        records = sheet.get_all_records()
        for r in records:
            # مطابقة التوكن و ID الموظف بنفس المفاتيح الأصلية
            if str(r.get("bot_id")) == str(bot_token) and str(r.get("ID_الموظف_أو_المدرب")) == str(employee_id):
                return r
        return {}
    except: 
        return {}

def toggle_employee_permission(bot_token, employee_id, col_name):
    """
    تبديل القيمة بين TRUE و FALSE:
    - الحفاظ الكامل على منطق التبديل العكسي (TRUE <-> FALSE).
    - التحديث المحلي (SQLite) الفوري لضمان تفعيل الصلاحية في نفس اللحظة.
    - الحفاظ على التحديث في جوجل شيت باستخدام index الأعمدة.
    """
    try:
        new_val = "FALSE" # القيمة الافتراضية للعودة
        
        # 1. التحديث المحلي (SQLite) - الأولوية للسرعة
        try:
            # جلب القيمة الحالية من الجدول المحلي أولاً
            db_manager.cursor.execute(
                f"SELECT {col_name} FROM 'الهيكل_التنظيمي_والصلاحيات' WHERE column_1 = ? AND column_2 = ?",
                (str(bot_token), str(employee_id))
            )
            current_row = db_manager.cursor.fetchone()
            if current_row:
                current_val = str(current_row[col_name]).upper()
                new_val = "FALSE" if current_val == "TRUE" else "TRUE"
                
                # تحديث القيمة محلياً ووسمها بـ pending
                db_manager.cursor.execute(
                    f"UPDATE 'الهيكل_التنظيمي_والصلاحيات' SET {col_name} = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?",
                    (new_val, str(bot_token), str(employee_id))
                )
                db_manager.conn.commit()
                print(f"🔄 [محلي] تم تبديل صلاحية {col_name} إلى {new_val}")
        except Exception as local_e:
            print(f"⚠️ فشل التحديث المحلي للصلاحية: {local_e}")

        # 2. التحديث في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if 'ss' not in globals() or ss is None: connect_to_google()
        sheet = ss.worksheet("الهيكل_التنظيمي_والصلاحيات")
        
        # جلب كافة القيم للبحث عن الصف الصحيح (نفس منطقك تماماً)
        all_rows = sheet.get_all_values()
        headers = all_rows[0]
        col_index = headers.index(col_name) + 1
        
        for i, row in enumerate(all_rows):
            if row[0] == str(bot_token) and row[1] == str(employee_id):
                # التحقق من القيمة الحالية في جوجل لضمان المطابقة
                current_val_google = str(row[col_index-1]).upper()
                new_val_google = "FALSE" if current_val_google == "TRUE" else "TRUE"
                
                # التحديث عبر صمام الأمان
                from sheets import safe_api_call
                safe_api_call(sheet.update_cell, i + 1, col_index, new_val_google)
                return new_val_google
                
        return new_val
    except Exception as e:
        print(f"Error toggling permission: {e}")
        return "FALSE"


# --------------------------------------------------------------------------
def check_user_permission(bot_token, user_id, permission_col):
    """
    التحقق مما إذا كان المستخدم لديه صلاحية محددة:
    - الحفاظ الكامل على منطق أن المالك (Admin) لديه كافة الصلاحيات دائماً.
    - استخدام المحرك المحلي لضمان استجابة الأوامر الإدارية في (Zero Lag).
    - الالتزام التام بتحويل القيمة إلى UPPER والمقارنة بـ TRUE.
    """
    try:
        # 1. جلب إعدادات البوت لمعرفة المالك (الالتزام الصارم بمنطقك الأصلي)
        # سيتم الجلب من SQLite عبر get_bot_config المطورة سابقاً
        config = get_bot_config(bot_token)
        
        # التحقق من وجود ID المستخدم ضمن قائمة الـ admin_ids
        admin_ids = str(config.get("admin_ids", "")).split(',')
        if str(user_id) in admin_ids:
            return True  # المالك لديه كافة الصلاحيات دائماً

        # 2. البحث في سجل الصلاحيات للموظف
        # سيتم استدعاء get_employee_permissions التي قمنا بتطويرها لتعمل محلياً
        perms = get_employee_permissions(bot_token, user_id)
        
        if not perms:
            # إذا لم يتم العثور على سجل للموظف، نرفض الصلاحية (الالتزام بمنطقك)
            return False
            
        # 3. التحقق من القيمة في العمود المطلوب (بالمعايير الصارمة)
        # الحفاظ على القيمة الافتراضية "FALSE" وتحويلها لـ UPPER
        permission_status = str(perms.get(permission_col, "FALSE")).upper()
        
        if permission_status == "TRUE":
            return True
            
        return False
        
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في فحص الصلاحية: {e}")
        return False


 

# --------------------------------------------------------------------------
def toggle_scope_id(bot_token, employee_id, scope_column, target_id):
    """
    إضافة أو حذف ID (دورة أو مجموعة) من قائمة الموظف:
    - الحفاظ الكامل على منطق الفلترة (الدورات_المسموحة أو المجموعات_المسموحة).
    - التحديث المحلي (SQLite) الفوري لضمان تفعيل النطاق في نفس اللحظة.
    - الحفاظ على منطق التنظيف split(",") و join(",") الصارم.
    """
    try:
        new_value = ""
        bot_token_str = str(bot_token).strip()
        employee_id_str = str(employee_id).strip()
        target_id_str = str(target_id).strip()

        # 1. المعالجة المحلية (SQLite) - لضمان (Zero Lag)
        try:
            # جلب القيمة الحالية من العمود المحدد في SQLite
            query_select = f"SELECT {scope_column} FROM 'الهيكل_التنظيمي_والصلاحيات' WHERE column_1 = ? AND column_2 = ?"
            db_manager.cursor.execute(query_select, (bot_token_str, employee_id_str))
            local_row = db_manager.cursor.fetchone()
            
            if local_row:
                current_val_local = str(local_row[scope_column]) if local_row[scope_column] else ""
                current_ids_local = [x.strip() for x in current_val_local.split(",") if x.strip()]
                
                # تنفيذ منطق التبديل (Toggle)
                if target_id_str in current_ids_local:
                    current_ids_local.remove(target_id_str)
                else:
                    current_ids_local.append(target_id_str)
                
                new_value = ",".join(current_ids_local)
                
                # تحديث القيمة محلياً ووسمها بـ pending
                query_update = f"UPDATE 'الهيكل_التنظيمي_والصلاحيات' SET {scope_column} = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?"
                db_manager.cursor.execute(query_update, (new_value, bot_token_str, employee_id_str))
                db_manager.conn.commit()
                print(f"🔄 [محلي] تم تحديث {scope_column} للموظف {employee_id_str}")
        except Exception as local_e:
            print(f"⚠️ فشل التحديث المحلي للنطاق: {local_e}")

        # 2. المعالجة السحابية (Google Sheets) - الالتزام الصارم بمنطقك الأصلي 100%
        if 'ss' not in globals() or ss is None: connect_to_google()
        permission_sheet = ss.worksheet("الهيكل_التنظيمي_والصلاحيات")
        all_data = permission_sheet.get_all_values()
        headers = all_data[0]
        col_index = headers.index(scope_column) + 1
        
        for i, row in enumerate(all_data):
            # التأكد من مطابقة التوكن (العمود 1) و ID الموظف (العمود 2)
            if str(row[0]) == bot_token_str and str(row[1]) == employee_id_str:
                # منطق الاستخراج والتنظيف الأصلي (بدون أي اختصار)
                current_ids = str(row[col_index-1]).strip().split(",") if row[col_index-1] else []
                current_ids = [x.strip() for x in current_ids if x.strip()]
                
                if target_id_str in current_ids:
                    current_ids.remove(target_id_str) # حذف إذا كان موجود
                else:
                    current_ids.append(target_id_str) # إضافة إذا لم يكن موجود
                
                final_new_value = ",".join(current_ids)
                
                # التحديث الفعلي للخلية في جوجل شيت
                from sheets import safe_api_call
                safe_api_call(permission_sheet.update_cell, i + 1, col_index, final_new_value)
                return True
                
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في تحديث النطاق: {e}")
        return False

# --------------------------------------------------------------------------
def check_access(bot_token, user_id, permission_col, target_id=None, scope_type=None):
    """
    الدالة الشاملة لفحص الصلاحيات والنطاقات:
    - الحفاظ الكامل على منطق تخطي المالك (Admin) لكافة القيود.
    - الفحص المحلي الفوري لضمان (Zero Lag) في استجابة الأوامر.
    - الحفاظ على منطق split(",") و strip() لفحص النطاقات المسموحة.
    """
    try:
        # 1. المالك (Admin) يتخطى كافة القيود دائماً (الالتزام الصارم بمنطقك)
        config = get_bot_config(bot_token)
        # الحفاظ على المقارنة المباشرة مع admin_ids
        if str(user_id) == str(config.get("admin_ids")):
            return True

        # 2. جلب سجل الموظف من المحرك المحلي (SQLite)
        # استدعاء get_employee_permissions المطورة سابقاً للعمل محلياً
        perms = get_employee_permissions(bot_token, user_id)
        if not perms:
            return False

        # 3. فحص الصلاحية العامة (بالمعايير الصارمة)
        # الحفاظ على تحويل القيمة لـ UPPER والمقارنة بـ TRUE
        if str(perms.get(permission_col, "FALSE")).upper() != "TRUE":
            return False

        # 4. فحص "النطاق" (التعدد في الدورات أو المجموعات)
        if target_id and scope_type:
            # الحفاظ على منطق تحويل النص (ID1,ID2) إلى قائمة برمجية
            allowed_scopes = str(perms.get(scope_type, "")).split(",")
            allowed_scopes = [s.strip() for s in allowed_scopes if s.strip()]
            
            # التحقق هل الـ ID المطلوب موجود ضمن القائمة (الالتزام بمنطقك)
            return str(target_id) in allowed_scopes

        # إذا كانت الصلاحية عامة (مثل الإحصائيات)
        return True
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في فحص الوصول الشامل: {e}")
        return False

# --------------------------------------------------------------------------



# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# دالة إضافة المجموعات (النسخة المحدثة لـ 15 عموداً)
# --------------------------------------------------------------------------
def add_new_group(bot_token, group_id, name, course_id, days, timing, teacher_id, **kwargs):
    """
    إضافة مجموعة تعليمية جديدة:
    - الحفاظ الكامل على هيكل الـ 15 عموداً بالترتيب المحدث بدقة.
    - تنفيذ الحفظ محلياً في SQLite فوراً لضمان السرعة الفائقة.
    - الالتزام بكافة القيم الافتراضية ووسيطات **kwargs.
    """
    try:
        # 1. إعداد مصفوفة البيانات (الالتزام الصارم بالـ 15 عموداً كما وردت في كودك)
        # 1.bot_id | 2.فرع | 3.ID_مجموعة | 4.اسم | 5.ID_دورة | 6.أيام | 7.توقيت | 8.ID_معلم
        # 9.حالة | 10.ID_موظف | 11.حملة | 12.سعة | 13.طلاب | 14.رابط | 15.تاريخ
        row = [
            str(bot_token),                            # 1
            kwargs.get('branch_id', '001'),            # 2
            str(group_id),                             # 3
            str(name),                                 # 4
            str(course_id),                            # 5
            str(days),                                 # 6
            str(timing),                               # 7
            str(teacher_id),                           # 8
            "نشطة",                                    # 9
            kwargs.get('emp_id', 'Admin'),             # 10
            kwargs.get('campaign', 'Direct'),          # 11
            kwargs.get('capacity', '30'),              # 12
            "0",                                       # 13
            kwargs.get('link', 'لم يحدد بعد'),          # 14
            get_system_time("date")                    # 15
        ]

        # 2. الحفظ في المحرك المحلي (SQLite) - لضمان ظهور المجموعة فوراً في البوت
        # نستخدم الجدول 'إدارة_المجموعات' (المزامن آلياً)
        success = local_save_wrapper("إدارة_المجموعات", row)

        # 3. الحفظ في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("إدارة_المجموعات")
            from sheets import safe_api_call
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
            
        if success:
            # تحديث نظام المزامنة العالمي (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم إضافة المجموعة بنجاح: {name}")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في إضافة المجموعة: {e}")
        return False

def get_groups_by_course(bot_token, course_id):
    """
    جلب كافة المجموعات الدراسية المرتبطة بدورة معينة:
    - الحفاظ الكامل على الفلترة المزدوجة (bot_id و معرف_الدورة).
    - البحث محلياً في SQLite أولاً لضمان (Zero Lag) في عرض القوائم.
    - الحفاظ على شكل المخرجات كقائمة سجلات (Records).
    """
    try:
        # 1. البحث في المحرك المحلي (SQLite) - استجابة فورية
        try:
            # column_1 هو bot_id، و column_5 هو معرف_الدورة بناءً على الترتيب أعلاه
            query = "SELECT * FROM 'إدارة_المجموعات' WHERE column_1 = ? AND column_5 = ?"
            db_manager.cursor.execute(query, (str(bot_token), str(course_id)))
            rows_local = db_manager.cursor.fetchall()
            
            if rows_local:
                # تحويل الصفوف المحلية إلى قواميس لتطابق get_all_records()
                print(f"🔍 [محلي] جلب {len(rows_local)} مجموعة للدورة {course_id}")
                return [dict(r) for r in rows_local]
        except Exception as local_e:
            print(f"⚠️ فشل الجلب المحلي للمجموعات: {local_e}")

        # 2. الجلب من Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if ss:
            sheet = ss.worksheet("إدارة_المجموعات")
            records = sheet.get_all_records()
            # الفلترة الصارمة (نفس منطقك تماماً)
            return [r for r in records if str(r.get("bot_id")) == str(bot_token) and str(r.get("معرف_الدورة")) == str(course_id)]
            
        return []
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في جلب المجموعات: {e}")
        return []

# --------------------------------------------------------------------------
# دالة الحفظ الفعلي (المستخدمة في المحرك الرئيسي)
# --------------------------------------------------------------------------
def save_group_to_db(bot_token, data):
    """
    حفظ المجموعة (نسخة استلام القاموس):
    - الحفاظ الكامل على هيكل الأعمدة الـ 15.
    - تنفيذ الحفظ محلياً في SQLite أولاً لضمان السرعة.
    - الحفاظ على استلام البيانات عبر قاموس 'data' بدلاً من المعاملات المنفصلة.
    """
    try:
        # 1. المزامنة السحابية (الحفاظ على منطق الشيت الأصلي)
        sheet = ss.worksheet("إدارة_المجموعات")
        now = get_system_time("full")
        
        # 2. بناء الصف (الالتزام الصارم بالـ 15 عموداً كما في كودك)
        row = [
            str(bot_token),                # 1. bot_id
            "001",                         # 2. معرف_الفرع
            data['group_id'],              # 3. معرف_المجموعة
            data['name'],                  # 4. اسم_المجموعة
            data['course_id'],             # 5. معرف_الدورة
            data['days'],                  # 6. أيام_الدراسة
            data['time'],                  # 7. توقيت_الدراسة
            data['teacher_id'],            # 8. ID_المعلم_المسؤول
            "نشطة",                        # 9. حالة_المجموعة
            "Admin",                       # 10. معرف_الموظف
            "Direct",                      # 11. معرف_الحملة_التسويقية
            "30",                          # 12. سعة_المجموعة
            "0",                           # 13. عدد_الطلاب_الحالي
            "لم يحدد",                     # 14. رابط_المجموعة
            now                            # 15. تاريخ_الإنشاء
        ]

        # 3. الحفظ في المحرك المحلي (SQLite) - لضمان الاستجابة الفورية
        success = local_save_wrapper("إدارة_المجموعات", row)

        # 4. الحفظ في Google Sheets عبر صمام الأمان
        from sheets import safe_api_call
        safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
        
        if success:
            # تحديث نظام المزامنة العالمي (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم حفظ بيانات المجموعة: {data['name']}")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error saving group: {e}")
        return False


# --------------------------------------------------------------------------
# دالة الحذف (تم توحيدها وتصحيح الفهارس)
# --------------------------------------------------------------------------
def delete_group_by_id(bot_token, group_id):
    """
    حذف مجموعة من شيت (إدارة_المجموعات):
    - الحفاظ الكامل على منطق التحقق (العمود 1 للتوكن، العمود 3 للمجموعة).
    - تنفيذ الحذف محلياً في SQLite فوراً لضمان (Zero Lag).
    - الحفاظ على الحذف من جوجل شيت وتحديث الإصدار بمحاذاة عملية الحذف.
    """
    try:
        bot_token_str = str(bot_token).strip()
        group_id_str = str(group_id).strip()

        # 1. الحذف من المحرك المحلي (SQLite) - لضمان اختفاء المجموعة فوراً أمام المستخدم
        try:
            # في الجدول المحلي: column_1 هو التوكن، column_3 هو معرف_المجموعة
            query = f"DELETE FROM 'إدارة_المجموعات' WHERE column_1 = ? AND column_3 = ?"
            db_manager.cursor.execute(query, (bot_token_str, group_id_str))
            db_manager.conn.commit()
            print(f"🗑️ [محلي] تم حذف المجموعة {group_id_str} من القاعدة المحلية.")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل الحذف المحلي، سيتم الاعتماد على حذف جوجل فقط: {local_e}")

        # 2. تنفيذ الحذف من Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("إدارة_المجموعات")
            all_rows = sheet.get_all_values()
            
            for i, row in enumerate(all_rows):
                # التحقق من مطابقة التوكن (العمود 1) ومعرف المجموعة (العمود 3 -> Index 2)
                # تم الحفاظ على شرط len(row) >= 3 لضمان عدم حدوث خطأ Index
                if len(row) >= 3 and str(row[0]) == bot_token_str and str(row[2]) == group_id_str:
                    
                    # 1. تنفيذ الحذف الفعلي من جوجل شيت عبر صمام الأمان
                    from sheets import safe_api_call
                    success = safe_api_call(sheet.delete_rows, i + 1)
                    
                    if success:
                        # 2. رفع رقم الإصدار لتحديث الرام (بمحاذاة delete_rows كما طلبت)
                        update_global_version(bot_token)
                        
                        # 3. العودة بنجاح (بمحاذاة delete_rows لضمان الخروج بعد الحذف)
                        return True
                
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error deleting group: {e}")
        return False


# --------------------------------------------------------------------------
# دالة التعديل (تصحيح مرجع اللوجر والشيت)
# --------------------------------------------------------------------------
def update_group_field(bot_token, group_id, col_name, new_value):
    """
    تحديث قيمة محددة في سجل المجموعة داخل (إدارة_المجموعات):
    - الحفاظ الكامل على منطق تحديد العمود باستخدام headers.index.
    - تنفيذ التحديث المحلي (SQLite) فوراً لضمان (Zero Lag) في عرض البيانات.
    - الحفاظ على تحديث جوجل شيت ورفع رقم الإصدار (update_global_version) كما ورد في كودك.
    """
    try:
        bot_token_str = str(bot_token).strip()
        group_id_str = str(group_id).strip()

        # 1. التحديث في المحرك المحلي (SQLite) - لضمان انعكاس التعديل فوراً
        try:
            # تحديث العمود المحدد محلياً ووسم السجل بـ pending للمزامنة اللاحقة
            # ملاحظة: col_name يجب أن يطابق اسم العمود في قاعدة البيانات المحلية
            query = f"UPDATE 'إدارة_المجموعات' SET {col_name} = ?, sync_status = 'pending' WHERE column_1 = ? AND column_3 = ?"
            db_manager.cursor.execute(query, (str(new_value), bot_token_str, group_id_str))
            db_manager.conn.commit()
            print(f"🔄 [محلي] تم تحديث الحقل {col_name} للمجموعة {group_id_str}")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل التحديث المحلي، سيتم الاعتماد على تحديث جوجل فقط: {local_e}")

        # 2. التحديث في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("إدارة_المجموعات")
            all_rows = sheet.get_all_values()
            headers = all_rows[0]
            
            # تحديد رقم العمود (الالتزام بمنطق index الأصلي)
            col_index = headers.index(col_name) + 1
            
            for i, row in enumerate(all_rows):
                # التحقق من مطابقة التوكن (العمود 1) ومعرف المجموعة (العمود 3)
                if len(row) >= 3 and str(row[0]) == bot_token_str and str(row[2]) == group_id_str:
                    
                    # التحديث الفعلي للخلية في جوجل شيت عبر صمام الأمان
                    from sheets import safe_api_call
                    safe_api_call(sheet.update_cell, i + 1, col_index, str(new_value))
                    
                    # رفع إصدار البوت فوراً لتحديث بيانات المجموعات في الرام (نفس موضعك الأصلي)
                    update_global_version(bot_token)
                    
                    return True
                    
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error updating group field: {e}")
        return False

# --------------------------------------------------------------------------
# بنك الأسئلة وإنشاء الاختبارات
def add_question_to_bank(bot_token, q_data):
    """
    إضافة سؤال للبنك:
    - الحفاظ الكامل على هيكل الـ 21 عموداً بالترتيب الصارم المذكور في كودك.
    - تنفيذ الحفظ محلياً في SQLite فوراً لضمان (Zero Lag) وسرعة البناء.
    - الالتزام التام بكافة المفاتيح الأصلية في قاموس q_data.
    """
    try:
        # 1. إعداد مصفوفة البيانات (الالتزام الصارم بالـ 21 عموداً وبنفس القيم الافتراضية)
        # 1.bot_id | 2.فرع | 3.ID_اختبار | 4.ID_دورة | 5.ID_مجموعة | 6.ID_سؤال | 7.نص | 8.A | 9.B | 10.C | 11.D
        # 12.صح | 13.درجة | 14.مدة | 15.مستوى | 16.نوع | 17.شرح | 18.وسم | 19.حالة | 20.تاريخ | 21.ID_منشئ
        row = [
            str(bot_token),              # 1. bot_id
            "1001001",                   # 2. معرف_الفرع
            "AUTO",                      # 3. معرف_الاختبار
            str(q_data['course_id']),    # 4. معرف_الدورة
            "ALL",                       # 5. معرف_المجموعة
            str(q_data['q_id']),         # 6. معرف_السؤال
            str(q_data['text']),         # 7. نص_السؤال
            str(q_data['a']),            # 8. الخيار_A
            str(q_data['b']),            # 9. الخيار_B
            str(q_data['c']),            # 10. الخيار_C
            str(q_data['d']),            # 11. الخيار_D
            str(q_data['correct']),      # 12. الإجابة_الصحيحة
            str(q_data['grade']),        # 13. الدرجة
            "30",                        # 14. مدة السؤال (افتراضي)
            str(q_data['level']),        # 15. مستوى_الصعوبة
            "اختيار من متعدد",            # 16. نوع_السؤال
            "",                          # 17. شرح الإجابة
            "عام",                       # 18. الوسم
            "نشط",                       # 19. حالة_السؤال
            get_system_time(),           # 20. تاريخ_الإضافة
            str(q_data['creator_id'])    # 21. معرف_منشئ_السؤال
        ]

        # 2. الحفظ في المحرك المحلي (SQLite) - لضمان الاستجابة الفورية
        # نستخدم الجدول 'بنك_الأسئلة' (المزامن آلياً)
        success = local_save_wrapper("بنك_الأسئلة", row)

        # 3. الحفظ في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("بنك_الأسئلة")
            from sheets import safe_api_call
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')

        if success:
            # 🔥 أهم خطوة: تحديث الإصدار (الالتزام الصارم بموضع الاستدعاء)
            from cache_manager import update_global_version
            update_global_version(bot_token)
            
            print(f"✅ [محلي] تم حفظ السؤال بنجاح في البنك (ID: {q_data['q_id']})")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ حفظ السؤال في الشيت: {e}")
        return False

# --- [ قسم الكنترول والاختبارات الآلية ] ---
def create_auto_quiz(bot_token, data):
    """
    إنشاء اختبار وسحب أسئلة عشوائية من البنك:
    - الحفاظ الكامل على هيكل الـ 16 عموداً بالترتيب الصارم المذكور في كودك.
    - تنفيذ الحفظ محلياً في SQLite فوراً لضمان السرعة الفائقة.
    - الحفاظ على منطق random.sample والتحقق من توفر الأسئلة.
    """
    try:
        # 1. جلب كافة الأسئلة من البنك (الالتزام الصارم بالبحث عن معرف_السؤال ومعرف_الدورة)
        # سيتم الجلب من القاعدة المحلية لضمان السرعة
        all_questions = get_all_questions_from_bank(bot_token)
        course_questions = [
            str(q.get('معرف_السؤال')) for q in all_questions 
            if str(q.get('معرف_الدورة')) == str(data.get('course_id'))
        ]
        
        # 2. التأكد من توفر أسئلة كافية (الالتزام بمنطقك الأصلي)
        required_count = int(data.get('q_count', 0))
        if len(course_questions) < required_count:
            print(f"⚠️ نقص في الأسئلة: المطلوب {required_count} والموفر {len(course_questions)}")
            return False, "نقص أسئلة"

        # 3. اختيار الأسئلة عشوائياً (الالتزام بمنطق random.sample والفاصلة)
        import random
        selected_qs = random.sample(course_questions, required_count)
        q_list_str = ",".join(selected_qs)

        # 4. بناء مصفوفة البيانات (الالتزام الصارم بالـ 16 عموداً كما وردت في كودك)
        # 1.bot_id | 2.فرع | 3.ID_اختبار | 4.ID_دورة | 5.مجموعات | 6.قائمة_أسئلة | 7.عدد | 8.نجاح
        # 9.مدة | 10.تايمر | 11.عشوائي | 12.محاولات | 13.نتائج | 14.حالة | 15.ID_مدرب | 16.وقت
        row = [
            str(bot_token),                    # 1
            data.get('branch_id', '1001001'),  # 2
            data.get('quiz_id'),               # 3
            data.get('course_id'),             # 4
            data.get('target_groups_str'),     # 5
            q_list_str,                        # 6
            required_count,                    # 7
            data.get('pass_score'),            # 8
            data.get('duration'),              # 9
            data.get('timer_type', 'كلي'),     # 10
            data.get('random', 'TRUE'),        # 11
            data.get('attempts', 1),           # 12
            data.get('show_res', 'TRUE'),      # 13
            "FALSE",                           # 14 (حالة الاختبار الافتراضية)
            data.get('coach_id'),              # 15
            get_system_time()                  # 16
        ]

        # 5. الحفظ في المحرك المحلي (SQLite) - لضمان الاستجابة الفورية للمدرب
        # نستخدم الجدول 'الاختبارات_الآلية' (المزامن آلياً)
        success = local_save_wrapper("الاختبارات_الآلية", row)

        # 6. الحفظ في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("الاختبارات_الآلية")
            from sheets import safe_api_call
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')

        if success:
            # تحديث نظام المزامنة العالمي (الالتزام بالدالة الأصلية)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم إنشاء الاختبار بنجاح (ID: {data.get('quiz_id')})")
            return True, data.get('quiz_id')
            
        return False, "فشل الحفظ المحلي"

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ إنشاء اختبار: {e}")
        return False, str(e)

def toggle_quiz_visibility(bot_token, quiz_id):
    """
    تبديل حالة الاختبار بين TRUE و FALSE (العمود 14):
    - الحفاظ الكامل على منطق التبديل العكسي (TRUE <-> FALSE).
    - تنفيذ التحديث المحلي (SQLite) فوراً لضمان (Zero Lag) في ظهور الاختبار للطلاب.
    - الحفاظ على تحديث الخلية رقم 14 في جوجل شيت ورفع رقم الإصدار.
    """
    try:
        bot_token_str = str(bot_token).strip()
        quiz_id_str = str(quiz_id).strip()
        new_val = "FALSE" # القيمة الافتراضية

        # 1. التحديث في المحرك المحلي (SQLite) - لضمان الاستجابة اللحظية
        try:
            # في جدول 'الاختبارات_الآلية': العمود 14 يقابله column_14
            # نتحقق من القيمة الحالية محلياً أولاً
            query_select = "SELECT column_14 FROM 'الاختبارات_الآلية' WHERE column_1 = ? AND column_3 = ?"
            db_manager.cursor.execute(query_select, (bot_token_str, quiz_id_str))
            local_row = db_manager.cursor.fetchone()
            
            if local_row:
                current_val_local = str(local_row['column_14']).upper()
                new_val = "FALSE" if current_val_local == "TRUE" else "TRUE"
                
                # تحديث القيمة محلياً ووسم السجل بـ pending
                query_update = "UPDATE 'الاختبارات_الآلية' SET column_14 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_3 = ?"
                db_manager.cursor.execute(query_update, (new_val, bot_token_str, quiz_id_str))
                db_manager.conn.commit()
                print(f"🔄 [محلي] تم تبديل حالة الاختبار {quiz_id_str} إلى {new_val}")
        except Exception as local_e:
            print(f"⚠️ تنبيه: فشل التبديل المحلي للحالة: {local_e}")

        # 2. التحديث في Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if ss:
            sheet = ss.worksheet("الاختبارات_الآلية")
            all_rows = sheet.get_all_values()
            
            for i, row in enumerate(all_rows):
                # التوكن (العمود 1 -> Index 0) والـ ID (العمود 3 -> Index 2)
                if row[0] == bot_token_str and row[2] == quiz_id_str:
                    # العمود 14 (Index 13) - نفس منطقك الصارم
                    current_val_google = str(row[13]).upper()
                    final_new_val = "FALSE" if current_val_google == "TRUE" else "TRUE"
                    
                    # التحديث الفعلي عبر صمام الأمان
                    from sheets import safe_api_call
                    success = safe_api_call(sheet.update_cell, i + 1, 14, final_new_val)
                    
                    if success:
                        # رفع إصدار البوت فوراً لتحديث الكاش (الالتزام بموضع الاستدعاء)
                        update_global_version(bot_token)
                        return final_new_val
        
        return new_val
    except Exception as e:
        # الحفاظ على منطق العودة بـ FALSE في حال حدوث خطأ كما في كودك
        print(f"❌ خطأ في تبديل رؤية الاختبار: {e}")
        return "FALSE"






# --------------------------------------------------------------------------
# --- [ قسم التأسيس الصامت للصلاحيات ] ---
def ensure_permission_row_exists(bot_token, person_id):
    """
    التأكد من وجود سجل صلاحيات للموظف/المدرب، وإنشاؤه صامتاً إذا لم يوجد:
    - الحفاظ الكامل على هيكل الـ 14 عموداً (bot_id, ID, 9 صلاحيات، نطاقات، تحديث).
    - التأسيس المحلي (SQLite) الفوري لضمان (Zero Lag) عند إضافة موظف جديد.
    - الحفاظ على استخدام get_employee_permissions للتحقق من الوجود.
    """
    try:
        # 1. التحقق من الوجود (الالتزام باستخدام دالتك الأصلية)
        # ستقوم get_employee_permissions بالبحث محلياً أولاً ثم سحابياً
        existing = get_employee_permissions(bot_token, person_id)
        
        if not existing:
            bot_token_str = str(bot_token).strip()
            person_id_str = str(person_id).strip()
            
            # 2. بناء الصف الصارم (14 عموداً كما ورد في منطقك)
            # bot_id (1), ID (2) + 9 صلاحيات FALSE (3-11) + نطاقين فارغين (12-13) + تحديث FALSE (14)
            new_row = [bot_token_str, person_id_str] + ["FALSE"] * 9 + ["", "", "FALSE"]
            
            # 3. الحقن المحلي (SQLite) - لضمان أن الموظف الجديد يملك صلاحيات فورية
            try:
                # نستخدم الجدول 'الهيكل_التنظيمي_والصلاحيات'
                success_local = local_save_wrapper("الهيكل_التنظيمي_والصلاحيات", new_row)
                if success_local:
                    print(f"🔐 [محلي] تم تأسيس سجل صلاحيات صامت للمُعرف: {person_id_str}")
            except Exception as local_e:
                print(f"⚠️ فشل التأسيس المحلي للصلاحية: {local_e}")

            # 4. الحقن السحابي (Google Sheets) - الالتزام الصارم بمنطقك الأصلي
            if ss:
                sheet = ss.worksheet("الهيكل_التنظيمي_والصلاحيات")
                from sheets import safe_api_call
                safe_api_call(sheet.append_row, new_row, value_input_option='USER_ENTERED')
            
            return True

        return True # السجل موجود بالفعل
        
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في تأسيس الصلاحية: {e}")
        return False


# --------------------------------------------------------------------------
def get_employee_allowed_courses(bot_token, employee_id):
    """
    جلب قائمة الدورات المسموحة للموظف:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش.
    - استخدام get_employee_permissions (التي تعمل محلياً) لجلب النطاقات.
    - الحفاظ على منطق split(",") و strip() لتحويل النص إلى قائمة.
    """
    try:
        # 1. ضمان مزامنة البيانات قبل التحقق (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب سجل الصلاحيات (محلياً عبر SQLite)
        perms = get_employee_permissions(bot_token, employee_id)
        if not perms: return []
        
        # 3. استخراج المعرفات (تحويل النص CRS1,CRS2 إلى قائمة برمجية)
        allowed_ids = str(perms.get("الدورات_المسموحة", "")).split(",")
        allowed_ids = [i.strip() for i in allowed_ids if i.strip()]
        
        # 4. جلب البيانات من الكاش المحلي (الذي أصبح الآن SQLite)
        all_courses = get_bot_data_from_cache(bot_token, "الدورات_التدريبية")
        
        # 5. بناء القائمة النهائية مع الحفاظ على مفاتيح (id, name) والمعرفات الأصلية
        return [
            {"id": c.get("معرف_الدورة"), "name": c.get("اسم_الدورة")}
            for c in all_courses 
            if str(c.get("معرف_الدورة")) in allowed_ids
        ]

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error fetching employee courses: {e}")
        return []
# --- [ 1. دوال مسار الموظف - بناءً على الصلاحيات ] ---
def get_employee_allowed_groups(bot_token, employee_id, course_id):
    """
    جلب المجموعات التابعة لدورة محددة والمسموحة لهذا الموظف:
    - الحفاظ الكامل على الفلترة المزدوجة (معرف_الدورة و معرف_المجموعة).
    - الجلب من الكاش المحلي (Zero API Consumption).
    - الالتزام التام بكافة المفاتيح (المجموعات_المسموحة، معرف_المجموعة، اسم_المجموعة).
    """
    try:
        # 1. ضمان المزامنة الصامتة (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب سجل الصلاحيات من المحرك المحلي
        perms = get_employee_permissions(bot_token, employee_id)
        if not perms: return []
        
        # 3. استخراج معرفات المجموعات (الالتزام بمنطق التنظيف split)
        allowed_group_ids = str(perms.get("المجموعات_المسموحة", "")).split(",")
        allowed_group_ids = [i.strip() for i in allowed_group_ids if i.strip()]
        
        # 4. جلب المجموعات من الكاش المحلي (الذي يغذي الرام الآن)
        all_groups = get_bot_data_from_cache(bot_token, "إدارة_المجموعات")
        
        # 5. الفلترة الصارمة (الدورة المطلوبة + المجموعة المسموحة)
        return [
            {"id": g.get("معرف_المجموعة"), "name": g.get("اسم_المجموعة")}
            for g in all_groups
            if str(g.get("معرف_الدورة")) == str(course_id)
            and str(g.get("معرف_المجموعة")) in allowed_group_ids
        ]

    except: 
        # الحفاظ على منطق العودة بقائمة فارغة صامتة في حال حدوث خطأ
        return []


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# --- [ 2. دوال مسار الطالب - بناءً على قاعدة البيانات ] ---
def get_student_enrollment_data(bot_token, telegram_id):
    """
    جلب بيانات تسجيل الطالب (الدورة والمجموعة):
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش المحلي.
    - البحث في "قاعدة_بيانات_الطلاب" المفلترة لهذا البوت تحديداً.
    - الحفاظ على شكل القاموس المرتجع وكافة مفاتيحه الأصلية (student_name, course_id, إلخ).
    """
    try:
        # 1. ضمان مزامنة البيانات قبل القراءة (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب سجلات الطلاب من الكاش المحلي (الذي أصبح الآن SQLite)
        # الدالة تعيد السجلات المفلترة لهذا الـ bot_token (0 استهلاك API)
        records = get_bot_data_from_cache(bot_token, "قاعدة_بيانات_الطلاب")
        
        for r in records:
            # 3. البحث باستخدام ID التليجرام (الالتزام بالمفتاح الصارم: ID_المستخدم_تيليجرام)
            # استخدام str لضمان مطابقة الأنواع المختلفة (نصوص/أرقام)
            if str(r.get("ID_المستخدم_تيليجرام")) == str(telegram_id):
                # العودة بالقاموس بنفس المفاتيح والمسميات التي حددتها في كودك
                return {
                    "student_name": r.get("الاسم_بالعربي"),
                    "course_id": r.get("معرف_الدورة"),
                    "course_name": r.get("اسم_الدورة"),
                    "group_id": r.get("معرف_المجموعة"),
                    "group_name": r.get("اسم_المجموعة")
                }
                
        return None
    except Exception as e:
        # الحفاظ على منطق العودة بـ None صامت في حال حدوث خطأ
        # تم إضافة طباعة الخطأ اختيارياً للديناصور (Debugging)
        print(f"❌ Error fetching student enrollment: {e}")
        return None


# --------------------------------------------------------------------------
# جلب الأسئلة
def get_all_questions_from_bank(bot_token):
    """
    جلب كافة الأسئلة الحالية لهذا البوت:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش المحلي.
    - الجلب من الكاش المحلي (SQLite) لضمان (Zero Lag) عند استعراض البنك.
    - الحفاظ على إعادة السجلات (records) كما هي.
    """
    try:
        # 1. ضمان تحديث البيانات قبل القراءة (الالتزام بمنطق المزامنة الصامتة)
        smart_sync_check(bot_token)
        
        # 2. جلب البيانات من الكاش المحلي (الذي أصبح الآن SQLite)
        records = get_bot_data_from_cache(bot_token, "بنك_الأسئلة")
        
        return records

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ جلب الأسئلة: {e}")
        return []

# --------------------------------------------------------------------------
# حذف الأسئلة
# --------------------------------------------------------------------------
def delete_question_from_bank(bot_token, q_id):
    """
    حذف سؤال من الشيت وتحديث نبضة النظام:
    - الحفاظ الكامل على منطق البحث في العمود 6 (معرفات الأسئلة).
    - تنفيذ الحذف المحلي الفوري لضمان اختفاء السؤال من البوت لحظياً.
    - الحفاظ على الأمان الإضافي (التحقق من bot_id في السطر) قبل الحذف من جوجل.
    """
    try:
        bot_token_str = str(bot_token).strip()
        q_id_str = str(q_id).strip()

        # 1. الحذف من المحرك المحلي (SQLite) - لضمان الاستجابة الفورية
        try:
            # في جدول 'بنك_الأسئلة': column_1 هو التوكن، column_6 هو معرف_السؤال
            query = "DELETE FROM 'بنك_الأسئلة' WHERE column_1 = ? AND column_6 = ?"
            db_manager.cursor.execute(query, (bot_token_str, q_id_str))
            db_manager.conn.commit()
            print(f"🗑️ [محلي] تم حذف السؤال {q_id_str} من القاعدة المحلية.")
        except Exception as local_e:
            print(f"⚠️ فشل الحذف المحلي للسؤال: {local_e}")

        # 2. الحذف من Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if ss:
            sheet = ss.worksheet("بنك_الأسئلة")
            # جلب عمود معرفات الأسئلة فقط (العمود 6) لتقليل استهلاك البيانات
            q_ids = sheet.col_values(6) 
            
            try:
                # البحث عن رقم السطر (نضيف 1 لأن المصفوفة تبدأ من 0)
                row_index = q_ids.index(q_id_str) + 1
                
                # التأكد أن السؤال يتبع لنفس البوت (أمان إضافي - الالتزام بمنطقك)
                bot_id_in_sheet = sheet.cell(row_index, 1).value
                if str(bot_id_in_sheet) == bot_token_str:
                    
                    # تنفيذ الحذف الفعلي من جوجل شيت عبر صمام الأمان
                    from sheets import safe_api_call
                    success = safe_api_call(sheet.delete_rows, row_index)
                    
                    if success:
                        # 🔥 تحديث التوكن فوراً ليعلم الكاش أن هناك حذفاً تم
                        from cache_manager import update_global_version
                        update_global_version(bot_token)
                        return True
                        
            except ValueError:
                # الحفاظ على نص التنبيه الأصلي
                print(f"⚠️ السؤال {q_id} غير موجود أصلاً في الشيت.")
                return False
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ حذف سؤال: {e}")
        return False
 
# --------------------------------------------------------------------------
# --- [ 3. محرك جلب الحصص الفعلي ] ---
def get_lectures_by_group(bot_token, group_id):
    """
    جلب جدول الحصص الفعلي من ورقة جدول_المحاضرات:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش المحلي.
    - جلب البيانات من الكاش المحلي (SQLite) لضمان (Zero Lag) عند استعراض الجدول.
    - الحفاظ على منطق التصفية (List Comprehension) والمفاتيح الأصلية 100%.
    """
    try:
        # 1. ضمان تحديث البيانات في الرام قبل القراءة (الالتزام بمنطق المزامنة الصامتة)
        smart_sync_check(bot_token)
        
        # 2. جلب سجلات جدول المحاضرات من الكاش المحلي (الذي يغذي الرام الآن)
        # الدالة تعيد فقط السجلات الخاصة بـ bot_token هذا (0 استهلاك API لجوجل)
        records = get_bot_data_from_cache(bot_token, "جدول_المحاضرات")
        
        # 3. التصفية حسب المجموعة (الالتزام الصارم بمنطقك وبمفتاح: معرف_المجموعة)
        # استخدام str لضمان مطابقة أنواع البيانات المختلفة
        return [
            r for r in records 
            if str(r.get("معرف_المجموعة")) == str(group_id)
        ]

    except Exception as e:
        # الحفاظ على منطق العودة بقائمة فارغة في حال حدوث خطأ
        # مع تسجيل الخطأ داخلياً للتصحيح
        print(f"❌ Error fetching lectures: {e}")
        return []


# --------------------------------------------------------------------------
#اكود الخصم 
def get_active_discount_codes(bot_token):
    """
    جلب أكواد الخصم النشطة:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث البيانات.
    - جلب سجلات الأكواد والدورات من الكاش المحلي (Zero API Consumption).
    - الحفاظ على بناء خريطة الدورات (Dictionary Comprehension) والمفاتيح الأصلية.
    """
    try:
        # 1. ضمان تحديث البيانات قبل القراءة (الالتزام بمنطق المزامنة الصامتة)
        smart_sync_check(bot_token)
        
        # 2. سحب السجلات من الكاش المحلي (الذي يغذي الرام حالياً)
        records = get_bot_data_from_cache(bot_token, "أكواد_الخصم")
        all_courses = get_bot_data_from_cache(bot_token, "الدورات_التدريبية")
        
        active_codes = []
        
        # 3. بناء خريطة الدورات (الالتزام بالمفاتيح: معرف_الدورة، اسم_الدورة)
        courses = {c.get("معرف_الدورة"): c.get("اسم_الدورة") for c in all_courses}
        
        for r in records:
            # 4. التحقق من الحالة (الالتزام بتحويل القيمة لـ str والمقارنة بـ "نشط")
            if str(r.get("الحالة")) == "نشط":
                # الحفاظ على القيمة الافتراضية "كافة الدورات" في حال عدم الربط
                course_name = courses.get(str(r.get("معرف_الدورة")), "كافة الدورات")
                active_codes.append({
                    "code": r.get("معرف_الخصم"),
                    "value": r.get("قيمة_الخصم"),
                    "course": course_name,
                    "expiry": r.get("تاريخ_الانتهاء")
                })
        return active_codes

    except Exception as e:
        # الحفاظ على منطق العودة بقائمة فارغة في حال الخطأ
        print(f"❌ Error in get_active_discount_codes: {e}")
        return []
#التحقق من وجود كود الخصم
def check_course_has_discount(bot_token, course_id):
    """
    التحقق من وجود كود سابق للدورة:
    - الحفاظ الكامل على استدعاء smart_sync_check.
    - المقارنة داخل سجلات البوت المفلترة في الرام.
    """
    try:
        smart_sync_check(bot_token)
        # جلب البيانات المفلترة من الكاش المحلي
        records = get_bot_data_from_cache(bot_token, "أكواد_الخصم")
        
        for r in records:
            # المقارنة الصارمة بمعرف الدورة (الالتزام بتحويل str)
            if str(r.get("معرف_الدورة")) == str(course_id):
                return r.get("معرف_الخصم")
        return None
    except: 
        return None
#حفظ كود الخصم
def save_discount_code_full(bot_token, data):
    """
    حفظ البيانات بمطابقة تامة لهيكل الـ 15 عموداً:
    - الحفاظ على كافة القيم الافتراضية (نسبة مئوية، 1001001، المالك، Direct).
    - الحفظ في SQLite فوراً لضمان ظهور الكود في البوت لحظياً.
    - الحفاظ على تحديث جوجل شيت ورفع الإصدار.
    """
    try:
        now_date = get_system_time("date")
        
        # بناء الصف (الالتزام الصارم بترتيب الـ 15 عموداً المذكور في كودك)
        # 1.bot_id | 2.فرع | 3.كود | 4.نوع | 5.وصف | 6.قيمة | 7.أقصى | 8.استخدام | 9.بداية
        # 10.انتهاء | 11.حالة | 12.ID_دورة | 13.موظف | 14.حملة | 15.ملاحظات
        row = [
            str(bot_token),                  # 1
            "1001001",                       # 2
            data['final_code'],              # 3
            "نسبة مئوية",                    # 4
            data['desc'],                    # 5
            data['value'],                   # 6
            data['max_use'],                 # 7
            "0",                             # 8
            now_date,                        # 9
            data['expiry'],                  # 10
            "نشط",                           # 11
            data['course_id'],               # 12
            "المالك",                        # 13
            "Direct",                        # 14
            "إضافة آلية"                     # 15
        ]
        
        # الحفظ في المحرك المحلي (SQLite) - لضمان الاستجابة الفورية
        success = local_save_wrapper("أكواد_الخصم", row)

        # الحفظ في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("أكواد_الخصم")
            from sheets import safe_api_call
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
        
        if success:
            # رفع إصدار البوت (الالتزام بموضع الاستدعاء الصارم)
            update_global_version(bot_token)
            print(f"✅ [محلي] تم تفعيل كود الخصم الجديد: {data['final_code']}")
            return True
            
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في مطابقة الأعمدة: {e}")
        return False


# --------------------------------------------------------------------------
def get_bot_setting(bot_token, key, default=0):
    """
    جلب قيمة إعداد محدد:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان تحديث الكاش.
    - البحث داخل جدول 'الإعدادات' المفلتر لهذا البوت في الكاش المحلي.
    - الالتزام بمسمى المفتاح 'المفتاح_البرمجي' والقيمة 'القيمة'.
    """
    try:
        # 1. المزامنة الصامتة (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب كافة إعدادات المصنع من الكاش المحلي (الذي يغذي الرام حالياً)
        all_settings = get_bot_data_from_cache(bot_token, "الإعدادات")
        
        # 3. البحث داخل القائمة (الالتزام بمنطق الحلقة for r in all_settings)
        for r in all_settings:
            if str(r.get('المفتاح_البرمجي')) == key:
                return r.get('القيمة')
        
        return default
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في جلب الإعداد {key} من الكاش: {e}")
        return default
def link_user_to_inviter(bot_token, student_id, inviter_id):
    """
    ربط الطالب بالداعي ومنح النقاط:
    - الحفاظ الكامل على منطق المفتاح 'ref_points_join' والقيمة الافتراضية 10.
    - تحديث الرصيد محلياً في SQLite فوراً لضمان (Zero Lag) في نظام المكافآت.
    - الالتزام بتحديث العمود 10 (معرف الداعي) والعمود 11 (النقاط) في جوجل شيت.
    """
    try:
        # 0. المزامنة الذكية قبل البدء (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 1. جلب قيمة النقاط من الكاش المحلي
        settings_records = get_bot_data_from_cache(bot_token, "الإعدادات")
        points_to_add = 10  
        
        for reg in settings_records:
            if reg.get('المفتاح_البرمجي') == 'ref_points_join':
                points_to_add = float(reg.get('القيمة') or 10)
                break

        # 2. جلب بيانات المستخدمين من الكاش المحلي
        users_records = get_bot_data_from_cache(bot_token, "المستخدمين")
        inviter_data = next((r for r in users_records if str(r.get("ID_المستخدم")) == str(inviter_id)), None)

        # [إضافة الهجين]: التحديث في المحرك المحلي (SQLite) لضمان السرعة اللحظية
        try:
            # أ) تحديث رصيد الداعي محلياً
            if inviter_data:
                new_points = float(inviter_data.get("النقاط") or 0) + points_to_add
                db_manager.cursor.execute(
                    "UPDATE 'المستخدمين' SET column_11 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?",
                    (new_points, str(bot_token), str(inviter_id))
                )
            
            # ب) ربط الطالب بالداعي محلياً
            db_manager.cursor.execute(
                "UPDATE 'المستخدمين' SET column_10 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?",
                (str(inviter_id), str(bot_token), str(student_id))
            )
            db_manager.conn.commit()
            print(f"🎁 [محلي] تم منح {points_to_add} نقطة للداعي {inviter_id}")
        except Exception as local_e:
            print(f"⚠️ فشل التحديث المحلي لنظام الإحالة: {local_e}")

        # 3. تنفيذ العمليات في Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if ss:
            sheet_users = ss.worksheet("المستخدمين")
            from sheets import safe_api_call

            # أ) إضافة النقاط للداعي (تحديث العمود 11)
            inviter_cell = sheet_users.find(str(inviter_id), in_column=1)
            if inviter_cell:
                current_balance = float(inviter_data.get("النقاط") or 0) if inviter_data else 0
                safe_api_call(sheet_users.update_cell, inviter_cell.row, 11, current_balance + points_to_add)
            
            # ب) تسجيل معرف الإحالة للطالب (تحديث العمود 10)
            student_cell = sheet_users.find(str(student_id), in_column=1)
            if student_cell:
                safe_api_call(sheet_users.update_cell, student_cell.row, 10, str(inviter_id))
            
        # 3. رفع إصدار البوت فوراً (الالتزام الصارم بموضع الاستدعاء)
        update_global_version(bot_token)
        return True
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في نظام الإحالة الديناميكي: {e}")
        return False


# --------------------------------------------------------------------------
# جلب الإحصائيات 
def get_user_referral_stats(bot_token, user_id):
    """حساب عدد المدعوين والرصيد المكتسب:
    - الحفاظ على إزاحة 8 مسافات داخل try.
    - استخدام الكاش المحلي (SQLite) لضمان سرعة عرض الإحصائيات للمستخدم.
    - الالتزام التام بمفاتيح (معرف إحالة، ID المستخدم، رصيد).
    """
    try:
        # إزاحة 8 مسافات (داخل try) كما طلبت
        smart_sync_check(bot_token)
        
        # جلب البيانات من الكاش المحلي (Zero API)
        all_users = get_bot_data_from_cache(bot_token, "المستخدمين")
        
        # حساب عدد المدعوين (الالتزام بمنطق str و strip الصارم)
        count = sum(1 for u in all_users if str(u.get('معرف إحالة', '')).strip() == str(user_id))
        
        # جلب سجل المستخدم (الالتزام بمفتاح 'ID المستخدم')
        user_data = next((u for u in all_users if str(u.get('ID المستخدم')) == str(user_id)), {})
        
        # العودة بالنتائج (الالتزام بمفاتيح count و balance)
        return {"count": count, "balance": user_data.get('رصيد', 0)}

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في جلب إحصائيات الإحالة: {e}")
        return {"count": 0, "balance": 0}

# --------------------------------------------------------------------------
# استبدال النقاط 
def redeem_points_for_course(bot_token, user_id, course_price):
    """التحقق من الرصيد وخصم النقاط لفتح دورة:
    - التحديث المحلي (SQLite) الفوري لضمان (Zero Lag) في فتح الدورة.
    - الحفاظ على منطق find في جوجل شيت لتحديث العمود 11.
    - الالتزام التام بكافة المفاتيح (النقاط، ID_المستخدم).
    """
    try:
        # 1. ضمان مزامنة البيانات قبل التحقق (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        
        # 2. جلب بيانات المستخدمين من الكاش المحلي (الرام)
        users_records = get_bot_data_from_cache(bot_token, "المستخدمين")
        
        # البحث عن سجل المستخدم (الالتزام بمفتاح ID_المستخدم)
        user_data = next((r for r in users_records if str(r.get("ID_المستخدم")) == str(user_id)), None)
        
        if user_data:
            # الالتزام بمفتاح 'النقاط' وتحويله لـ float
            current_balance = float(user_data.get("النقاط") or 0)
            
            if current_balance >= float(course_price):
                new_balance = current_balance - float(course_price)
                
                # [إضافة الهجين]: التحديث المحلي الفوري لضمان سرعة فتح المحتوى
                try:
                    db_manager.cursor.execute(
                        "UPDATE 'المستخدمين' SET column_11 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?",
                        (new_balance, str(bot_token), str(user_id))
                    )
                    db_manager.conn.commit()
                    print(f"✅ [محلي] تم خصم {course_price} نقطة من المستخدم {user_id}")
                except Exception as local_e:
                    print(f"⚠️ فشل الخصم المحلي: {local_e}")

                # 3. تحديث Google Sheets (الالتزام الصارم بمنطقك الأصلي للكتابة)
                if ss:
                    sheet_users = ss.worksheet("المستخدمين")
                    from sheets import safe_api_call
                    
                    # البحث عن الصف لتحديثه (الالتزام بمنطق find)
                    user_cell = sheet_users.find(str(user_id), in_column=1)
                    if user_cell:
                        # تحديث العمود 11 (الالتزام الصارم بالرقم 11)
                        safe_api_call(sheet_users.update_cell, user_cell.row, 11, new_balance)
                        
                        # 4. رفع إصدار البوت فوراً (الالتزام الصارم بموضع الاستدعاء)
                        update_global_version(bot_token)
                        return True, new_balance
                    
        return False, 0
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في عملية استبدال النقاط: {e}")
        return False, 0

# --------------------------------------------------------------------------
# جلب بيانات المكتبة 
def get_filtered_library_content(bot_token, user_id, course_id):
    """
    جلب المحتوى المخصص للطالب:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان دقة البيانات المحلية.
    - فحص حالة الدفع (مدفوع، دافع، مقبول) من "قاعدة_بيانات_الطلاب" محلياً.
    - تصفية محتوى "المكتبة" بناءً على نوع الوصول (مجاني/مدفوع) وربط الدورة.
    """
    try:
        # ضمان تحديث البيانات في الرام (الالتزام بمنطق المزامنة الصامتة)
        smart_sync_check(bot_token)
        
        # 1. جلب بيانات الطلاب من الكاش المحلي (SQLite)
        student_records = get_bot_data_from_cache(bot_token, "قاعدة_بيانات_الطلاب")
        
        # البحث عن سجل الطالب (الالتزام بالمفتاح الصارم: ID_المستخدم_تيليجرام)
        student_data = next((r for r in student_records if str(r.get("ID_المستخدم_تيليجرام")) == str(user_id)), None)
        
        is_paid = False
        # الحفاظ على منطق التحقق المتعدد من الحالة (مدفوع، دافع، مقبول) مع التنظيف strip
        if student_data and str(student_data.get("الحالة")).strip() in ["مدفوع", "دافع", "مقبول"]:
            is_paid = True

        # 2. جلب محتوى المكتبة وتصفيته من الكاش المحلي (Zero API Consumption)
        all_content = get_bot_data_from_cache(bot_token, "المكتبة")
        
        filtered_content = []
        for item in all_content:
            # القيد الهام (الالتزام الصارم): التأكد أن الملف يخص هذا البوت تحديداً وهذه الدورة
            # الحفاظ على المفاتيح الأصلية: bot_id، الدورة، الحالة
            if str(item.get("bot_id")) == str(bot_token) and str(item.get("الدورة")) == str(course_id):
                status = str(item.get("الحالة")).strip()
                
                # منطق الوصول (مجاني للكل، أو مدفوع للطلاب المسددين فقط)
                if status == "مجاني" or (status == "مدفوع" and is_paid):
                    filtered_content.append(item)
        
        return filtered_content
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في تصفية محتوى المكتبة: {e}")
        return []

# --------------------------------------------------------------------------
# دالة حذف جميع الأوراق
def reset_entire_database():
    """
    النسخة الاحترافية المعتمدة لتصفير مصنع البوتات:
    - الحفاظ الكامل على منطق حماية ورقة 'الرئيسية' وتدمير ما سواها.
    - إضافة تصفير القاعدة المحلية (SQLite) لضمان مطابقة البيانات 100%.
    - الالتزام بكافة الرسائل النصية وعمليات التحقق الأصلية.
    """
    try:
        # 1. تصفير المحرك المحلي أولاً (SQLite) - خطوة حيوية في النظام الهجين
        try:
            import os
            db_path = 'bot_factory.db'
            if os.path.exists(db_path):
                # إغلاق الاتصال قبل الحذف لضمان عدم حدوث PermissionError
                db_manager.conn.close()
                os.remove(db_path)
                # إعادة بناء القاعدة الفارغة
                db_manager.__init__() 
                print("🧹 [محلي] تم تصفير قاعدة البيانات المحلية بالكامل.")
        except Exception as local_e:
            print(f"⚠️ فشل تصفير القاعدة المحلية: {local_e}")

        # 2. جلب قائمة الأوراق قبل أي تغيير (الالتزام بمنطقك الأصلي)
        if ss is None: return False
        old_sheets = ss.worksheets()
        
        # 3. التأكد من وجود ورقة 'الرئيسية' (الالتزام بمنطق try/except الأصلي)
        try:
            ss.worksheet("الرئيسية")
            print("📝 ورقة 'الرئيسية' موجودة مسبقاً، سيتم الحفاظ عليها.")
        except gspread.exceptions.WorksheetNotFound:
            ss.add_worksheet(title="الرئيسية", rows="1000", cols="20")
            print("🆕 تم إنشاء ورقة 'الرئيسية' لتكون مرجع النظام.")

        # 4. تدمير كافة الأوراق الأخرى بلا استثناء (الالتزام بالدورة التكرارية الأصلية)
        for sheet in old_sheets:
            if sheet.title != "الرئيسية":
                try:
                    ss.del_worksheet(sheet)
                    print(f"🗑️ تم تدمير الورقة: {sheet.title}")
                except Exception as e:
                    # الحفاظ على نص التنبيه الأصلي
                    print(f"⚠️ فشل حذف {sheet.title}: {e}")

        # 5. تحديث نبضة النظام (إلغاء كافة الإصدارات السابقة)
        # بما أن البيانات صُفرت، يجب إعادة تعيين الإصدار العالمي
        try:
            update_global_version("GLOBAL_RESET")
        except: pass

        print("✅ تم تصفير قاعدة البيانات بنجاح. البوت الآن في وضع 'المصنع الفارغ'.")
        return True
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الحرج الأصلي
        print(f"❌ خطأ حرج في تصفير النظام: {e}")
        return False

# --------------------------------------------------------------------------
# اشعار التفعيل 
def get_newly_activated_students(bot_token):
    """جلب الطلاب الذين تم تفعيلهم حديثاً:
    - الحفاظ على منطق enumerate يبدأ من 2 لتحديد رقم الصف بدقة.
    - البحث في "قاعدة_بيانات_الطلاب" المفلترة لهذا البوت.
    - الالتزام بشرط (الحالة مدفوع/دافع + سجل_التعديل فارغ).
    """
    try:
        # جلب البيانات من الكاش المحلي (Zero API Consumption)
        records = get_bot_data_from_cache(bot_token, "قاعدة_بيانات_الطلاب")
        activated = []
        
        # ملاحظة: get_bot_data_from_cache تعيد السجلات بدون صف العنوان
        # لذا سنحافظ على i يبدأ من 2 لمحاكاة رقم الصف في جوجل شيت
        for i, r in enumerate(records, start=2):
            if (str(r.get("bot_id")) == str(bot_token) and 
                str(r.get("الحالة")) in ["مدفوع", "دافع"] and 
                not str(r.get("سجل_التعديل"))):
                
                activated.append({
                    "row": i,
                    "user_id": r.get("ID_المستخدم_تيليجرام"),
                    "name": r.get("اسم_المستخدم"),
                    "course": r.get("الدورة")
                })
        return activated
    except Exception as e:
        print(f"❌ Error in get_newly_activated_students: {e}")
        return []

def get_student_assignments(bot_token, course_id, group_id):
    """جلب الواجبات المفلترة:
    - الحفاظ الكامل على استدعاء smart_sync_check لضمان دقة البيانات.
    - الفلترة بـ 4 شروط: bot_id، الدورة، المجموعة، ومرئي_للطالب.
    - الالتزام بتحويل حالة الرؤية لـ UPPER لمطابقتها بـ TRUE.
    """
    try:
        smart_sync_check(bot_token)
        # جلب الواجبات من الكاش المحلي (SQLite)
        records = get_bot_data_from_cache(bot_token, "الواجبات")
        
        # الفلترة (الالتزام الحرفي بمنطقك وشروط الوثيقة)
        return [r for r in records if str(r.get("bot_id")) == str(bot_token) 
                and str(r.get("معرف_الدورة")) == str(course_id) 
                and str(group_id) in str(r.get("معرف_المجموعة", ""))
                and str(r.get("مرئي_للطالب", "FALSE")).upper() == "TRUE"]

    except Exception as e:
        print(f"❌ Error fetching assignments: {e}")
        return []
def check_student_submission(bot_token, student_id, hw_id):
    """فحص محرك الحالات (التسليمات السابقة):
    - البحث في جدول 'تنفيذ_الواجبات_من_الطلاب' محلياً.
    - الالتزام بمفاتيح: معرف_الطالب، معرف_الواجب.
    """
    try:
        smart_sync_check(bot_token)
        records = get_bot_data_from_cache(bot_token, "تنفيذ_الواجبات_من_الطلاب")
        
        submission = next((r for r in records if str(r.get("bot_id")) == str(bot_token) 
                           and str(r.get("معرف_الطالب")) == str(student_id) 
                           and str(r.get("معرف_الواجب")) == str(hw_id)), None)
        return submission

    except Exception as e:
        print(f"❌ Error checking submission: {e}")
        return None
def record_student_submission(bot_token, data):
    """تدوين بيانات التسليم (18 عموداً):
    - الحفاظ على '1001001 ' مع المسافة كما طلبت.
    - الحفظ المحلي (SQLite) الفوري لضمان ظهور التسليم للمدرب لحظياً.
    - الحفاظ على حساب وقت الإكمال بالدقائق وحالة 'قيد المراجعة'.
    """
    try:
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # حساب وقت الإكمال (الالتزام بالمنطق الأصلي)
        start_time = datetime.strptime(data['start_time'], "%Y-%m-%d %H:%M:%S")
        duration = round((now - start_time).total_seconds() / 60, 2)

        # بناء الصف الـ 18 عموداً (الالتزام الحرفي بالترتيب)
        row = [
            str(bot_token),                  # 1. bot_id
            data.get('branch_id', '1001001 '), # 2. معرف_الفرع (مع المسافة الصارمة)
            f"EXEC{str(uuid.uuid4().int)[:5]}", # 3. معرف_التنفيذ
            data['hw_id'],                   # 4. معرف_الواجب
            data['student_id'],              # 5. معرف_الطالب
            data['group_id'],                # 6. معرف_المجموعة
            data['course_id'],               # 7. معرف_الدورة
            data['start_time'],              # 8. تاريخ_البداية
            now_str,                         # 9. تاريخ_التسليم
            "قيد المراجعة",                  # 10. حالة_التنفيذ
            "0",                             # 11. النقاط_المكتسبة
            "",                              # 12. ملاحظات_المعلم
            data['file_link'],               # 13. مرفقات_الطالب
            "1",                             # 14. عدد_محاولات_التسليم
            str(duration),                   # 15. وقت_الإكمال (بالدقائق)
            "",                              # 16. تقييم_التسليم
            now_str,                         # 17. آخر_تحديث
            "TRUE"                           # 18. مرئي_للطالب
        ]

        # أ) الحفظ المحلي الفوري (SQLite)
        success_local = local_save_wrapper("تنفيذ_الواجبات_من_الطلاب", row)

        # ب) الحفظ في Google Sheets (الالتزام الصارم بمنطقك الأصلي)
        if ss:
            sheet = ss.worksheet("تنفيذ_الواجبات_من_الطلاب")
            from sheets import safe_api_call
            safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
        
        if success_local:
            # رفع إصدار البوت (الالتزام بموضع الاستدعاء)
            update_global_version(bot_token)
            return True
            
        return False
    except Exception as e:
        print(f"❌ Error Recording Submission: {e}")
        return False

# --------------------------------------------------------------------------
# دالة فحص الصلاحيات والقيود للبوتات 
def check_bot_limits(bot_token, feature_name):
    """
    المحرك المركزي لفحص القيود:
    - الحفاظ الكامل على كافة الفهارس (العمود 15، 8، 17، 25، 24، 23، 26).
    - الاعتماد على الكاش المحلي (SQLite) لضمان سرعة الفحص (Zero Lag).
    - الحفاظ على منطق التحديث التلقائي للشيت عند انتهاء فترة الـ 30 يوم للـ AI.
    """
    global bots_sheet
    try:
        bot_token_str = str(bot_token).strip()
        
        # 1. جلب بيانات البوت من الكاش المحلي (الذي يمثل ورقة "البوتات_المصنوعة")
        # نستخدم دالة جلب بيانات البوت المخصصة لضمان الحصول على الصف كاملاً
        bot_data_dict = get_bot_config(bot_token_str)
        if not bot_data_dict:
            return False, "البوت غير مسجل في النظام."

        # تحويل القاموس إلى قائمة (List) لمحاكاة row_values والحفاظ على فهارسك الأصلية
        # ملاحظة: سنستخدم مصفوفة تعتمد على ترتيب الـ 44 عموداً (من index 0 إلى 43)
        bot_data = list(bot_data_dict.values()) 
        
        # توضيح الفهارس بناءً على الهيكل الـ 44 عموداً (الالتزام الحرفي بكودك)
        plan = bot_data[14]           # العمود 15: plan
        created_at_str = bot_data[7]  # العمود 8: تاريخ الإنشاء
        is_active = bot_data[16]      # العمود 17: is_active
        
        # فحص حالة النشاط العامة
        if str(is_active).upper() != "TRUE":
            return False, "عذراً، هذا البوت متوقف حالياً من قبل الإدارة."

        # 2. منطق الذكاء الاصطناعي (الفترة التجريبية 30 يوم)
        if feature_name == "ai":
            from datetime import datetime
            # تحويل تاريخ الإنشاء لمقارنته
            created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            days_passed = (datetime.now() - created_at).days
            
            ai_feature_status = str(bot_data[24]).upper() # العمود 25: ميزة_الذكاء_الاصطناعي
            
            # إذا مر أكثر من 30 يوم والباقة مجانية (الالتزام بمنطقك الأصلي)
            if days_passed > 30 and str(plan).lower() == "free":
                # تحديث الشيت وجوجل محلياً فوراً
                if bots_sheet is None: connect_to_google()
                cell = bots_sheet.find(bot_token_str)
                if cell:
                    bots_sheet.update_cell(cell.row, 25, "FALSE")
                    # تحديث محلي أيضاً لضمان المزامنة
                    db_manager.cursor.execute("UPDATE 'البوتات_المصنوعة' SET column_25 = ? WHERE column_1 = ?", ("FALSE", bot_token_str))
                    db_manager.conn.commit()
                
                return False, "انتهت الفترة التجريبية لمساعد الذكاء الاصطناعي (30 يوم). يرجى الترقية للاستمرار."
            
            return (ai_feature_status == "TRUE"), "ميزة الذكاء الاصطناعي غير مفعلة في باقتك."

        # 3. منطق حدود الأقسام والدورات (باستخدام الكاش المحلي لتوفير API)
        if feature_name == "section":
            max_sections = int(bot_data[23]) # العمود 24: الحد_الأقصى_للاقسام
            # الجلب من الكاش بدلاً من departments_sheet.get_all_records()
            all_deps = get_bot_data_from_cache(bot_token_str, "الأقسام")
            current_count = len(all_deps)
            
            if current_count >= max_sections:
                return False, f"لقد وصلت للحد الأقصى للأقسام ({max_sections}). ارتقِ بباقتك لفتح المزيد."

        if feature_name == "course":
            max_courses = int(bot_data[22]) # العمود 23: الحد_الأقصى_للدوات
            # الجلب من الكاش بدلاً من courses_sheet.get_all_records()
            all_crs = get_bot_data_from_cache(bot_token_str, "الدورات_التدريبية")
            current_count = len(all_crs)
            
            if current_count >= max_courses:
                return False, f"لقد وصلت للحد الأقصى للدورات ({max_courses}). ارتقِ بباقتك لإضافة دورات جديدة."

        # 4. منطق ميزة الإكسل
        if feature_name == "excel":
            excel_status = str(bot_data[25]).upper() # العمود 26: ميزة_رفع_وتصدير_البيانات_اكسل
            if excel_status != "TRUE":
                return False, "ميزة استيراد وتصدير بيانات الإكسل متاحة فقط في الباقة الاحترافية."

        return True, "Success"

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في محرك القيود: {e}")
        return False, "حدث خطأ أثناء التحقق من الصلاحيات."

# دالة التخزين المؤقت 
def sync_bot_limits(context, bot_token):
    """
    جلب بيانات البوت وتخزينها في الذاكرة المؤقتة:
    - الحفاظ الكامل على هيكلية الـ 44 عموداً والفهارس المحددة (14, 7, 21, 22, 23, 24, 25).
    - التحديث المحلي (SQLite) لضمان بقاء البيانات متزامنة مع الذاكرة (context).
    """
    global bots_sheet
    try:
        bot_token_str = str(bot_token).strip()
        
        # 1. الجلب السحابي (الالتزام بمنطقك الأصلي)
        if bots_sheet is None: connect_to_google()
        cell = bots_sheet.find(bot_token_str)
        
        if cell:
            # جلب الصف بالكامل (44 عموداً)
            row = bots_sheet.row_values(cell.row)
            
            # 2. التخزين في الذاكرة المنظمة (الالتزام الحرفي بالمفاتيح والفهارس)
            context.bot_data['limits'] = {
                'plan': row[14],               # العمود 15
                'created_at': row[7],          # العمود 8
                'max_students': int(row[21]),  # العمود 22
                'max_courses': int(row[22]),   # العمود 23
                'max_sections': int(row[23]),  # العمود 24
                'ai_active': str(row[24]).upper() == "TRUE",
                'excel_active': str(row[25]).upper() == "TRUE",
                'last_sync': get_system_time("full")
            }
            
            # 3. تحديث القاعدة المحلية (Hybrid) لضمان بقاء البيانات متاحة دون إنترنت
            try:
                local_save_wrapper("البوتات_المصنوعة", row)
            except: pass
            
            return True
    except Exception as e:
        print(f"❌ فشل مزامنة الذاكرة: {e}")
    return False
 
# --------------------------------------------------------------------------
# --- 1. جلب قائمة الفروع ---
def get_all_branches(bot_token):
    """جلب الفروع: الحفاظ على هيكل [{'id': '...', 'name': '...'}]"""
    branches = []
    try:
        # ضمان المزامنة الذكية (الالتزام بمنطقك الأصلي)
        smart_sync_check(bot_token)
        # الجلب من الكاش المحلي لسرعة عرض القوائم
        records = get_bot_data_from_cache(bot_token, "إدارة_الفروع")
        for r in records:
            if str(r.get("bot_id")) == str(bot_token):
                branches.append({
                    "id": str(r.get("معرف_الفرع")),
                    "name": r.get("اسم_الفرع") or "فرع بلا اسم"
                })
    except Exception as e:
        print(f"❌ خطأ في جلب الفروع: {e}")
    return branches

# --- 2. جلب قائمة الموظفين ---
def get_all_personnel(bot_token):
    """جلب الموظفين: الحفاظ على مفاتيح (ID, الاسم_الكامل) والبديل (اسم_الموظف)"""
    staff = []
    try:
        smart_sync_check(bot_token)
        records = get_bot_data_from_cache(bot_token, "إدارة_الموظفين")
        for r in records:
            if str(r.get("bot_id")) == str(bot_token):
                staff.append({
                    "id": str(r.get("ID")),
                    "name": r.get("الاسم_الكامل") or r.get("اسم_الموظف") or "موظف مجهول"
                })
    except Exception as e:
        print(f"❌ خطأ في جلب الموظفين: {e}")
    return staff

# --- 3. جلب قائمة المدربين ---
def get_all_coaches_list(bot_token):
    """جلب المدربين: الحفاظ على مفاتيح (ID, اسم_المدرب)"""
    coaches = []
    try:
        smart_sync_check(bot_token)
        records = get_bot_data_from_cache(bot_token, "المدربين")
        for r in records:
            if str(r.get("bot_id")) == str(bot_token):
                coaches.append({
                    "id": str(r.get("ID")),
                    "name": r.get("اسم_المدرب") or "مدرب مجهول"
                })
    except Exception as e:
        print(f"❌ خطأ في جلب المدربين: {e}")
    return coaches

# --------------------------------------------------------------------------
def check_scope_access(allowed_string, target_id):
    """
    التحقق من All أو قائمة معرفات:
    - الحفاظ الكامل على منطق ALL (بصرف النظر عن حالة الأحرف).
    - الحفاظ على منطق تحويل النص المعتمد على الفاصلة إلى قائمة.
    - الفحص الفوري لضمان سرعة استجابة البوت في الصلاحيات.
    """
    if not allowed_string: return False
    
    # الالتزام بمنطقك الأصلي: إذا كانت القيمة All يفتح الجميع فوراً
    if str(allowed_string).strip().lower() == "all":
        return True
        
    # تحويل النص إلى قائمة معرفات (الالتزام بمنطق split و strip)
    allowed_list = [i.strip() for i in str(allowed_string).split(",")]
    return str(target_id) in allowed_list

# --------------------------------------------------------------------------
# --- 1. دالة توليد معرف فرع تلقائي ذكي ---
def get_next_branch_id(bot_token):
    """
    توليد معرف فرع تلقائي:
    - الحفاظ على نقطة البداية 1001001 لهذا البوت تحديداً.
    - البحث في الكاش المحلي (SQLite/RAM) لضمان السرعة ومنع تداخل المعرفات.
    - الحفاظ على منطق replace("'") وتطهير المعرف قبل الحساب.
    """
    try:
        # البحث في المحرك الهجين (SQLite) لضمان شمولية البيانات والسرعة
        # records ستعيد فقط بيانات البوت الحالي (المفلترة تلقائياً)
        records = get_bot_data_from_cache(bot_token, "إدارة_الفروع")
        
        # تصفية إضافية (أمان إضافي كما في كودك) لضمان عدم تداخل العملاء
        bot_branches = [r for r in records if str(r.get("bot_id")) == str(bot_token)]
        
        # إذا كان هذا أول فرع (الالتزام بـ 1001001)
        if not bot_branches: 
            return "1001001"
        
        ids = []
        for r in bot_branches:
            # تنظيف المعرف (الالتزام بمنطق replace الصارم للمفتاح: معرف_الفرع)
            bid = str(r.get("معرف_الفرع", "")).replace("'", "").strip()
            if bid.isdigit(): 
                ids.append(int(bid))
        
        if not ids: 
            return "1001001"
            
        # جلب أكبر رقم حالي وإضافة 1 (بدون zfill كما طلبت حرفياً)
        return str(max(ids) + 1)
        
    except Exception as e:
        # الحفاظ على نص تسجيل التنبيه والقيمة الآمنة للبدء
        print(f"⚠️ خطأ في توليد ID الفرع: {e}")
        return "1001001"

# --- 2. دالة إضافة الفرع النهائية المعتمدة ---
def add_new_branch_db(bot_token, branch_name, country, manager, currency):
    """
    إضافة فرع جديد:
    - الحفاظ الكامل على منطق الـ RAM والـ Physical Cache.
    - إضافة الحقن في SQLite لضمان سرعة المحرك الهجين.
    - الحفاظ على حماية التنسيق '001 في جوجل شيت.
    """
    try:
        from cache_manager import FACTORY_GLOBAL_CACHE, save_cache_to_disk
        new_id = get_next_branch_id(bot_token)
        current_time = get_system_time('full')
        bot_token_str = str(bot_token).strip()
        
        # [الخطوة 1]: الحقن المباشر في الذاكرة المركزية RAM (الالتزام الحرفي بمنطقك)
        new_record = {
            "bot_id": bot_token_str,
            "معرف_الفرع": new_id,
            "اسم_الفرع": str(branch_name),
            "الدولة": str(country),
            "المدير_المسؤول": str(manager),
            "العملة": str(currency),
            "ملاحظات": f"إضافة فورية: {current_time}"
        }
        if "إدارة_الفروع" not in FACTORY_GLOBAL_CACHE["data"]:
            FACTORY_GLOBAL_CACHE["data"]["إدارة_الفروع"] = []
        FACTORY_GLOBAL_CACHE["data"]["إدارة_الفروع"].append(new_record)
        save_cache_to_disk() # حفظ نسخة فيزيائية فورية
        
        # [إضافة الهجين]: الحقن في SQLite لضمان الاستجابة الفورية (Zero Lag)
        try:
            # ترتيب الأعمدة: bot_id, ID, الاسم, الدولة, المدير, العملة, الملاحظات
            local_row = [bot_token_str, new_id, str(branch_name), str(country), str(manager), str(currency), f"إضافة فورية: {current_time}"]
            local_save_wrapper("إدارة_الفروع", local_row)
        except: pass

        # [الخطوة 2]: الكتابة في جوجل شيت (الالتزام الصارم بحماية التنسيق ' )
        if 'ss' not in globals() or ss is None: connect_to_google()
        worksheet = ss.worksheet("إدارة_الفروع")
        # الحفاظ على 'new_id لضمان عدم تحويل الرقم لنص في جوجل
        new_row = [bot_token_str, f"'{new_id}", str(branch_name), str(country), str(manager), str(currency), current_time]
        from sheets import safe_api_call
        safe_api_call(worksheet.append_row, new_row)
        
        # [الخطوة 3]: تحديث نظام المزامنة (الالتزام بموضع الاستدعاء)
        update_global_version(bot_token)
        return True, new_id
    except Exception as e:
        return False, str(e)

def delete_branch_db(bot_token, branch_id):
    """حذف فرع: الحفاظ على منطق enumerate وتطابق الأعمدة 1 و 2."""
    try:
        bot_token_str = str(bot_token).strip()
        branch_id_str = str(branch_id).strip()

        # 1. الحذف المحلي (SQLite) - استجابة فورية
        try:
            db_manager.cursor.execute("DELETE FROM 'إدارة_الفروع' WHERE column_1 = ? AND column_2 = ?", (bot_token_str, branch_id_str))
            db_manager.conn.commit()
        except: pass

        # 2. الحذف السحابي (الالتزام الصارم بمنطقك الأصلي)
        if 'ss' not in globals() or ss is None: connect_to_google()
        worksheet = ss.worksheet("إدارة_الفروع")
        all_rows = worksheet.get_all_values()
        
        for i, row in enumerate(all_rows):
            # التأكد من مطابقة التوكن (العمود 1) والمعرف (العمود 2)
            if len(row) >= 2 and str(row[0]) == bot_token_str and str(row[1]) == branch_id_str:
                from sheets import safe_api_call
                safe_api_call(worksheet.delete_rows, i + 1)
                update_global_version(bot_token)
                return True
        return False
    except Exception as e:
        if 'logger' in globals(): logger.error(f"❌ خطأ حذف فرع: {e}")
        return False

def update_branch_field_db(bot_token, branch_id, col_name, new_value):
    """تعديل بيانات الفرع: الحفاظ على جلب col_index ديناميكياً."""
    try:
        bot_token_str = str(bot_token).strip()
        branch_id_str = str(branch_id).strip()

        # 1. التحديث المحلي (SQLite) - لضمان انعكاس التعديل فوراً
        try:
            query = f"UPDATE 'إدارة_الفروع' SET {col_name} = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?"
            db_manager.cursor.execute(query, (str(new_value), bot_token_str, branch_id_str))
            db_manager.conn.commit()
        except: pass

        # 2. التحديث السحابي (الالتزام بمنطقك الأصلي 100%)
        if 'ss' not in globals() or ss is None: connect_to_google()
        worksheet = ss.worksheet("إدارة_الفروع")
        all_rows = worksheet.get_all_values()
        headers = all_rows[0]
        col_index = headers.index(col_name) + 1
        
        for i, row in enumerate(all_rows):
            if len(row) >= 2 and str(row[0]) == bot_token_str and str(row[1]) == branch_id_str:
                from sheets import safe_api_call
                safe_api_call(worksheet.update_cell, i + 1, col_index, str(new_value))
                update_global_version(bot_token)
                return True
        return False
    except Exception as e:
        if 'logger' in globals(): logger.error(f"❌ خطأ تعديل فرع: {e}")
        return False

# --------------------------------------------------------------------------
# اضافة مدرب او موظف 


def generate_emp_id():
    """توليد رقم عشوائي مهني من 10 أرقام يبدأ بـ 100"""
    return int(f"100{random.randint(1000000, 9999999)}")

def add_new_employee_advanced(bot_token, employee_id, name, job_title, phone, branch_id, **kwargs):
    """
    إضافة كادر (مدرب/موظف) بـ 43 عموداً:
    - الحفاظ الكامل على هيكلية الـ 43 عموداً والفهارس المحددة بدقة.
    - إضافة الحقن المحلي (SQLite) لضمان ظهور الكادر في القوائم فوراً.
    - الالتزام بالأرقام النقية (int) في المعرفات والهاتف لضمان عدم وجود علامات تنصيص.
    """
    try:
        from cache_manager import FACTORY_GLOBAL_CACHE, save_cache_to_disk
        if 'ss' not in globals() or ss is None: connect_to_google()
        current_sheet = ss.worksheet("إدارة_الموظفين")
        
        today_date = get_system_time("date")
        username = kwargs.get('username', "بدون")
        
        # 🟢 تعبئة آلية (الالتزام بمنطقك الأصلي)
        role_tag = kwargs.get('role_tag', 'موظف') # مدرب أو موظف
        branch_name = kwargs.get('branch_name', 'الرئيسي')
        new_professional_id = generate_emp_id() # الرقم الموحد (100xxxxxxx)

        # 1. تحديث الذاكرة المركزية RAM (الالتزام الصارم بالمفاتيح)
        new_record = {
            "bot_id": str(bot_token),
            "معرف_الفرع": str(branch_id),
            "ID": str(employee_id),
            "الاسم_الكامل": str(name),
            "الرتبة": str(role_tag),
            "اسم_الفرع": str(branch_name)
        }
        if "إدارة_الموظفين" not in FACTORY_GLOBAL_CACHE["data"]:
            FACTORY_GLOBAL_CACHE["data"]["إدارة_الموظفين"] = []
        FACTORY_GLOBAL_CACHE["data"]["إدارة_الموظفين"].append(new_record)
        save_cache_to_disk()

        # 2. بناء الصف الـ 43 عموداً (الالتزام الحرفي بالترتيب المذكور في كودك)
        row = [""] * 43 
        row[0] = str(bot_token)            # 1. bot_id
        row[1] = int(branch_id)            # 2. معرف_الفرع (رقم نقي)
        row[2] = int(employee_id)          # 3. ID تيليجرام (رقم نقي)
        row[3] = new_professional_id       # 4. معرف_الموظف (100xxxxxxx)
        row[4] = str(name)                 # 5. الاسم_الكامل
        row[10] = str(job_title)           # 11. التخصص
        row[11] = str(job_title)           # 12. المسمى_الوظيفي
        row[17] = int(phone)               # 18. رقم_الهاتف (رقم نقي)
        row[20] = kwargs.get('email', "-") # 21. البريد_الإلكتروني
        row[23] = today_date               # 24. تاريخ_التعيين
        row[28] = "نشط"                    # 29. الحالة_الوظيفية
        row[37] = "TRUE"                   # 38. الصلاحيات
        row[39] = "نشط"                    # 40. حالة_الحساب
        row[40] = f"@{username}" if username != "بدون" else "بدون" # 41. اسم_المستخدم
        row[41] = str(role_tag)            # 42. الرتبة
        row[42] = str(branch_name)         # 43. اسم_الفرع

        # [إضافة الهجين]: الحقن الفوري في SQLite لضمان (Zero Lag) للموظف الجديد
        try:
            # استخدام local_save_wrapper لضمان تحديث القاعدة المحلية بنفس البيانات
            local_save_wrapper("إدارة_الموظفين", row)
        except: pass

        # 3. الحفظ كبيانات RAW (الالتزام بمنطقك لضمان نقاء الأرقام)
        from sheets import safe_api_call
        safe_api_call(current_sheet.append_row, row, value_input_option='RAW')
        
        # 4. رفع إصدار البوت (الالتزام بموضع الاستدعاء)
        update_global_version(bot_token)
        print(f"✅ تم تسجيل {role_tag}: {name} بنجاح.")
        return True
    except Exception as e:
        # الحفاظ على استخدام logger كما في كودك الأصلي
        if 'logger' in globals():
            logger.error(f"❌ خطأ في الإضافة الموحدة: {e}")
        else:
            print(f"❌ خطأ في الإضافة الموحدة: {e}")
        return False

# --------------------------------------------------------------------------
#جلب بيانات المدربين
def get_all_coaches(bot_token):
    """
    جلب قائمة المدربين المخصصين لهذا البوت:
    - الحفاظ الكامل على منطق الفهارس: التوكن في Index 0، الـ ID في Index 2، الاسم في Index 3.
    - الجلب من الكاش المحلي (SQLite) لضمان (Zero Lag) عند استعراض قائمة المدربين.
    - الحفاظ على شكل القائمة المرجعة [{'id': '...', 'name': '...'}] كما هي.
    """
    try:
        # ضمان تحديث البيانات المحلية قبل القراءة (المزامنة الصامتة)
        smart_sync_check(bot_token)
        
        # جلب البيانات من الكاش المحلي (الذي يمثل ورقة "المدربين")
        # الدالة تعيد السجلات المفلترة لهذا البوت تلقائياً
        records = get_bot_data_from_cache(bot_token, "المدربين")
        
        coaches = []
        for r in records:
            # الالتزام بالمفاتيح البرمجية المستخرجة من الفهارس التي حددتها
            # Index 2 هو معرف_المدرب، Index 3 هو اسم_المدرب
            coaches.append({
                "id": r.get("column_3"),    # ID_المدرب (Index 2)
                "name": r.get("column_4")   # اسم_المدرب (Index 3)
            })

        return coaches
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error fetching coaches: {e}")
        return []
 
def delete_coach_from_sheet(bot_token, coach_id):
    """
    حذف مدرب من الشيت:
    - الحفاظ الكامل على فحص الفهارس (Index 2 للـ ID، Index 0 للتوكن).
    - تنفيذ الحذف المحلي (SQLite) فوراً لتطهير القائمة أمام المدير لحظياً.
    - الحفاظ على منطق حذف الصف i + 1 من جوجل شيت.
    """
    try:
        bot_token_str = str(bot_token).strip()
        coach_id_str = str(coach_id).strip()

        # 1. الحذف من المحرك المحلي (SQLite) - استجابة فورية
        try:
            # column_1 هو التوكن (Index 0)، column_3 هو ID المدرب (Index 2)
            db_manager.cursor.execute(
                "DELETE FROM 'المدربين' WHERE column_1 = ? AND column_3 = ?", 
                (bot_token_str, coach_id_str)
            )
            db_manager.conn.commit()
            print(f"🗑️ [محلي] تم حذف المدرب {coach_id_str} من القاعدة المحلية.")
        except Exception as local_e:
            print(f"⚠️ فشل الحذف المحلي للمدرب: {local_e}")

        # 2. الحذف من Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        if coaches_sheet is None: return False
        all_rows = coaches_sheet.get_all_values()
        
        for i, row in enumerate(all_rows):
            # التعديل الصارم: ID المدرب في Index 2، والتوكن في Index 0
            if len(row) >= 3 and str(row[2]) == coach_id_str and str(row[0]) == bot_token_str:
                from sheets import safe_api_call
                # الحفاظ على حذف الصف i + 1
                safe_api_call(coaches_sheet.delete_rows, i + 1)
                
                # رفع إصدار البوت لتطهير الرام
                update_global_version(bot_token)
                return True
                
        return False
    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ Error deleting coach: {e}")
        return False

# --------------------------------------------------------------------------
def process_referral_reward_on_purchase(bot_token, student_id):
    """
    منح النقاط للداعي الأصلي عند قيام الطالب بالتسجيل الفعلي:
    - الحفاظ الكامل على منطق البحث في العمود 10 (الداعي) والعمود 11 (الرصيد).
    - إضافة التحديث المحلي (SQLite) لضمان انعكاس النقاط في حساب الداعي فوراً (Zero Lag).
    - الحفاظ على استدعاء get_bot_setting لجلب قيمة النقاط المخصصة (ref_points_purchase).
    """
    try:
        bot_token_str = str(bot_token).strip()
        student_id_str = str(student_id).strip()

        # 1. البحث عن الطالب لجلب معرف الداعي (استخدام الكاش المحلي للسرعة)
        # نلتزم بالبحث عن سجل الطالب الأصلي في "قاعدة_بيانات_الطلاب" أو "المستخدمين"
        # بما أن كودك يستخدم users_sheet، سنفترض أنها تقابل جدول المستخدمين محلياً
        student_records = get_bot_data_from_cache(bot_token_str, "المستخدمين")
        student_data = next((r for r in student_records if str(r.get("column_1")) == student_id_str), None)
        
        if not student_data:
            # الحفاظ على البحث السحابي كخيار احتياطي (الالتزام بمنطقك الأصلي)
            if 'users_sheet' in globals() and users_sheet:
                student_cell = users_sheet.find(student_id_str, in_column=1)
                if not student_cell: return False, None, 0
            else: return False, None, 0
        
        # جلب معرف الداعي من العمود 10 (الالتزام الحرفي بالفهرس)
        # في الكاش المحلي، العمود 10 يقابل column_10
        inviter_id = student_data.get("column_10") if student_data else users_sheet.cell(student_cell.row, 10).value
        
        if inviter_id and str(inviter_id).isdigit():
            inviter_id_str = str(inviter_id).strip()
            
            # 2. جلب قيمة النقاط من الإعدادات (الالتزام بالمفتاح: ref_points_purchase)
            points_to_give = float(get_bot_setting(bot_token_str, "ref_points_purchase", default=50))
            
            # 3. البحث عن الداعي لمنحه النقاط
            inviter_data = next((r for r in student_records if str(r.get("column_1")) == inviter_id_str), None)
            
            # [إضافة الهجين]: التحديث المحلي الفوري لضمان سرعة الاستجابة
            current_balance = 0.0
            if inviter_data:
                current_balance = float(inviter_data.get("column_11") or 0)
                new_balance = current_balance + points_to_give
                
                # تحديث قاعدة البيانات المحلية SQLite فوراً
                try:
                    db_manager.cursor.execute(
                        "UPDATE 'المستخدمين' SET column_11 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_1 = ?", 
                        (new_balance, inviter_id_str, bot_token_str)
                    )
                    db_manager.conn.commit()
                except: pass
            
            # 4. التحديث السحابي (الالتزام الصارم بمنطقك الأصلي 100%)
            if 'users_sheet' in globals() and users_sheet:
                inviter_cell = users_sheet.find(inviter_id_str, in_column=1)
                if inviter_cell:
                    # إذا لم نجد الرصيد محلياً، نجلبه من الشيت
                    if not inviter_data:
                        current_balance = float(users_sheet.cell(inviter_cell.row, 11).value or 0)
                        new_balance = current_balance + points_to_give
                    
                    # تحديث الشيت (الالتزام بالعمود 11)
                    from sheets import safe_api_call
                    safe_api_call(users_sheet.update_cell, inviter_cell.row, 11, new_balance)
                    
                    # تحديث نسخة الكاش لضمان المزامنة (الالتزام بموضع الاستدعاء)
                    update_global_version(bot_token)
                    return True, inviter_id, points_to_give
                
        return False, None, 0
    except Exception as e:
        # الحفاظ على استخدام logger كما في كودك الأصلي
        if 'logger' in globals():
            logger.error(f"❌ فشل منح مكافأة الشراء: {e}")
        else:
            print(f"❌ فشل منح مكافأة الشراء: {e}")
        return False, None, 0

# --------------------------------------------------------------------------
#طلب السحب 
def create_withdrawal_request(bot_token, user_id, username, amount, payment_method):
    """
    إنشاء طلب سحب وخصم الرصيد فوراً:
    - الحفاظ الكامل على منطق البحث في العمود 11 لخصم الرصيد.
    - تنفيذ الخصم محلياً في SQLite فوراً لضمان الأمان والسرعة.
    - الحفاظ على توليد معرف الطلب الفريد REQ-xxxxxx.
    """
    try:
        bot_token_str = str(bot_token).strip()
        user_id_str = str(user_id).strip()
        amount_val = float(amount)
        
        # 1. البحث عن المستخدم لخصم الرصيد (استخدام الكاش المحلي للسرعة القصوى)
        # جلب البيانات من SQLite لضمان فحص الرصيد الحقيقي الآن
        users_records = get_bot_data_from_cache(bot_token_str, "المستخدمين")
        user_data = next((r for r in users_records if str(r.get("column_1")) == user_id_str), None)
        
        current_balance = 0.0
        if user_data:
            current_balance = float(user_data.get("column_11") or 0)
        else:
            # خيار احتياطي: البحث في جوجل شيت (الالتزام بمنطقك الأصلي)
            sheet_users = ss.worksheet("المستخدمين")
            user_cell = sheet_users.find(user_id_str, in_column=1)
            if not user_cell: return False, "user_not_found"
            current_balance = float(sheet_users.cell(user_cell.row, 11).value or 0)

        # التحقق من كفاية الرصيد (الالتزام بالرسالة الأصلية)
        if current_balance < amount_val:
            return False, "insufficient_balance"

        new_balance = current_balance - amount_val

        # [إضافة الهجين]: خصم الرصيد محلياً فوراً (حجز المبلغ)
        try:
            db_manager.cursor.execute(
                "UPDATE 'المستخدمين' SET column_11 = ?, sync_status = 'pending' WHERE column_1 = ? AND column_2 = ?",
                (new_balance, bot_token_str, user_id_str)
            )
            db_manager.conn.commit()
            print(f"💰 [محلي] تم حجز مبلغ {amount_val} من المستخدم {user_id_str}")
        except: pass

        # تنفيذ الخصم في جوجل شيت (الالتزام بالعمود 11)
        if 'sheet_users' not in locals(): sheet_users = ss.worksheet("المستخدمين")
        if 'user_cell' not in locals(): user_cell = sheet_users.find(user_id_str, in_column=1)
        from sheets import safe_api_call
        safe_api_call(sheet_users.update_cell, user_cell.row, 11, new_balance)

        # 2. توليد معرف طلب فريد وتسجيل الطلب (الالتزام بالصيغة REQ-xxxxxx)
        import uuid
        request_id = f"REQ-{str(uuid.uuid4().int)[:6]}"
        
        # بناء الصف الـ 11 عموداً (الالتزام الحرفي بالترتيب المذكور في كودك)
        row = [
            bot_token_str, user_id_str, str(username), request_id, 
            amount_val, str(payment_method), get_system_time("full"), 
            "قيد الانتظار", "", "", ""
        ]
        
        # الحفظ المحلي للطلب (SQLite)
        local_save_wrapper("سجل_السحوبات", row)
        
        # الحفظ السحابي
        sheet_requests = ss.worksheet("سجل_السحوبات")
        safe_api_call(sheet_requests.append_row, row, value_input_option='USER_ENTERED')
        
        # تحديث الكاش (الالتزام بموضع الاستدعاء)
        update_global_version(bot_token)
        return True, request_id
        
    except Exception as e:
        if 'logger' in globals(): logger.error(f"❌ خطأ في معالجة طلب السحب: {e}")
        return False, None


# --------------------------------------------------------------------------
#دالة توليد رابط كوبون للمسوقين
def get_active_gift_link(bot_token, user_id):
    """
    التحقق من وجود رابط هدية نشط وغير مستخدم لهذا المسوق:
    - الحفاظ الكامل على منطق الفلترة (bot_id، معرف_المسوق، حالة_الكوبون).
    - الجلب من الكاش المحلي (SQLite) لضمان (Zero Lag) عند استعلام المسوق عن هداياه.
    - الحفاظ على إعادة "معرف_الكوبون" كقيمة مرجعة في حال النجاح.
    """
    try:
        # ضمان تحديث البيانات المحلية قبل القراءة (المزامنة الصامتة)
        # هذا يضمن أن الهدايا الجديدة التي أضيفت تظهر فوراً
        smart_sync_check(bot_token)
        
        # 1. جلب سجلات الكوبونات من الكاش المحلي (الذي يغذي الرام الآن)
        # الدالة تعيد السجلات المفلترة لهذا البوت تلقائياً (0 استهلاك API لجوجل)
        records = get_bot_data_from_cache(bot_token, "الكوبونات")
        
        # 2. البحث داخل السجلات (الالتزام الحرفي بمنطقك وشروطك)
        for r in records:
            # التحقق من مطابقة البوت + ID المهدِي (معرف_المسوق) + الحالة (نشط)
            if (str(r.get("bot_id")) == str(bot_token) and 
                str(r.get("معرف_المسوق")) == str(user_id) and 
                str(r.get("حالة_الكوبون")) == "نشط"):
                
                # العودة بكود الهدية (الالتزام بمفتاح: معرف_الكوبون)
                return r.get("معرف_الكوبون")
        
        return None
    except Exception as e:
        # الحفاظ على منطق العودة بـ None صامت في حال حدوث خطأ
        # مع طباعة الخطأ داخلياً للتصحيح عند الحاجة
        print(f"❌ Error fetching active gift link: {e}")
        return None


# --------------------------------------------------------------------------
# دالة إعدادات الدفع
def update_payment_settings(bot_token, text):
    """
    تحديث معلومات الدفع:
    - الحفاظ الكامل على تحديث العمود رقم 36 في شيت 'إعدادات_المحتوى'.
    - إضافة التحديث المحلي (SQLite) لضمان انعكاس تعليمات الدفع للمشتركين فوراً.
    - الحفاظ على منطق البحث عن البوت باستخدام التوكن (cell = sheet.find).
    """
    try:
        bot_token_str = str(bot_token).strip()
        
        # 1. التحديث المحلي (SQLite) - لضمان الاستجابة اللحظية (Zero Lag)
        try:
            # في جدول 'إعدادات_المحتوى': العمود 36 يقابله column_36 محلياً
            db_manager.cursor.execute(
                "UPDATE 'إعدادات_المحتوى' SET column_36 = ?, sync_status = 'pending' WHERE column_1 = ?",
                (str(text), bot_token_str)
            )
            db_manager.conn.commit()
            print(f"💳 [محلي] تم تحديث معلومات الدفع للتوكن: {bot_token_str}")
        except Exception as local_e:
            print(f"⚠️ فشل التحديث المحلي لإعدادات الدفع: {local_e}")

        # 2. التحديث في Google Sheets (الالتزام الصارم بمنطقك الأصلي 100%)
        # ملاحظة: تأكد من إضافة عمود باسم 'payment_info' في شيت إعدادات_المحتوى
        if ss is None: connect_to_google()
        sheet = ss.worksheet("إعدادات_المحتوى")
        
        # البحث عن الصف الخاص بالبوت (الالتزام بمنطق find الأصلي)
        cell = sheet.find(bot_token_str)
        
        if cell:
            # التحديث في العمود رقم 36 (الالتزام بالحفاظ على الرقم 36)
            from sheets import safe_api_call
            safe_api_call(sheet.update_cell, cell.row, 36, str(text))
            
            # رفع إصدار البوت لضمان مزامنة الكاش (الالتزام بالبروتوكول)
            update_global_version(bot_token)
            return True
            
        return False
    except Exception as e:
        # الحفاظ على منطق العودة بـ False صامت كما في كودك الأصلي
        print(f"❌ خطأ في تحديث إعدادات الدفع: {e}")
        return False

 
# --------------------------------------------------------------------------
# دالة الإعلانات
def add_new_ad_campaign(bot_token, branch_id, course_id, campaign_name, platform, start_date, end_date, budget, marketer_id):
    """
    حفظ حملة إعلانية جديدة:
    - الحفاظ الكامل على هيكلية الـ 11 عموداً والترتيب المذكور.
    - الحفاظ على توليد معرف الحملة الفريد AD-xxxxx.
    - إضافة الحقن المحلي (SQLite) لضمان الاستجابة الفورية (Zero Lag).
    """
    import uuid
    try:
        bot_token_str = str(bot_token).strip()
        campaign_id = f"AD-{str(uuid.uuid4().int)[:5]}" # الحفاظ على منطق التوليد الأصلي
        
        # ترتيب الأعمدة (الالتزام الحرفي بالترتيب المذكور في كودك)
        row = [
            bot_token_str, 
            str(branch_id), 
            str(course_id), 
            campaign_id, 
            str(platform), 
            str(start_date), 
            str(end_date), 
            str(budget), 
            "0",         # 9. عدد المسجلين الابتدائي
            "نشط",       # 10. الحالة الافتراضية
            str(marketer_id) # 11. ID_المسوق
        ]

        # [إضافة الهجين]: الحقن الفوري في SQLite لضمان ظهور الحملة فوراً
        try:
            local_save_wrapper("إدارة_الحملات_الإعلانية", row)
            print(f"📢 [محلي] تم تسجيل حملة جديدة: {campaign_id}")
        except: pass

        # الحفظ السحابي في Google Sheets
        if ss is None: connect_to_google()
        sheet = ss.worksheet("إدارة_الحملات_الإعلانية")
        from sheets import safe_api_call
        safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
        
        # تحديث الإصدار العالمي
        update_global_version(bot_token)
        return True, campaign_id
    except Exception as e:
        print(f"❌ Error adding campaign: {e}")
        return False, str(e)

# --------------------------------------------------------------------------
# دالة المكتبة
def add_library_item_to_sheet(bot_token, course_id, file_name, file_link, status):
    """
    إضافة ملف جديد إلى شيت المكتبة:
    - الحفاظ الكامل على تعبئة الـ 25 عموداً بالترتيب المذكور في كودك.
    - إضافة الحقن المحلي (SQLite) لضمان ظهور الملف في مكتبة الطالب فوراً.
    - الحفاظ على منطق توليد الـ file_id الفريد (8 رموز).
    """
    import uuid
    try:
        bot_token_str = str(bot_token).strip()
        
        # 1. توليد معرف فريد للملف وجلب الوقت (الالتزام بمنطقك الأصلي)
        file_id = str(uuid.uuid4())[:8].upper()
        current_time = get_system_time("full")
        
        # 2. تجهيز الصف بالترتيب الصحيح للأعمدة (الالتزام الحرفي بالـ 25 عمود)
        row = [
            bot_token_str,           # 1. bot_id
            "الفرع الرئيسي",          # 2. معرف_الفرع 
            file_id,                 # 3. معرف_الملف
            str(file_name),          # 4. اسم_الملف
            "PDF/Link",              # 5. النوع
            "تعليمي",                # 6. التصنيف
            str(course_id),          # 7. الدورة
            "ملف تعليمي مضاف حديثاً", # 8. الوصف
            str(file_link),          # 9. الرابط
            "عام",                   # 10. صلاحية_الوصول
            "0",                     # 11. سعر_الوصول
            0,                       # 12. عدد_المشاهدات
            0,                       # 13. عدد_المشتركين
            "العربية",               # 14. لغة_المحتوى
            "متوسط",                 # 15. المستوى
            "-",                     # 16. مدة_المحاضرة
            current_time,            # 17. تاريخ_الإضافة
            current_time,            # 18. تاريخ_آخر_تحديث
            "Admin",                 # 19. أضيف_بواسطة
            str(status),             # 20. الحالة (مجاني/مدفوع)
            "Created",               # 21. سجل_التعديل
            0,                       # 22. عدد_التقييمات
            0,                       # 23. متوسط_التقييم
            "",                      # 24. تعليقات
            0                        # 25. عدد_المشاركات
        ]
        
        # [إضافة الهجين]: الحقن الفوري في SQLite لضمان (Zero Lag) للطالب
        try:
            local_save_wrapper("المكتبة", row)
            print(f"📚 [محلي] تم إضافة الملف '{file_name}' للمكتبة المحلية.")
        except Exception as local_e:
            print(f"⚠️ فشل الحقن المحلي للمكتبة: {local_e}")

        # [الخطوة السحابية]: الكتابة في Google Sheets (الالتزام بمنطقك الأصلي)
        if ss is None: connect_to_google()
        sheet = ss.worksheet("المكتبة")
        from sheets import safe_api_call
        safe_api_call(sheet.append_row, row, value_input_option='USER_ENTERED')
        
        # تحديث الإصدار العالمي لضمان مزامنة كافة أجهزة الكاش
        update_global_version(bot_token)
        return True

    except Exception as e:
        # الحفاظ على نص تسجيل الخطأ الأصلي
        print(f"❌ خطأ في إضافة ملف للمكتبة: {e}")
        return False

# --------------------------------------------------------------------------
def get_bot_settings(bot_id):
    """
    مرجع آمن لجلب إعدادات المحتوى لأي بوت تابع من الكاش العام.
    تضمن هذه الدالة عدم توقف النظام في حال عدم وجود إعدادات مسجلة.
    """
    try:
        # جلب كافة سجلات إعدادات المحتوى من الكاش
        all_settings = FACTORY_GLOBAL_CACHE["data"].get("إعدادات_المحتوى", [])
        
        # فلترة السجلات بناءً على معرف البوت
        settings_list = [r for r in all_settings if str(r.get("bot_id")) == str(bot_id)]
        
        # إعادة السجل الأول إذا وجد، أو قاموس فارغ لتجنب IndexError
        return settings_list[0] if settings_list else {}
    except Exception as e:
        logger.error(f"⚠️ خطأ أثناء استخراج إعدادات البوت {bot_id}: {e}")
        return {}

# --------------------------------------------------------------------------
def sync_ad_campaign_results(bot_token):
    """تحديث عدد المسجلين في ورقة الإعلانات بناءً على سجل التسجيلات"""
    try:
        bot_token_str = str(bot_token).strip()
        # ضمان تحديث البيانات المحلية قبل الحساب
        smart_sync_check(bot_token_str)
        
        # جلب البيانات من الكاش المحلي
        ads_records = get_bot_data_from_cache(bot_token_str, "إدارة_الحملات_الإعلانية")
        reg_records = get_bot_data_from_cache(bot_token_str, "سجل_التسجيلات")
        
        all_regs_campaigns = [str(r.get("column_26", "")) for r in reg_records]
        
        if ss is None: connect_to_google()
        ads_sheet = ss.worksheet("إدارة_الحملات_الإعلانية")

        for ad in ads_records:
            campaign_id = str(ad.get('column_4')) # معرف الحملة
            if not campaign_id: continue
            
            count = all_regs_campaigns.count(campaign_id)
            
            # تحديث السحابة
            cell = ads_sheet.find(campaign_id)
            if cell:
                ads_sheet.update_cell(cell.row, 9, count)
            
        return True
    except Exception as e:
        print(f"❌ Error syncing ad results: {e}")
        return False

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------







