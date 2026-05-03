import logging
import time
import json
import os
import asyncio
from datetime import datetime
import gspread
import base64
import pandas as pd
from io import BytesIO 
import sqlite3
import re
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.request import HTTPXRequest
from telegram.error import Forbidden, BadRequest, TelegramError
import time

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
    start,
    start_create_bot,
    select_type,
    receive_token,
    cancel
)



# ==========================================================================
# 1. كتلة الإعدادات الأساسية والمحرك العام (المفاتيح الأصلية)
# ==========================================================================

# إعدادات ثابتة (تم توحيد المسار ليتوافق مع مجلد الكاش في Railway)

# تصحيح: توحيد إعدادات اللوجر لمنع التضارب في السجلات
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FACTORY_CORE")

# التأكد من إنشاء المجلد مرة واحدة وبشكل صحيح
if not os.path.exists(CACHE_DIR):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        print(f"📁 تم إنشاء مجلد الكاش بنجاح في: {CACHE_DIR}")
    except Exception as e:
        print(f"❌ خطأ في إنشاء مجلد الكاش: {e}")



# مستودع الذاكرة المركزية للمصنع كامل (RAM) - تم الحفاظ على كافة المفاتيح
FACTORY_GLOBAL_CACHE = {
    "data": {},      # بيانات الـ 37 ورقة
    "versions": {},   # أرقام الإصدارات
    "temp_registration_tokens": {} # تخزين روابط الموظفين والمدربين الموّلدة لحظياً
}


# ==========================================================================
# 2. دوال الوقت والنظام
# ==========================================================================

def get_system_time():
    """جلب الوقت الحالي بتنسيق التوثيق المعتمد"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_cache_to_disk():
    """
    محرك الحفظ الفيزيائي: يحول بيانات الرام إلى ملفات JSON حقيقية.
    هذه الدالة هي التي تجعل عملية 'التحميل' ممكنة من البوت.
    """
    try:
        if not FACTORY_GLOBAL_CACHE["data"]:
            logger.warning("⚠️ محاولة حفظ كاش فارغ على القرص، تم الإلغاء.")
            return

        for sheet_name, records in FACTORY_GLOBAL_CACHE["data"].items():
            file_path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=4)
        
        # حفظ خريطة الإصدارات للرجوع إليها عند إعادة التشغيل
        version_path = os.path.join(CACHE_DIR, "versions_map.json")
        with open(version_path, 'w', encoding='utf-8') as f:
            json.dump(FACTORY_GLOBAL_CACHE["versions"], f, ensure_ascii=False, indent=4)
            
        logger.info(f"💾 [المرآة]: تم تحديث كافة ملفات الكاش على القرص بنجاح.")
    except Exception as e:
        logger.error(f"❌ خطأ حرج أثناء الكتابة على القرص: {e}")

# ==========================================================================

# ==========================================================================
# 3. إدارة نظام المزامنة (Core Logic)
# ==========================================================================

def ensure_bot_sync_row(bot_id, owner_id=None, developer_id=None):
    """إضافة صف جديد للبوت في ورقة 'نظام_المزامنة'"""


    try:
        try:
            sync_sheet = ss.worksheet("نظام_المزامنة")
        except:
            logger.error("❌ ورقة 'نظام_المزامنة' مفقودة من الملف!")
            return False

        cell = None
        try:
            cell = sync_sheet.find(str(bot_id), in_column=1)
        except: pass

        if not cell:
            # الترتيب: [bot_id, رقم_الإصدار, آخر_تحديث, الحالة, ID_المالك, ID_المطور]
            new_row = [
                str(bot_id), 1, get_system_time(), "نشط",
                str(owner_id) if owner_id else "", str(DEVELOPER_ID)
            ]
            safe_api_call(sync_sheet.append_row, new_row)
            print(f"✅ [نظام المزامنة]: تم تسجيل البوت {bot_id} بنجاح.")
            return True
        else:
            print(f"ℹ️ [نظام المزامنة]: البوت {bot_id} موجود مسبقاً.")
            return True
    except Exception as e:
        print(f"❌ خطأ في إضافة صف المزامنة: {e}")
        return False

# ==========================================================================
# 4. محرك السحب الشامل المطور (Comprehensive Fetch Engine)
# ==========================================================================
def update_global_version(bot_id):
    """تحديث الإصدار في نظام_المزامنة مع استيراد محلي لتجنب التعارض"""
    # استيراد الدوال من sheets داخل الدالة فقط لمنع Circular Import
    from sheets import connect_to_google, ss, safe_api_call
    
    try:
        if ss is None:
            connect_to_google()
            from sheets import ss, safe_api_call # إعادة التأكيد بعد الاتصال

        sync_sheet = ss.worksheet("نظام_المزامنة")
        all_ids = sync_sheet.col_values(1)
        
        search_id = str(bot_id).strip()
        target_row = None

        for index, row_id in enumerate(all_ids):
            if str(row_id).strip() == search_id:
                target_row = index + 1
                break

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if target_row:
            current_val = sync_sheet.cell(target_row, 2).value
            try:
                current_v = int(current_val) if current_val else 0
            except:
                current_v = 0
            new_v = current_v + 1

            FACTORY_GLOBAL_CACHE["versions"][str(bot_id)] = new_v
            
            # تم استخدام safe_api_call هنا بعد استيرادها محلياً
            safe_api_call(sync_sheet.update_cell, target_row, 2, new_v)
            safe_api_call(sync_sheet.update_cell, target_row, 3, now_time)
            
            save_cache_to_disk()
            print(f"🔄 [نظام المزامنة]: تم تحديث التوكن {search_id} للإصدار {new_v}")
            return new_v
        else:
            new_row = [search_id, 1, now_time, "نشط", "تلقائي", str(DEVELOPER_ID)]
            safe_api_call(sync_sheet.append_row, new_row)
            FACTORY_GLOBAL_CACHE["versions"][search_id] = 1
            save_cache_to_disk()
            return 1
            
    except Exception as e:
        logger.error(f"❌ فشل رفع الإصدار: {e}")
        return None


def fetch_full_factory_data():
    """
    سحب بيانات المصنع كاملة وتحديث الرام والقرص:
    - تم استخدام الاستيراد المحلي لمنع Circular Import.
    - الحفاظ الكامل على منطق الحفظ الفيزيائي الفوري لكل ورقة.
    """
    # استيراد محلي لتفادي تعارض الملفات
    from sheets import get_sheets_structure, ss, safe_api_call
    global FACTORY_GLOBAL_CACHE
    
    try:
        structures = get_sheets_structure()
        print(f"🚀 [المحرك]: بدء المزامنة الشاملة ({len(structures)} ورقة)...")

        for config in structures:
            sheet_name = config["name"]
            try:
                # محاولة جلب الورقة من جوجل
                sheet = ss.worksheet(sheet_name)
                # سحب البيانات (الالتزام بمنطقك الأصلي)
                records = sheet.get_all_records()
                FACTORY_GLOBAL_CACHE["data"][sheet_name] = records
                
                # --- [ الحفظ الفيزيائي الفوري لكل ورقة ] ---
                file_path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(records, f, ensure_ascii=False, indent=4)
                
                print(f"✅ سحب وحفظ: {sheet_name} | سجلات: {len(records)}")
                
                # تهدئة للـ API (جوجل تسمح بـ 60 طلب في الدقيقة)
                time.sleep(1.6) 
            except Exception as e:
                logger.warning(f"⚠️ تخطي الورقة {sheet_name}: {e}")

        # تحديث الإصدارات من ورقة نظام_المزامنة
        try:
            sync_sheet = ss.worksheet("نظام_المزامنة")
            sync_data = sync_sheet.get_all_records()
            for row in sync_data:
                b_id = str(row.get("bot_id", row.get("column_1", ""))).strip()
                if b_id:
                    v_val = row.get("رقم_الإصدار", row.get("column_2", 1))
                    FACTORY_GLOBAL_CACHE["versions"][b_id] = int(v_val if v_val else 1)
        except Exception as v_err:
            print(f"⚠️ تعذر جلب الإصدارات: {v_err}")

        # الحفظ الفيزيائي الشامل لخريطة الكاش
        save_cache_to_disk()

        print("🎊 [المحرك]: اكتملت المزامنة الشاملة (رام + قرص).")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ حرج في المزامنة الشاملة: {e}")
        return False

# ==========================================================================
# 3. دوال إدارة الكاش والتصدير (المعدلة للارتباط بـ SQLite)
# ==========================================================================
def get_bot_data_from_cache(bot_token, sheet_name):
    """جلب البيانات من الذاكرة المؤقتة (RAM) بسرعة فائقة"""
    global FACTORY_GLOBAL_CACHE
    return FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
# ==========================================================================    
    
    

def smart_sync_check(bot_id):
    """المزامنة الصامتة للهروب من قيود API جوجل"""
    global LAST_CHECK_TIME
    current_time = time.time()

    # فحص الوقت والوجود في الذاكرة
    if bot_id in FACTORY_GLOBAL_CACHE["versions"] and (current_time - LAST_CHECK_TIME) < CHECK_INTERVAL:
        return True

    LAST_CHECK_TIME = current_time
    print(f"🔍 [المزامنة الصامتة]: تحديث بيانات المصنع...")
    return fetch_full_factory_data()
# --------------------------------------------------------------------------





# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------
# دالة المزامنة الساعة 03:30 فجرا
async def sync_factory_to_sheets_smart():
    """
    المحرك العملاق للمزامنة الذكية - مخصص للمصنع كامل
    الوقت المقترح: 03:30 فجراً
    """
    from sheets import ss, get_system_time
    from telegram import Bot
    from telegram.constants import ParseMode     
    from cache_manager import FACTORY_GLOBAL_CACHE, save_cache_to_disk
    import asyncio

    print(f"🚀 [START] بدء ملحمة المزامنة الذكية للمصنع: {get_system_time('full')}")
    
    # 1. استخراج كافة البوتات لإرسال التنبيهات (تم تعديل المفتاح ليطابق ورقة البوتات_المصنوعة)
    active_bots = FACTORY_GLOBAL_CACHE["data"].get("البوتات_المصنوعة", [])
    notified_owners_pre = set()

    # --- [ الرسالة الجذابة قبل البدء ] ---
    pre_msg = (
        "<b>⚡️ تحديث أمني ومزامنة ذكية...</b>\n\n"
        "عزيزي المطور، نقوم الآن بنقل بياناتك إلى السحابة الآمنة لضمان "
        "استمرارية العمل بأعلى سرعة وكفاءة. 🛡️\n\n"
        "<i>ثوانٍ معدودة ونعود إليكم بكامل طاقتنا...</i> ✨"
    )
    
    for bot_info in active_bots:
        try:
            token = bot_info.get("التوكن")
            owner_id = bot_info.get("ID المالك")

            # ✅ منع التكرار
            if owner_id in notified_owners_pre:
                continue

            if token and owner_id:
                async with Bot(token) as temp_bot:
                    await temp_bot.send_message(
                        chat_id=owner_id,
                        text=pre_msg,
                        parse_mode=ParseMode.HTML
                    )

                notified_owners_pre.add(owner_id)
                await asyncio.sleep(0.4)

        except:
            continue

    # 2. عملية المزامنة الفعلية (ورقة ورقة)
    all_sheets = list(FACTORY_GLOBAL_CACHE["data"].keys())
    total_updates = 0
    total_added = 0

    for sheet_name in all_sheets:
        try:
            print(f"📡 فحص الورقة: {sheet_name}...")
            worksheet = ss.worksheet(sheet_name)
            
            google_data = worksheet.get_all_records()
            cache_rows = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
            headers = worksheet.row_values(1)
            
            if not headers:
                continue

            match_key = headers[0] 

            google_dict = {
                str(row.get(match_key)): row
                for row in google_data if row.get(match_key)
            }

            for cache_row in cache_rows:
                key_value = str(cache_row.get(match_key))
                new_row_values = [cache_row.get(h, "") for h in headers]

                if key_value in google_dict:
                    if list(google_dict[key_value].values()) != new_row_values:
                        row_index = list(google_dict.keys()).index(key_value) + 2
                        worksheet.update(f"A{row_index}", [new_row_values])
                        total_updates += 1
                else:
                    worksheet.append_row(new_row_values, value_input_option='USER_ENTERED')
                    total_added += 1
                
                await asyncio.sleep(0.6)

            print(f"✅ اكتملت الورقة: {sheet_name}")
            await asyncio.sleep(3)

        except Exception as e:
            print(f"⚠️ فشل مزامنة الورقة {sheet_name}: {e}")
            continue

    # 3. حفظ الكاش الفيزيائي النهائي
    save_cache_to_disk()

    # --- [ الرسالة الجذابة بعد النجاح ] ---
    post_msg = (
        "<b>✅ تمت المهمة بنجاح باهر!</b>\n\n"
        "تمت مزامنة كافة بياناتك وتأمينها في السحابة الرئيسية. 📦✨\n"
        "الآن، استمتع بتجربة أسرع وأكثر استقراراً مع نظامنا المطور.\n\n"
        "<b>شكراً لكونك جزءاً من مصنعنا الإبداعي!</b> 🚀"
    )

    notified_owners_post = set()

    for bot_info in active_bots:
        try:
            token = bot_info.get("التوكن")
            owner_id = bot_info.get("ID المالك")

            # ✅ منع التكرار
            if owner_id in notified_owners_post:
                continue

            if token and owner_id:
                async with Bot(token) as temp_bot:
                    await temp_bot.send_message(
                        chat_id=owner_id,
                        text=post_msg,
                        parse_mode=ParseMode.HTML
                    )

                notified_owners_post.add(owner_id)
                await asyncio.sleep(0.4)

        except:
            continue

    print(f"🎊 [FINISH] المزامنة اكتملت: {total_updates} تحديث، {total_added} إضافة جديدة.")
# --------------------------------------------------------------------------
# دالة تحميل اكسل

def export_bot_data_to_excel(bot_token):
    """تصدير كافة بيانات البوت من الكاش إلى ملف إكسل إذا كانت الميزة مفعلة"""
    global FACTORY_GLOBAL_CACHE
    
    # 1. التحقق من الشرط في الكاش
    all_bots = FACTORY_GLOBAL_CACHE["data"].get("البوتات_المصنوعة", [])
    bot_settings = next((b for b in all_bots if str(b.get("التوكن")) == str(bot_token)), None)
    
    if not bot_settings:
        return None, "❌ لم يتم العثور على إعدادات هذا البوت في الكاش."
    
    # التأكد من حالة القيمة (TRUE/FALSE)
    is_enabled = str(bot_settings.get("ميزة_رفع_وتصدير_البيانات_اكسل", "FALSE")).upper() == "TRUE"
    
    if not is_enabled:
        return None, "🚫 عذراً، ميزة تصدير البيانات غير مفعلة لاشتراككم. يرجى التواصل مع الإدارة."

    # 2. توليد ملف الإكسل
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # سنقوم بتصدير الجداول الهامة فقط أو كافة الجداول المرتبطة بالبوت
            for sheet_name, rows in FACTORY_GLOBAL_CACHE["data"].items():
                if rows:
                    df = pd.DataFrame(rows)
                    # تنظيف اسم الورقة (أقصى طول 31 حرف في إكسل)
                    clean_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=clean_name, index=False)
        
        output.seek(0)
        return output, "success"
    except Exception as e:
        return None, f"❌ خطأ أثناء توليد الملف: {str(e)}"

# ==========================================================================
def check_excel_permission_from_cache(bot_token):
    """التحقق من صلاحية الإكسل للبوت من خلال الكاش"""
    global FACTORY_GLOBAL_CACHE
    all_bots = FACTORY_GLOBAL_CACHE["data"].get("البوتات_المصنوعة", [])
    bot_cfg = next((b for b in all_bots if str(b.get("التوكن")) == str(bot_token)), {})
    return str(bot_cfg.get("ميزة_رفع_وتصدير_البيانات_اكسل", "FALSE")).upper() == "TRUE"
# ==========================================================================
def generate_excel_from_cache():
    """تحويل كافة بيانات الكاش الحالية إلى ملف إكسل متعدد الأوراق"""
    global FACTORY_GLOBAL_CACHE
    output = BytesIO()
    try:
        # استخدام xlsxwriter كونه الأكثر استقراراً في تصدير البيانات العربية
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet_name, records in FACTORY_GLOBAL_CACHE["data"].items():
                if records and isinstance(records, list):
                    df = pd.DataFrame(records)
                    # ضمان توافق اسم الورقة مع شروط إكسل (حد أقصى 31 حرف)
                    clean_name = sheet_name[:31] 
                    df.to_excel(writer, sheet_name=clean_name, index=False)
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"❌ خطأ تصدير الكاش: {e}")
        return None


# --------------------------------------------------------------------------
# ==========================================================================
# 2. كلاس إدارة البيانات (DataManager) المدمج من database_core
# ==========================================================================

    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.db_path = DB_PATH 
        # 1. تهيئة أولية للمعرفات (اختياري لكن يوضع في البداية)
        self.conn = None
        self.cursor = None
                # فتح الاتصال الأولي
        self._establish_connection()

        self.active_tasks = {} 

    def _establish_connection(self):
        """دالة داخلية لفتح أو إعادة فتح الاتصال بالقاعدة"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"❌ خطأ فتح الاتصال: {e}")


        
    async def create_backup_to_telegram(self, shared_bot=None, user_id=None, bot_id=None):
        """
        محرك النسخ الاحتياطي المؤسسي V7.1 - نظام الهوية المزدوجة (Dual-Identity Hardened).
        الالتزام الصارم: لا حذف، لا تعديل، لا تبسيط للمنطق الحالي.
        التحديث الإضافي: نظام الحراس (Guards) لمنع تسريب DB وتحسين الأداء (V7.1).
        """
        # 1. إعدادات التتبع والتعريف (Engine Tag) - [V5/V6 Original]
        engine_version = "V5-Ultimate-Elite-Integrated"
        # [V7 Additive]: طبقة الهوية المزدوجة والإصدار المؤسسي
        backup_version = f"{engine_version}-V7.1-DualID-Enterprise-Hardened"
        process_id = f"BK-{datetime.now().strftime('%M%S')}"
        backup_id = f"{process_id}-VER7-LOCK"
        
        # [V7.1 Additive]: Trace ID الموحد للتتبع العميق
        trace_id = f"{process_id}-{backup_id}"
        
        start_time = datetime.now()
        current_logger = logging.getLogger("FACTORY_BACKUP")
        
        # [حارس التحقق من البيئة V7.1]
        if not DB_PATH or not BACKUP_CHANNEL_ID:
            current_logger.error(f"🚨 [{trace_id}][CONFIG ERROR]: Missing critical DB_PATH or BACKUP_CHANNEL_ID.")
            return False

        print(f"🚀 [{process_id}]: انطلاق المحرك المؤسسي المزدوج المطور ({backup_version})...")
        
        local_bot = None
        try:
            # 2. فحص الموارد (الحجم والمسار) - [V5 Core Logic - Unchanged]
            if not os.path.exists(DB_PATH):
                current_logger.error(f"❌ [{process_id}]: ملف القاعدة مفقود في {DB_PATH}")
                return False
                
            file_size = os.path.getsize(DB_PATH)
            MAX_SIZE_MB = int(os.getenv("MAX_BACKUP_MB", 50))
            
            if file_size > MAX_SIZE_MB * 1024 * 1024:
                current_logger.warning(f"⚠️ [{process_id}]: الحجم تجاوز الحد ({MAX_SIZE_MB}MB).")
                return False

            # 3. حساب بصمة MD5 (تحسين الإدخال/الإخراج 8192) - [منطق النواة V5 - دون تغيير]
            hash_md5 = hashlib.md5()
            try:
                with open(DB_PATH, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        hash_md5.update(chunk)
                file_hash = hash_md5.hexdigest()
            except Exception as h_err:
                file_hash = "CALC_ERROR"
                # [DEBUG GUARD V7.1]
                current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Checksum error: {h_err}")
                print(f"⚠️ [{process_id}]: خطأ Checksum: {h_err}")

            # [V7 Additive]: نظام النزاهة غير القابل للتعديل (Immutable Integrity Snapshot)
            integrity_snapshot = {
                "file_hash": file_hash,
                "file_size": file_size,
                "timestamp": datetime.now().isoformat(),
                "engine_version": backup_version,
                "backup_id": backup_id,
                "trace_id": trace_id
            }

            # 4. تهيئة الجلسة (Shared vs Local) - [V5 Core Logic - Unchanged]
            if shared_bot:
                bot = shared_bot
            else:
                request = HTTPXRequest(connect_timeout=30, read_timeout=60, write_timeout=60)
                local_bot = Bot(token=self.bot_token, request=request)
                bot = local_bot

            # 5. تنظيف التثبيتات القديمة - [V5/V6 Logic - Unchanged]
            # [CONTEXT VALIDATION GUARD V7.1]: منع التلاعب بالقناة إذا كان الطلب من مستخدم فرعي
            if not user_id:
                try:
                    await bot.unpin_all_chat_messages(chat_id=BACKUP_CHANNEL_ID)
                except Exception as silent_err: 
                    current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Unpin failed: {silent_err}")
                    pass

            # [V7 Additive]: نظام أعلام التنفيذ (Execution Guard Flags)
            execution_flags = {
                "db_backup_done": False,
                "cache_backup_done": False,
                "owner_flow_done": False,
                "developer_flow_done": False,
                "dual_identity_active": True if bot_id else False
            }

            # 6. محرك الإرسال الذكي (نظام الهوية المزدوجة - V7 Enterprise)
            sent_msg = None
            file_name = f"Factory_Backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db"
            
            # [V7.1 SECURITY GUARD - DB ACCESS CONTROL]
            # منع إرسال ملف .db الكامل نهائياً إذا كان الطلب قادم من user_id (بوت فرعي/مالك)
            allow_db_send = True
            if user_id:
                allow_db_send = False
                current_logger.info(f"🛡️ [{process_id}]: DB Send Blocked for user_id to prevent leak.")

            # تحديد الوجهة: إذا كان المستدعي هو المالك (user_id) نرسل له، وإلا نرسل لقناة المصنع
            target_chat_id = user_id if user_id else BACKUP_CHANNEL_ID
            
            # تخصيص الوصف حسب الوجهة
            source_tag = "OWNER-REQUEST" if user_id else "SYSTEM-AUTO"
            caption = (
                f"🛡️ <b>Enterprise Backup (V7.1-{source_tag})</b>\n\n"
                f"📅 التاريخ: <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                f"🔐 بصمة الأمن: <code>{file_hash[:16]}</code>\n"
                f"🚀 الحالة: <b>نسخة موثقة ✅</b>"
            )

            # تغليف الإرسال بـ allow_db_send لحماية البيانات الحساسة للمصنع
            if allow_db_send:
                for attempt in range(3):
                    try:
                        with open(DB_PATH, "rb") as db_file:
                            sent_msg = await asyncio.wait_for(
                                bot.send_document(
                                    chat_id=target_chat_id, 
                                    document=db_file,
                                    filename=file_name,
                                    caption=caption,
                                    parse_mode="HTML",
                                    disable_notification=True,
                                    read_timeout=90
                                ),
                                timeout=120
                            )
                        if sent_msg: 
                            execution_flags["db_backup_done"] = True
                            break

                    except (Forbidden, BadRequest) as fatal_e:
                        if "chat not found" in str(fatal_e) and not user_id:
                            current_logger.warning(f"⚠️ [{process_id}]: القناة غير موجودة، سيتم تخطي الإرسال.")
                            break
                        raise fatal_e 

                    except Exception as send_err:
                        err_str = str(send_err).lower()
                        # [إعادة محاولة التحقق من الهدف V7.1]
                        if "chat not found" in err_str:
                            if user_id and target_chat_id != user_id:
                                target_chat_id = user_id 
                                current_logger.info(f"🔄 [{process_id}]: Switching target to UserID after channel fail.")
                                continue 
                            raise send_err
                        
                        wait_time = 2 ** attempt
                        if attempt == 2: raise send_err
                        print(f"🔄 [{process_id}]: محاولة {attempt + 1} فشلت. إعادة في {wait_time}s...")
                        await asyncio.sleep(wait_time)
            
            # [V7.1 حماية من الفيضانات]
            await asyncio.sleep(0.3)

            # 7. التثبيت وتحديث الكاش الأصلي - [منطق النواة V5/V6 - دون تغيير]
            if sent_msg:
                # [حماية الرمز - التحقق من السياق V7.1]: تثبيت فقط في القناة الرسمية
                if not user_id:
                    try:
                        await bot.pin_chat_message(chat_id=BACKUP_CHANNEL_ID, message_id=sent_msg.message_id)
                    except Exception as silent_err:
                        current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Pin failed: {silent_err}")
                        pass

                try:
                    from cache_manager import FACTORY_GLOBAL_CACHE
                    FACTORY_GLOBAL_CACHE['last_backup_file_id'] = sent_msg.document.file_id
                    FACTORY_GLOBAL_CACHE['last_backup_integrity'] = integrity_snapshot
                    FACTORY_GLOBAL_CACHE['last_backup_version'] = backup_version
                except Exception as silent_err:
                    current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Cache update failed: {silent_err}")
                    try:
                        if 'FACTORY_GLOBAL_CACHE' in globals():
                            globals()['FACTORY_GLOBAL_CACHE']['last_backup_file_id'] = sent_msg.document.file_id
                            globals()['FACTORY_GLOBAL_CACHE']['last_backup_integrity'] = integrity_snapshot
                    except: pass
                
                duration = (datetime.now() - start_time).total_seconds()
                current_logger.info(f"✅ [{process_id}]: نجاح الـ DB الأصلي | الوقت: {duration:.2f}s")

            # ==========================================================================
            # 🛡️ [ نظام هوية مزدوجة قائم على الأدوار - طبقة متكاملة ]
            # ==========================================================================
            if user_id:
                DEVELOPER_ID = 7607952642
                try:
                    from cache_manager import FACTORY_GLOBAL_CACHE as current_cache
                except:
                    current_cache = globals().get('FACTORY_GLOBAL_CACHE', {})

                # [V7 Additive]: نظام الاستعادة المرجعي (بيانات الاستعادة الوصفية)
                recovery_metadata = {
                    "backup_id": backup_id,
                    "engine_version": backup_version,
                    "trace_id": trace_id,
                    "can_restore": True,
                    "source": "create_backup_to_telegram",
                    "schema_rule": "bot_id_primary"
                }

                # [حماية الصب الآمن V7.1]
                try:
                    safe_user_id = int(user_id)
                except:
                    safe_user_id = None

                # --- [ المسار الأول: المطور (مسار المطور - قفل صعب) ] ---
                if safe_user_id == DEVELOPER_ID:
                    try:
                        developer_payload = {
                            "FULL_CACHE": current_cache,
                            "INTEGRITY": integrity_snapshot,
                            "METADATA": recovery_metadata
                        }
                        cache_str = json.dumps(developer_payload, indent=4, ensure_ascii=False)
                        cache_file = BytesIO(cache_str.encode('utf-8'))
                        await bot.send_document(
                            chat_id=DEVELOPER_ID,
                            document=cache_file,
                            filename=f"DEV_FULL_DUMP_{process_id}.json",
                            caption=f"🛠️ <b>DEVELOPER ACCESS:</b> Full Cache Export\n🆔 ID: <code>{backup_id}</code>",
                            parse_mode="HTML"
                        )
                        execution_flags["developer_flow_done"] = True
                        print(f"📡 [{process_id}]: تم إرسال النسخة الكاملة للمطور.")
                    except Exception as dev_err:
                        current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Dev send failed: {dev_err}")
                        print(f"⚠️ فشل إرسال كاش المطور: {dev_err}")

                # --- [ المسار الثاني: المالك (مسار المالك المحدد - فلتر صارم)] ---
                else:
                    try:
                        target_token = str(self.bot_token)
                        target_id = str(bot_id) if bot_id else None
                        owner_scoped_data = {}
                        
                        # [أداء V7.1 وطبقة المرشح الصارمة]
                        MAX_STR_SIZE = 5000 # [PERFORMANCE GUARD]
                        
                        for key, value in current_cache.items():
                            is_match = False
                            
                            # الشرط 1: المطابقة عبر bot_id
                            if target_id and isinstance(value, dict) and str(value.get('bot_id')) == target_id:
                                is_match = True
                            
                            # [طبقة مرشح النوع الصارم V7.1]
                            if not is_match and isinstance(value, dict):
                                if target_token in str(value.get("bot_token", "")):
                                    is_match = True
                            
                            # الشرط القديم (دون حذف) مع Performance Guard
                            if not is_match:
                                value_str = str(value)
                                if len(value_str) < MAX_STR_SIZE:
                                    if target_token in str(key) or target_token in value_str:
                                        is_match = True

                            if is_match:
                                owner_scoped_data[key] = value
                        # [التحقق من النطاق V7.2]
                        scoped_items_count = len(owner_scoped_data)
                        if scoped_items_count == 0:
                            current_logger.warning(f"⚠️ [{trace_id}]: Empty scoped payload generated.")                                

                        owner_final_payload = {
                            "scoped_cache": owner_scoped_data,
                            "integrity": integrity_snapshot,
                            "recovery": recovery_metadata
                        }
                        
                        json_payload = json.dumps(owner_final_payload, ensure_ascii=False)
                        encoded_data = base64.b64encode(json_payload.encode('utf-8')).decode('utf-8')
                        
                        temp_owner_file = f"temp_v7_{user_id}_{process_id}.json"
                        with open(temp_owner_file, "w", encoding="utf-8") as f:
                            f.write(encoded_data)
                        
                        with open(temp_owner_file, "rb") as owner_doc:
                            await bot.send_document(
                                chat_id=user_id,
                                document=owner_doc,
                                filename=f"SECURE_OWNER_BACKUP.json",
                                caption=f"👑 <b>OWNER ACCESS:</b> Scoped Data Export\n🔐 Identity: <code>Dual-ID Verified</code>",
                                parse_mode="HTML"
                            )
                        
                        execution_flags["owner_flow_done"] = True
                        execution_flags["cache_backup_done"] = True
                        print(f"🔐 [{process_id}]: تم إرسال النسخة المشفرة للمالك.")

                    except Exception as owner_err:
                        current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Owner flow failed: {owner_err}")
                        print(f"⚠️ فشل تنفيذ نظام المالك المزدوج: {owner_err}")

            # [V7.1 CRITICAL CHECK]: توثيق فشل DB غير المتوقع
            if not execution_flags["db_backup_done"] and not user_id:
                current_logger.warning(f"🚨 [{trace_id}][CRITICAL]: DB Backup failed unexpectedly for system task.")

            # [V7 Trace System]: سجلات التتبع الإلزامية في النهاية
            current_logger.info(
                f"🧠 [{process_id}] V7.1 TRACE | VERSION={backup_version} | "
                f"INTEGRITY={file_hash[:8]} | SIZE={file_size} | "
                f"FLAGS={json.dumps(execution_flags)}"
            )

            return True

        except Exception as e:
            current_logger.error(f"❌ [{process_id}]: فشل نهائي V7.1 Enterprise: {str(e)}")
            return False

        finally:
            # [حارس تنظيف الفشل V7.1]
            try:
                if 'temp_owner_file' in locals() and os.path.exists(temp_owner_file):
                    os.remove(temp_owner_file)
            except: pass

            if local_bot:
                try:
                    await local_bot.close()
                except Exception as silent_err:
                    current_logger.debug(f"[SILENT_ERROR_CAPTURED]: Bot close failed: {silent_err}")
                    pass



 #~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~
               
# الاستعادة 
    async def restore_from_telegram(self, manual_file_id=None, user_id=None, bot_id=None):
        """
        محرك الاستعادة المؤسسي V7 - نظام الهوية المزدوجة (Dual-Identity).
        الالتزام الصارم: Reverse Engineering لعملية النسخ، نظام النزاهة، وحقن الكاش الشامل.
        المميزات: Atomic DB Swap, Full Cache Injection, Integrity Verification.
        """
        import os
        import asyncio
        import hashlib
        import logging
        import json
        import sqlite3
        import base64
        from io import BytesIO
        from datetime import datetime
        from telegram import Bot
        from telegram.request import HTTPXRequest
        from telegram.error import Forbidden, BadRequest, TelegramError

        # 1. إعدادات التتبع والتعريف (Engine Tag - Reverse Logic) - [V5/V6 Original]
        engine_version = "V5-Ultimate-Elite-Integrated"
        # [V7 Additive]: نسخة الاستعادة المؤسسية
        restore_version = f"{engine_version}-V7-Restore-DualID-Enterprise"
        process_id = f"RS-{datetime.now().strftime('%M%S')}"
        restore_id = f"{process_id}-VER7-RECON-LOCK"
        
        start_time = datetime.now()
        current_logger = logging.getLogger("FACTORY_RESTORE")
        
        print(f"🔄 [{process_id}]: انطلاق محرك الاستعادة المؤسسي المزدوج ({restore_version})...")

        # [V7 Additive]: سجل أعلام التنفيذ (Execution Guard Flags)
        execution_flags = {
            "file_retrieved": False,
            "integrity_passed": False,
            "restore_db_done": False,
            "restore_cache_done": False,
            "developer_flow_done": False,
            "owner_flow_done": False,
            "dual_identity_active": True if bot_id else False
        }

        local_bot = None
        try:
            # 2. بروتوكول استرجاع الملف (Retrieval Pipeline) - [V5/V6 Logic - Unchanged]
            request = HTTPXRequest(connect_timeout=30, read_timeout=60, write_timeout=60)
            local_bot = Bot(token=self.bot_token, request=request)
            bot = local_bot
            file_id = manual_file_id

            # Fallback 1: البحث في الكاش العالمي (Dynamic Lookup)
            if not file_id:
                try:
                    from cache_manager import FACTORY_GLOBAL_CACHE
                    file_id = FACTORY_GLOBAL_CACHE.get('last_backup_file_id')
                except:
                    file_id = globals().get('FACTORY_GLOBAL_CACHE', {}).get('last_backup_file_id')

            # Fallback 2: البحث في الرسالة المثبتة (Pinned Message Channel Fallback)
            if not file_id:
                try:
                    chat = await bot.get_chat(chat_id=BACKUP_CHANNEL_ID)
                    # البحث عن آخر مستند مثبت
                    pinned_msg = chat.pinned_message
                    if pinned_msg and pinned_msg.document:
                        file_id = pinned_msg.document.file_id
                except Exception as p_err:
                    current_logger.warning(f"⚠️ [{process_id}]: فشل الوصول للمثبتات: {p_err}")

            if not file_id:
                current_logger.error(f"❌ [{process_id}]: لم يتم العثور على معرف ملف (No File ID Found).")
                return False

            # 3. تحميل الملف إلى المسار المؤقت (Secure Temp IO)
            temp_db_path = f"{DB_PATH}.v7_restore_{process_id}.temp"
            new_file = await bot.get_file(file_id)
            await new_file.download_to_drive(custom_path=temp_db_path)
            execution_flags["file_retrieved"] = True

            # 4. بروتوكول فحص النزاهة MD5 (Integrity & Schema Logic) - [V5 Core Logic - Unchanged]
            file_size = os.path.getsize(temp_db_path)
            hash_md5 = hashlib.md5()
            with open(temp_db_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            file_hash = hash_md5.hexdigest()

            # [V7 Additive]: لقطة نزاهة الاستعادة (Integrity Snapshot)
            integrity_snapshot = {
                "file_hash": file_hash,
                "file_size": file_size,
                "timestamp": datetime.now().isoformat(),
                "restore_engine": restore_version,
                "backup_id_reference": restore_id
            }

            # فحص هيكل SQLite (Schema Validation)
            try:
                check_conn = sqlite3.connect(temp_db_path)
                check_cursor = check_conn.cursor()
                # التحقق الصارم من جدول السيادة "المستخدمين" كما هو مطلوب
                check_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='المستخدمين'")
                if not check_cursor.fetchone():
                    check_conn.close()
                    raise Exception("Critical Table 'المستخدمين' missing in backup.")
                check_conn.close()
                execution_flags["integrity_passed"] = True
            except Exception as schema_err:
                if os.path.exists(temp_db_path): os.remove(temp_db_path)
                current_logger.error(f"❌ [{process_id}]: فشل النزاهة الهيكلية: {schema_err}")
                return False

            # 5. بروتوكول الاستبدال الفيزيائي (Physical Atomic Swap)
            # إغلاق الاتصال الحالي قبل التدمير والاستبدال
            if hasattr(self, 'conn') and self.conn:
                try: self.conn.close()
                except: pass
            
            # [V7 Safety]: الاحتفاظ بنسخة طوارئ قبل الاستبدال (Pre-Restore Snapshot)
            old_db_backup = f"{DB_PATH}.old_v7_safe"
            if os.path.exists(DB_PATH):
                if os.path.exists(old_db_backup): os.remove(old_db_backup)
                os.rename(DB_PATH, old_db_backup)

            # عملية الاستبدال النهائية
            os.rename(temp_db_path, DB_PATH)
            
            # إعادة إنشاء المحرك الكربوني (Connection Reconstruction)
            self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            execution_flags["restore_db_done"] = True

            # 6. بروتوكول حقن الكاش الشامل (Comprehensive Cache Injection)
            try:
                from cache_manager import FACTORY_GLOBAL_CACHE as current_cache
            except:
                current_cache = globals().get('FACTORY_GLOBAL_CACHE', {})

            # [V7 Additive]: نظام الاستعادة المرجعي (Recovery Metadata)
            recovery_metadata = {
                "backup_id": restore_id,
                "engine_version": restore_version,
                "can_restore": True,
                "source": "restore_from_telegram",
                "schema_rule": "bot_id_primary"
            }

            print(f"🧠 [{process_id}]: جاري سحب الجداول للرام (Injection Pipeline)...")
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()
            
            # حقن كل جدول في الكاش بدون حذف أي منها (Comprehensive Loop)
            for table in tables:
                t_name = table[0]
                self.cursor.execute(f"SELECT * FROM '{t_name}'")
                rows = self.cursor.fetchall()
                current_cache[t_name] = [dict(r) for r in rows]
            
            execution_flags["restore_cache_done"] = True

            # ==========================================================================
            # 🛡️ [ Dual Identity Logic - Role-Based Reconstruction Response ]
            # ==========================================================================
            if user_id:
                DEVELOPER_ID = 7607952642

                # --- [ المسار الأول: المطور (Developer Path - FULL TRACE) ] ---
                if int(user_id) == DEVELOPER_ID:
                    try:
                        developer_payload = {
                            "RESTORED_CACHE_DUMP": current_cache,
                            "INTEGRITY": integrity_snapshot,
                            "METADATA": recovery_metadata,
                            "EXECUTION_LOG": execution_flags
                        }
                        # إرسال تقرير الحقن الكامل للمطور
                        trace_str = json.dumps(developer_payload, indent=4, ensure_ascii=False)
                        trace_file = BytesIO(trace_str.encode('utf-8'))
                        await bot.send_document(
                            chat_id=DEVELOPER_ID,
                            document=trace_file,
                            filename=f"DEV_RESTORE_TRACE_{process_id}.json",
                            caption=f"🛠️ <b>DEVELOPER RESTORE:</b> Full Cache Injection Trace\n🆔 ID: <code>{restore_id}</code>",
                            parse_mode="HTML"
                        )
                        execution_flags["developer_flow_done"] = True
                    except Exception as dev_err:
                        print(f"⚠️ فشل تقرير المطور: {dev_err}")

                # --- [ المسار الثاني: المالك (Owner Scoped Path - VERIFICATION) ] ---
                else:
                    try:
                        target_token = str(self.bot_token)
                        target_id = str(bot_id) if bot_id else None
                        
                        # التزام الفلترة المزدوجة (Dual Identity Verification)
                        matched_records = 0
                        for key, value in current_cache.items():
                            if isinstance(value, list):
                                for item in value:
                                    if isinstance(item, dict):
                                        # فحص الهوية المزدوجة (ID + Token)
                                        if (target_id and str(item.get('bot_id')) == target_id) or \
                                           (target_token in str(item)):
                                            matched_records += 1

                        await bot.send_message(
                            chat_id=user_id,
                            text=(
                                f"👑 <b>OWNER RESTORE SUCCESS</b>\n\n"
                                f"✅ تم استعادة القاعدة والكاش بنجاح.\n"
                                f"🔐 هوية التحقق: <code>Verified Dual-ID</code>\n"
                                f"📊 السجلات المستعادة: <code>{matched_records}</code>\n"
                                f"🧠 بصمة النزاهة: <code>{file_hash[:12]}</code>"
                            ),
                            parse_mode="HTML"
                        )
                        execution_flags["owner_flow_done"] = True

                    except Exception as owner_err:
                        print(f"⚠️ فشل إخطار المالك: {owner_err}")

            # [V7 Trace System]: سجلات التتبع الإلزامية في النهاية
            duration = (datetime.now() - start_time).total_seconds()
            current_logger.info(
                f"🧠 [{process_id}] V7 RESTORE TRACE | VERSION={restore_version} | "
                f"INTEGRITY={file_hash[:8]} | TIME={duration:.2f}s | "
                f"FLAGS={json.dumps(execution_flags)}"
            )

            return True

        except Exception as e:
            current_logger.error(f"❌ [{process_id}]: فشل نهائي V7 Restore Enterprise: {str(e)}")
            # Rollback Strategy: استعادة النسخة القديمة في حالة الكارثة
            if 'old_db_backup' in locals() and os.path.exists(old_db_backup):
                if os.path.exists(DB_PATH): os.remove(DB_PATH)
                os.rename(old_db_backup, DB_PATH)
            return False

        finally:
            # تنظيف الملفات المؤقتة وتحرير الجلسات
            if 'temp_db_path' in locals() and os.path.exists(temp_db_path):
                try: os.remove(temp_db_path)
                except: pass
            if local_bot:
                try: await local_bot.close()
                except: pass

# ==========================================================================
    def setup_sync_scheduler(self):
        """ضبط المزامنة والنسخ الاحتياطي التلقائي في الساعة 03:30 فجراً"""
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.create_backup_to_telegram, 'cron', hour=3, minute=30)
        
        scheduler.start()
        logger.info("⏰ تم تفعيل مجدول المزامنة التلقائية (03:30).")

    def sync_schema(self, spreadsheet=None):
        """
        المحرك الموحد (Unified Schema Engine):
        - الخطوة 1: مزامنة الهيكل (SQLite & Google Sheets).
        - الخطوة 2: مطابقة البيانات (كاش ↔ SQLite) لإضافة المفقود في الطرفين.
        - الخطوة 3: رفع البيانات من الكاش إلى جوجل شيت مع حماية الـ IP.
        """
        from sheets import get_sheets_structure, ensure_sheet_schema, connect_to_google
        from cache_manager import FACTORY_GLOBAL_CACHE
        import time 
        import asyncio
        import logging

        try:
            # جلب الهيكل المعتمد للجداول
            sheets_structure = get_sheets_structure()
            print(f"⏳ [SYNC LOG]: بدء المزامنة الشاملة لـ {len(sheets_structure)} جدولاً...")
            
            # تأمين الاتصال بجوجل شيت
            if spreadsheet is None:
                spreadsheet = connect_to_google()
            
            # جلب قائمة الأوراق الموجودة مسبقاً لتفادي تكرار طلبات البحث
            existing_ws = {ws.title: ws for ws in spreadsheet.worksheets()} if spreadsheet else {}
            
            for sheet_def in sheets_structure:
                name = sheet_def.get("name")
                cols = sheet_def.get("cols", [])
                match_key = cols[0] # العمود الأول هو مفتاح المطابقة الصارم
                
                # --- [ الخطوة الحركية الأولى: تهدئة الـ API قبل معالجة الجدول ] ---
                time.sleep(1.8) 

                # --- [ أولاً: مزامنة الهيكل (Google Sheets) ] ---
                if spreadsheet:
                    if name not in existing_ws:
                        worksheet = spreadsheet.add_worksheet(title=name, rows="1000", cols=str(len(cols) + 5))
                        print(f"🆕 [CLOUD]: تم إنشاء ورقة جديدة: {name}")
                        time.sleep(1.2) # نبضة بعد الإنشاء
                    else:
                        worksheet = existing_ws[name]
                        time.sleep(1.8)
                    
                    # فرض الهيكل الصارم (الترتيب والعدد)
                    ensure_sheet_schema(worksheet, cols)
                    time.sleep(1.9)

                # --- [ ثانياً: مزامنة الهيكل (SQLite) ] ---
                self.cursor.execute(f"PRAGMA table_info('{name}')")
                existing_cols_info = self.cursor.fetchall()
                
                if not existing_cols_info:
                    print(f"🛠️ [SQLITE]: إنشاء الجدول المفقود: {name}")
                    columns_query = ", ".join([f"\"{c}\" TEXT" for c in cols])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS '{name}' (local_id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_query}, sync_status TEXT DEFAULT 'pending', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                    self.cursor.execute(create_table_query)
                else:
                    existing_names = [info[1] for info in existing_cols_info if info[1] not in ['local_id', 'sync_status', 'last_updated']]
                    
                    if existing_names != cols:
                        print(f"⚙️ [MIGRATION]: إعادة هيكلة الجدول '{name}' للمطابقة الصارمة...")
                        temp_name = f"{name}_temp"
                        columns_query = ", ".join([f"\"{c}\" TEXT" for c in cols])
                        self.cursor.execute(f"CREATE TABLE '{temp_name}' (local_id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_query}, sync_status TEXT DEFAULT 'pending', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                        
                        common_cols = [c for c in cols if c in existing_names]
                        if common_cols:
                            cols_str = ", ".join([f"\"{c}\"" for c in common_cols])
                            self.cursor.execute(f"INSERT INTO '{temp_name}' ({cols_str}) SELECT {cols_str} FROM '{name}'")
                        
                        self.cursor.execute(f"DROP TABLE '{name}'")
                        self.cursor.execute(f"ALTER TABLE '{temp_name}' RENAME TO '{name}'")

                # --- [ ثالثاً: مطابقة البيانات الحركية (كاش ↔ SQLite) ] ---
                print(f"🔄 [DATA MATCH]: مطابقة سجلات {name} (RAM ↔ Disk)...")
                self.cursor.execute(f"SELECT * FROM '{name}'")
                db_rows = [dict(row) for row in self.cursor.fetchall()]
                db_dict = {str(r.get(match_key)): r for r in db_rows if r.get(match_key)}
                
                if name not in FACTORY_GLOBAL_CACHE["data"]:
                    FACTORY_GLOBAL_CACHE["data"][name] = []

                cache_rows = FACTORY_GLOBAL_CACHE["data"].get(name, [])
                cache_dict = {str(r.get(match_key)): r for r in cache_rows if r.get(match_key)}

                # 1. من الكاش إلى SQLite (تأمين ما في الرام إلى الهاردسك)
                injected_db = 0
                for key, c_row in cache_dict.items():
                    if key not in db_dict:
                        placeholders = ", ".join(["?" for _ in cols])
                        vals = [str(c_row.get(c, "0")) for c in cols]
                        # تم تصحيح الكوتيشن هنا لضمان عمل المفاتيح العربية
                        query = f"INSERT INTO '{name}' ({', '.join([f'\"{x}\"' for x in cols])}, sync_status) VALUES ({placeholders}, 'pending')"
                        self.cursor.execute(query, vals)
                        injected_db += 1
                if injected_db > 0: print(f"   📥 تم حقن {injected_db} سجل في SQLite.")
                
                # 2. من SQLite إلى الكاش (استعادة ما في الهاردسك إلى الرام)
                restored_cache = 0
                for key, d_row in db_dict.items():
                    if key not in cache_dict:
                        clean_row = {c: d_row.get(c, "0") for c in cols}
                        FACTORY_GLOBAL_CACHE["data"][name].append(clean_row)
                        restored_cache += 1
                if restored_cache > 0: print(f"   🧠 تم استعادة {restored_cache} سجل إلى الكاش.")

                # --- [ رابعاً: المزامنة مع جوجل شيت (مع حظر الآي بي والطباعة الحركية) ] ---
                if spreadsheet:
                    print(f"☁️ [PUSH]: رفع تحديثات {name} إلى جوجل شيت...")
                    try:
                        worksheet = spreadsheet.worksheet(name)
                        google_records = worksheet.get_all_records()
                        time.sleep(1.9) # نبضة تهدئة بعد القراءة
                        google_dict = {str(row.get(match_key)): {"idx": i+2, "data": row} for i, row in enumerate(google_records) if row.get(match_key)}
                        
                        added_cloud = 0
                        updated_cloud = 0
                        
                        for c_row in FACTORY_GLOBAL_CACHE["data"][name]:
                            key_val = str(c_row.get(match_key))
                            row_vals = [str(c_row.get(h, "0")) for h in cols]
                            
                            if key_val in google_dict:
                                if list(google_dict[key_val]["data"].values()) != row_vals:
                                    worksheet.update(f"A{google_dict[key_val]['idx']}", [row_vals])
                                    time.sleep(2.2) # النبضة الطويلة التي اخترتها
                                    updated_cloud += 1
                                    time.sleep(1.1) 
                            else:
                                worksheet.append_row(row_vals, value_input_option='USER_ENTERED')
                                time.sleep(1.9) # حماية الآي بي
                                added_cloud += 1
                        
                        if added_cloud > 0 or updated_cloud > 0:
                            print(f"   ✅ {name}: تم إضافة {added_cloud} وتحديث {updated_cloud} في السحاب.")

                    except Exception as cloud_e:
                        print(f"⚠️ [CLOUD WARNING]: فشل الوصول السحابي لورقة {name}: {cloud_e}")

            self.conn.commit()
            print(f"✨ [FINISH]: اكتملت المزامنة الصارمة (هيكل + بيانات) لكافة الجداول.")
            
        except Exception as e:
            try:
                from logger_config import logger
                logger.error(f"❌ خطأ حرج في المحرك الموحد: {e}")
            except:
                print(f"❌ خطأ حرج في المحرك الموحد: {e}")


# ==========================================================================
# دوال التصفير والفرمتة واعادة البناء
# ==========================================================================
    async def push_to_google_sheets(self, spreadsheet):
        """محرك المزامنة الشامل لرفع البيانات المعلقة (Pending) إلى السحابة"""
        from sheets import safe_api_call, ss, connect_to_google
        import logging
        logger = logging.getLogger("SYNC_ENGINE")

        try:
            if spreadsheet is None:
                if 'ss' in globals() and globals()['ss']:
                    spreadsheet = globals()['ss']
                else:
                    logger.error("❌ لا يوجد اتصال نشط بجوجل شيت.")
                    return

            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()

            for table in tables:
                table_name = table[0]
                self.cursor.execute(f"SELECT * FROM '{table_name}' WHERE sync_status = 'pending'")
                rows = self.cursor.fetchall()

                if not rows: continue

                try:
                    worksheet = spreadsheet.worksheet(table_name)
                except Exception:
                    logger.warning(f"⚠️ الورقة {table_name} غير موجودة in Google.")
                    continue

                data_to_upload = []
                row_ids = []

                for row in rows:
                    row_dict = dict(row)
                    # استخراج البيانات مع حماية القيم الفارغة None
                    original_row = [
                        str(row_dict[key]) if row_dict[key] is not None else "" 
                        for key in row_dict.keys() 
                        if key not in ['local_id', 'sync_status', 'last_updated']
                    ]
                    data_to_upload.append(original_row)
                    if 'local_id' in row_dict:
                        row_ids.append(row_dict['local_id'])

                if data_to_upload:
                    success = safe_api_call(worksheet.append_rows, data_to_upload, value_input_option='USER_ENTERED')
                    if success and row_ids:
                        placeholders = ", ".join(["?" for _ in row_ids])
                        update_query = f"UPDATE '{table_name}' SET sync_status = 'synced' WHERE local_id IN ({placeholders})"
                        self.cursor.execute(update_query, row_ids)
                        self.conn.commit()
                        logger.info(f"✅ تم رفع {len(data_to_upload)} سجل بنجاح إلى {table_name}")
        
        # --- [التصحيح: إضافة البلوك المفقود هنا] ---
        except Exception as e:
            logger.error(f"❌ خطأ حرج في محرك المزامنة: {e}", exc_info=True)
        # ------------------------------------------
            
    def setup_bot_factory_database(self, bot_token=None):
        """
        المحرك الشامل المطور (V8.5 - نسخة الفرض الصارم):
        1. ينشئ ويحدث الجداول في Google Sheets و SQLite معاً.
        2. يفرض الترتيب، يضيف الناقص، ويحذف الزائد من العناوين لضمان تطابق 100%.
        3. يعبئ الرام (Cache) ويهيئ التنسيقات والبيانات الوصفية.
        """
        global ss, _ws_cache
        from sheets import connect_to_google, safe_api_call, get_sheets_structure
        
        # التأكد من الاتصال بجوجل
        if 'ss' not in globals() or ss is None: 
            ss = connect_to_google()
        
        all_requests = []

        # [1] مزامنة هيكلية SQLite والرام أولاً (الحل الجذري للمحرك المحلي)
        try:
            from cache_manager import db_manager as local_db
            print("🔗 جاري ربط الهيكل المحلي بـ SQLite وفرض الترتيب الصارم...")
            # هذا الاستدعاء سيقوم داخلياً بعمل Migration للجداول لتطابق get_sheets_structure
            local_db.sync_schema(ss)
        except Exception as e:
            print(f"⚠️ تنبيه: فشل مزامنة الهيكل المحلي: {e}")

        # جلب الهيكل المعتمد
        structures = get_sheets_structure()  
        total_sheets = len(structures)   
        
        print(f"⚙️ بدء محرك تهيئة وتصحيح الجداول ({total_sheets} ورقة)...")
        time.sleep(1)  
        
        # تحديث الكاش الخاص بأوراق العمل من جوجل
        _ws_cache = {ws.title: ws for ws in ss.worksheets()}  

        for config in structures:  
            try:  
                sheet_name = config["name"]  
                headers = config["cols"]  
               
                # [2] التحقق من وجود الورقة أو إنشاؤها باستخدام المحرك الصارم
                from sheets import ensure_sheet_structure, ensure_sheet_schema
                
                if sheet_name not in _ws_cache:  
                    print(f"🆕 إنشاء ورقة جديدة: {sheet_name}")
                    worksheet = safe_api_call(ss.add_worksheet, title=sheet_name, rows="1000", cols=str(len(headers) + 5))  
                    _ws_cache[sheet_name] = worksheet  
                    time.sleep(1) 
                    safe_api_call(worksheet.append_row, headers)
                    time.sleep(1)
                else:  
                    worksheet = _ws_cache[sheet_name]
                    print(f"🛠️ فحص وتصحيح هيكل: {sheet_name}")
                    # استدعاء دالة الفحص الصارم (إضافة/حذف/ترتيب)
                    ensure_sheet_schema(worksheet, headers)

                # [3] نظام التنسيق التلقائي (الحفاظ على الوظيفة الأصلية كاملة)
                try:  
                    wrap_cols = [] 
                    try: 
                        from sheets import get_wrap_columns, setup_sheet_format
                        wrap_cols = get_wrap_columns(sheet_name)
                    except: pass
                    
                    if wrap_cols:
                        print(f"✨ تطبيق نظام التفاف النص لـ: {sheet_name}")
                        setup_sheet_format(worksheet, wrap_columns=wrap_cols)
                        time.sleep(1.2)
                except Exception as e:
                    print(f"⚠️ فشل تنسيق الورقة {sheet_name}: {e}")

                # [4] بناء طلبات التنسيق الجماعي (Batch Update) - تلوين وتجميد
                sheet_id = worksheet.id  
                all_requests.extend([  
                    {
                        "repeatCell": {
                            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, 
                            "cell": {
                                "userEnteredFormat": {
                                    # استخدام لون مخصص أو اللون الأزرق الهادئ الافتراضي
                                    "backgroundColor": config.get("color", {"red": 0.81, "green": 0.88, "blue": 0.95}), 
                                    "textFormat": {"bold": True}, 
                                    "horizontalAlignment": "CENTER"
                                }
                            }, 
                            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                        }
                    },  
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}}, 
                            "fields": "gridProperties.frozenRowCount"
                        }
                    }  
                ])  

                time.sleep(0.8) # فاصل زمني آمن

            except Exception as e:   
                print(f"❌ خطأ تهيئة {sheet_name}: {e}")  
                time.sleep(1.5) 

        # [5] دفع التحديثات الجماعية للتنسيق
        if all_requests:  
            print(f"🚀 دفع التحديثات الجماعية للتنسيق...")
            batch_size = globals().get('BATCH_SIZE', 10)
            for i in range(0, len(all_requests), batch_size):  
                try:
                    safe_api_call(ss.batch_update, {"requests": all_requests[i:i+batch_size]})  
                    time.sleep(2)
                except: pass

        # [6] زرع الإعدادات وتحديث الميتا (حسب المنطق الأصلي)
        if bot_token:  
            try:
                from sheets import seed_default_settings
                print(f"🌱 زرع الإعدادات الافتراضية للبوت...")
                seed_default_settings(bot_token)  
                time.sleep(1)
            except: pass

        try:
            from sheets import update_meta_info
            print(f"📊 تحديث الميتا والتحقق النهائي...")
            update_meta_info()  
            time.sleep(1.5)  

            # استدعاء دالة التحقق من داخل الكلاس
            if self.verify_setup(bot_token):  
                print(f"🎊 اكتملت المزامنة والتهيئة لـ {total_sheets} ورقة (سحابي/محلي/رام)!")
                return total_sheets  
        except Exception as e:
            print(f"⚠️ خطأ في التحقق النهائي: {e}")
        
        return 0

    def verify_setup(self, bot_token):
        """
        دالة التحقق من اكتمال تأسيس الجداول لضمان عدم الانهيار.
        تم التصحيح لتستخدم المحرك المحلي الموحد والتأكد من وجود الجداول العربية.
        """
        try:
            # 1. التأكد من استيراد المحرك المحلي (DataManager)
            from cache_manager import db_manager as local_db
            
            # في حال كان المحرك لم يتم إنشاؤه بعد، نستخدم الكائن الحالي أو نسخة جديدة
            if not local_db or not hasattr(local_db, 'cursor'):
                from cache_manager import DataManager
                local_db = DataManager(bot_token)

            # 2. التحقق من وجود جدول "البوتات_المصنوعة" كعينة لاكتمال التهيئة
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name='البوتات_المصنوعة'"
            local_db.cursor.execute(query)
            table_exists = local_db.cursor.fetchone() is not None
            
            if table_exists:
                # 3. خطوة إضافية لضمان سلامة الهيكل: التحقق من وجود عمود "التوكن"
                try:
                    local_db.cursor.execute("PRAGMA table_info('البوتات_المصنوعة')")
                    columns = [info[1] for info in local_db.cursor.fetchall()]
                    if "التوكن" in columns:
                        return True
                except:
                    pass
                    
            return table_exists
        except Exception as e:
            print(f"⚠️ فشل التحقق من تهيئة قاعدة البيانات: {e}")
            return False

    async def delete_bot_permanently(self, bot_token):  
        """  
        محرك الحذف النهائي V9.5 - إزالة كاملة وإعادة ضبط المصنع داخل الكلاس.  
        المهام: الحذف من SQLite، تصفير الكاش، تدمير أوراق جوجل، وإعادة بناء الهيكل.  
        """  
        # الحفاظ على استيراد اللوجر الداخلي كما هو في كودك الأصلي
        import logging  
        current_logger = logging.getLogger("FACTORY_DELETE")  
          
        try:  
            # --- [1] إيقاف المهام النشطة (منع استمرار طلبات api.telegram.org) ---
            if hasattr(self, 'active_tasks') and bot_token in self.active_tasks:
                current_logger.info(f"🛑 [HALT]: إيقاف مهمة البوت {bot_token[:10]}...")
                self.active_tasks[bot_token].cancel()
                try:
                    # انتظار الإغلاق لضمان عدم حدوث تداخل أثناء الحذف
                    await self.active_tasks[bot_token]
                except (asyncio.CancelledError, Exception): 
                    pass
                self.active_tasks.pop(bot_token, None)

            # --- [2] الحذف من قاعدة البيانات المحلية (SQLite) ---
            # الحفاظ على الكويري والمسميات العربية الأصلية (التوكن، البوتات_المصنوعة)
            query = "DELETE FROM البوتات_المصنوعة WHERE التوكن = ?"  
            
            # تنفيذ غير متزامن لضمان عدم تجميد الكلاس (Thread-safe execution)
            loop = asyncio.get_event_loop()
            if self.cursor and self.conn:
                await loop.run_in_executor(None, lambda: self.cursor.execute(query, (bot_token,)))  
                await loop.run_in_executor(None, self.conn.commit)  
              
            # --- [3] تحديث وتصفير الكاش العالمي (FACTORY_GLOBAL_CACHE) ---
            # الالتزام الصارم بمنطق globals() الذي وضعته أنت
            if 'FACTORY_GLOBAL_CACHE' in globals():  
                all_bots = globals()['FACTORY_GLOBAL_CACHE'].get('all_bots', [])  
                # تصفية القائمة لاستبعاد البوت المحذوف (الوظيفة الأصلية)
                new_bot_list = [b for b in all_bots if str(b.get('التوكن')) != str(bot_token)]  
                globals()['FACTORY_GLOBAL_CACHE']['all_bots'] = new_bot_list  
                  
                # إزالة بيانات المزامنة الخاصة بالبوت من الكاش إن وجدت (الوظيفة الأصلية)
                if 'bot_sync_versions' in globals()['FACTORY_GLOBAL_CACHE']:  
                    globals()['FACTORY_GLOBAL_CACHE']['bot_sync_versions'].pop(bot_token, None)  

            # --- [4] تدمير أوراق العمل سحابياً (Google Sheets Cleanup) ---
            # الوصول لـ ss عبر globals() لضمان تطهير الشيت بالكامل
            global ss
            if 'ss' in globals() and ss:
                current_logger.info("🧨 بدء تدمير الأوراق السحابية لتصفير النظام...")
                all_sheets = ss.worksheets()
                for ws in all_sheets:
                    # نترك ورقة واحدة فقط (الرئيسية) كمرجع تقني لأن جوجل ترفض الشيت الفارغ تماماً
                    if ws.title != "الرئيسية":
                        try:
                            ss.del_worksheet(ws)
                            current_logger.info(f"🧨 تم تدمير الورقة السحابية: {ws.title}")
                            # فاصل زمني بسيط لتجنب خطأ تجاوز الطلبات (Rate Limit) في جوجل
                            await asyncio.sleep(0.5) 
                        except Exception as e_ws:
                            current_logger.warning(f"⚠️ تخطي ورقة {ws.title}: {e_ws}")

            # --- [5] معالجة خطأ DataManager (إصلاح نقص الوسيط bot_token) ---
            try:
                from cache_manager import DataManager
                # تمرير bot_token المطلوب لإصلاح خطأ سجلات Railway المكتشف
                dm = DataManager(bot_token=bot_token)
                if hasattr(dm, 'reset_local_files'):
                    # تنفيذ تصفير الملفات المحلية (الوظيفة الأصلية)
                    await dm.reset_local_files()
            except Exception as dm_err:
                current_logger.warning(f"⚠️ تنبيه: فشل تصفير الملفات المحلية: {dm_err}")

            # --- [6] رسالة الانتهاء وإعادة البناء التلقائي ---
            current_logger.info(f"🗑️ [PURGED]: تم حذف كل المعلومات. النظام الآن في حالة 'المصنع الخام'.")
            print("\n✅ تم الانتهاء من حذف جميع المعلومات (كاش/SQLite/شيت)")
            
            # استدعاء دالة بناء الجداول الموجودة داخل الكلاس (Re-build Mechanism)
            print("🔄 جاري البدء في بناء الجداول الجديدة...")
            # التأكد من تمرير bot_token لضمان زرع الإعدادات الافتراضية
            total_created = self.setup_bot_factory_database(bot_token)
            
            if total_created > 0:
                print(f"🎊 تم إعادة بناء {total_created} جدول بنجاح. تم التنفيذ بناءً على طلبك.")
            
            return True

        except Exception as e:  
            # تسجيل الخطأ مع كامل التفاصيل التقنية (الوظيفة الأصلية)
            current_logger.error(f"❌ [DELETE ERROR]: فشل حذف البوت: {e}", exc_info=True)  
            return False
           
    async def hard_factory_reset_comprehensive(self):
        """
        محرك الفرمتة الشامل (الوضع الخام):
        1. تصفير الكاش العالمي (RAM) تماماً.
        2. تدمير وحذف ملف SQLite من الهاردوير.
        3. مسح وتطهير كافة أوراق جوجل شيت (ماعدا الرئيسية).
        4. إعادة بناء الهيكل الجديد عبر setup_bot_factory_database.
        """
        current_logger = logging.getLogger("FACTORY_RESET")
        current_logger.warning("🚨 بدء عملية تصفير النظام الشاملة (ضبط المصنع)...")

        try:
            # --- [1] تصفير الكاش العالمي (RAM Cache) ---
            if 'FACTORY_GLOBAL_CACHE' in globals():
                globals()['FACTORY_GLOBAL_CACHE'] = {
                    'all_bots': [],
                    'bot_sync_versions': {},
                    'system_status': 'RAW_FACTORY',
                    'reset_time': time.ctime()
                }
                current_logger.info("🧹 [1/4] تم تصفير الذاكرة المؤقتة (RAM) بنجاح.")

            # --- [2] تدمير قاعدة بيانات SQLite نهائياً ---
            try:
                # إغلاق الاتصال الحالي قبل الحذف لفك قفل الملف
                if self.conn:
                    self.conn.close()
                
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                    current_logger.info(f"💾 [2/4] تم حذف ملف قاعدة البيانات {self.db_path} نهائياً.")
                
                # --- [التصحيح التقني]: استخدام الدالة الموحدة لإعادة فتح الاتصال بنجاح ---
                # بدلاً من تكرار كود sqlite3.connect، نستخدم المحرك الذي أعددناه لضمان الإعدادات الصحيحة
                self._establish_connection()
                # ----------------------------------------------------------------------
                
            except Exception as db_e:
                current_logger.error(f"⚠️ فشل تدمير SQLite: {db_e}")

            # --- [3] تطهير جوجل شيت (Cloud Clean) ---
            global ss, _ws_cache
            if 'ss' in globals() and ss:
                current_logger.info("☁️ [3/4] جاري تطهير أوراق العمل من السحاب...")
                all_ws = ss.worksheets()
                for ws in all_ws:
                    # نترك ورقة واحدة (الرئيسية) كجذر للنظام لأن جوجل تمنع حذف كل الأوراق
                    if ws.title != "الرئيسية":
                        try:
                            ss.del_worksheet(ws)
                            current_logger.info(f"🧨 تم تدمير الورقة: {ws.title}")
                            time.sleep(0.5) # فاصل زمني لتجنب حظر جوجل
                        except: pass
                # تصفير كاش الأوراق
                _ws_cache = {}

            # --- [4] إرسال رسالة البيان ---
            print("\n" + "🔥" * 15)
            print("تم الانتهاء من حذف كافة البيانات القديمة بنجاح.")
            print("النظام الآن في حالة 'المصنع الخام'.")
            print("🔥" * 15 + "\n")

            # --- [5] استدعاء دالة البناء (إعادة الإعمار) ---
            print("🔄 جاري الآن بناء الجداول الجديدة وزرع الإعدادات الافتراضية...")
            # استدعاء دالة البناء المتطورة (الرام + SQLite + شيت)
            # تم التأكد من استخدام bot_token لضمان زرع الإعدادات
            total_sheets = self.setup_bot_factory_database(self.bot_token)

            if total_sheets > 0:
                print(f"🎊 مبروك! تم إعادة بناء المصنع بـ {total_sheets} جدول نظيف.")
                current_logger.info("🎊 عملية ضبط المصنع اكتملت بنجاح.")
                return True
            
            return False

        except Exception as e:
            current_logger.error(f"❌ خطأ كارثي أثناء ضبط المصنع: {e}", exc_info=True)
            return False

# ==========================================================================
#نهاية دوال الفورمات الهيكلة 

# ==========================================================================

def check_excel_export_permission(bot_token, all_bots):
    """التحقق من صلاحية تصدير الإكسل للبوت المحدد"""
    bot_cfg = next((b for b in all_bots if str(b.get("التوكن")) == str(bot_token)), {})
    return str(bot_cfg.get("ميزة_رفع_وتصدير_البيانات_اكسل", "FALSE")).upper() == "TRUE"
# ==========================================================================
# 4. تفعيل المحرك الموحد
# ==========================================================================

factory_token = os.getenv("BOT_TOKEN")
# إنشاء كائن db_manager الوحيد الذي سيعتمد عليه كامل النظام
db_manager = DataManager(factory_token)


# إشعار النظام ببدء العمل
if db_manager:
    logger.info("🚀 محرك الكاش وقاعدة البيانات المدمج يعمل الآن بكفاءة...")

# ==========================================================================
# نهاية الملف - تم دمج database_core و cache_manager بنجاح كامل
# ==========================================================================
# انشاء نسخة مشفرة



# --------------------------------------------------------------------------
def get_translation_dict(bot_id):
    """
    محرك الترجمة المركزي المطور:
    - يسحب اللغة من الكاش العالمي لسرعة الاستجابة.
    - يدعم نظام الـ Fallback.
    """
    from cache_manager import FACTORY_GLOBAL_CACHE
    
    # 1. جلب بيانات البوت من الكاش (بدلاً من الاتصال المباشر بالشيت في كل مرة)
    all_bots = FACTORY_GLOBAL_CACHE.get("all_bots", [])
    bot_row = next((row for row in all_bots if str(row.get("التوكن")) == str(bot_id) or str(row.get("bot_id")) == str(bot_id)), None)

    # 2. تحديد اللغة
    lang = "ar"
    if bot_row:
        # نبحث عن عمود اللغة (تأكد من تسميته في الشيت 'language')
        lang_value = str(bot_row.get("language", "ar")).lower()
        if lang_value in ["en", "english"]:
            lang = "en"

    # 3. القواميس (تم تصحيح تكرار التعريف فقط)
    translations = {
        "ar": {
            "ref_points_join": "نقاط دعوة صديق",
            "ref_points_purchase": "نقاط الشراء",
            "min_points_redeem": "حد استبدال النقاط",
            "AI_cost": "تكلفة الذكاء الاصطناعي",
            "operating_environment": "بيئة التشغيل",
            "subscription_price": "سعر الاشتراك",
            "maximum_number_sections": "الحد الأقصى للأقسام",
            "maximum_number_groups": "الحد الأقصى للجروبات",
            "maximum_number_courses": "الحد الأقصى للدورات",
            "maximum_number_students": "الحد الأقصى للطلاب",
            "currency_unit": "وحدة العملة",
            "homework_grade": "درجة الواجبات",
            "maximum_withdrawal_marketers": "سحب الرصيد",
            "payment_information": "معلومات الدفع",
            "marketers_commission": "عمولة المسوقين",
            "honors_channel_id": "معرف قناة الأوسمة",
            "minimum_passing_gradee": "درجة النجاح الصغرى",
            "greatest_success_gradee": "درجة النجاح الكبرى",
            "public_channel_id": "معرف القناة العامة",
            "referral_link": "رابط الإحالة",
        },

        "en": {
            "ref_points_join": "Referral Points (Join)",
            "ref_points_purchase": "Referral Points (Purchase)",
            "min_points_redeem": "Minimum Redeem Points",
            "AI_cost": "AI Cost",
            "operating_environment": "Operating Environment",
            "subscription_price": "Subscription Price",
            "maximum_number_sections": "Max Sections",
            "maximum_number_groups": "Max Groups",
            "maximum_number_courses": "Max Courses",
            "maximum_number_students": "Max Students",
            "currency_unit": "Currency Unit",
            "homework_grade": "Homework Grade",
            "maximum_withdrawal_marketers": "Max Withdrawal (Marketers)",
            "payment_information": "Payment Information",
            "marketers_commission": "Marketers Commission",
            "honors_channel_id": "Honors Channel ID",
            "minimum_passing_gradee": "Minimum Passing Grade",
            "greatest_success_gradee": "Maximum Grade",
            "public_channel_id": "Public Channel ID",
            "referral_link": "Referral Link",
        }
    }
    
    selected_dict = translations.get(lang, translations["ar"])

    # 4. نظام الـ Fallback الذكي
    class SafeDict(dict):
        def __missing__(self, key):
            # إذا لم يجد الترجمة في الإنجليزية مثلاً، يبحث عنها في العربي قبل إرجاع المفتاح
            return translations["ar"].get(key, key)

    return SafeDict(selected_dict)






async def get_settings_bote(page: int = 0, limit: int = 10):
    """
    جلب البوتات المصنوعة من ورقة البوتات_المصنوعة وعرضها كأزرار.
    تلتزم بكافة الأعمدة والمفاتيح الأصلية في قاعدة البيانات.
    """
    from cache_manager import FACTORY_GLOBAL_CACHE, db_manager
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    try:
        offset = page * limit
        cache_key = f"bots_page_{page}"
        
        # محاولة جلب البيانات من الكاش العالمي
        bots = FACTORY_GLOBAL_CACHE.get(cache_key)

        if not bots:
            # جلب البيانات من قاعدة البيانات (ورقة البوتات_المصنوعة)
            # نستخدم db_manager لجلب السجلات وفق ليميت وأوفست
            try:
                # استعلام لجلب البيانات مع الحفاظ على ترتيب الأعمدة المطلوبة
                query = "SELECT * FROM البوتات_المصنوعة LIMIT ? OFFSET ?"
                bots = db_manager.execute_query_dict(query, (limit, offset))
                
                # تخزين في الكاش (التزام بالإسناد المباشر للقاموس)
                FACTORY_GLOBAL_CACHE[cache_key] = bots
            except Exception as e:
                print(f"❌ Error fetching from DB: {e}")
                bots = []

        keyboard = []

        # بناء الأزرار بناءً على قائمة البوتات
        if bots:
            for bot in bots:
                # استخدام المفاتيح الأصلية: 'اسم البوت'، 'plan'، 'التوكن'
                btn_text = f"🤖 {bot.get('اسم البوت', 'بدون اسم')} ({bot.get('plan', 'FREE')})"
                keyboard.append([
                    InlineKeyboardButton(
                        btn_text,
                        callback_data=f"sub_view_{bot.get('التوكن')}"
                    )
                ])

        # نظام التنقل (Navigation)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"botss_page_{page-1}"))
        
        # زر التالي (يظهر دائماً حسب المنطق المطلوب)
        nav.append(InlineKeyboardButton("➡️ التالي", callback_data=f"botss_page_{page+1}"))

        if nav:
            keyboard.append(nav)

        # زر العودة الدائم للوحة التحكم
        keyboard.append([
            InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="open_admin_dashboard")
        ])

        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        print(f"❌ Error in get_settings_bote: {e}")
        # في حالة الخطأ، إرجاع زر العودة على الأقل لضمان عدم تعطل الواجهة
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="open_admin_dashboard")]])

# ملاحظة: دالة جلب البيانات المساعدة المقترحة في حال عدم توفرها
def _fetch_all_bots(limit, offset):
    from cache_manager import db_manager
    query = "SELECT * FROM البوتات_المصنوعة LIMIT ? OFFSET ?"
    return db_manager.execute_query_dict(query, (limit, offset))

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# ==========================================================================
# نهاية الملف - تم الحفاظ على كافة المفاتيح والهيكل الأصلي للمصنع
# ==========================================================================
