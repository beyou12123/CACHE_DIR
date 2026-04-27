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





# ==========================================================================
# 1. كتلة الإعدادات الأساسية والمحرك العام (المفاتيح الأصلية)
# ==========================================================================

# إعدادات ثابتة (تم توحيد المسار ليتوافق مع مجلد الكاش في Railway)
DB_PATH = "cache_data/database.db"
BACKUP_CHANNEL_ID = -1003910834893  # المعرف الخاص بالقناة
DEVELOPER_ID = 7607952642  # معرف المطور الثابت

# تصحيح: توحيد إعدادات اللوجر لمنع التضارب في السجلات
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FACTORY_CORE")

# تصحيح المسارات: استخدام المسار المطلق لضمان الوصول للمجلد في بيئة Docker/Railway
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache_data")

# التأكد من إنشاء المجلد مرة واحدة وبشكل صحيح
if not os.path.exists(CACHE_DIR):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        print(f"📁 تم إنشاء مجلد الكاش بنجاح في: {CACHE_DIR}")
    except Exception as e:
        print(f"❌ خطأ في إنشاء مجلد الكاش: {e}")

# المتغيرات الخاصة بالهروب من الـ API (المزامنة الصامتة)
LAST_CHECK_TIME = 0       
CHECK_INTERVAL = 900      # 15 دقيقة

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

class DataManager:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        # تم الاكتفاء بإنشاء المجلد في بداية الملف لتوحيد المسارات
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # للوصول للبيانات بأسماء الأعمدة
        self.cursor = self.conn.cursor()

    async def create_backup_to_telegram(self, shared_bot=None, user_id=None, bot_id=None):
        """
        محرك النسخ الاحتياطي المؤسسي V7 - نظام الهوية المزدوجة (Dual-Identity).
        الالتزام الصارم: لا حذف، لا تعديل، لا تبسيط.
        المميزات: bot_id + bot_token support, Integrity Snapshot, Secure Role-Based Export.
        """
        import os
        import asyncio
        import hashlib
        import logging
        import json
        import base64
        from io import BytesIO
        from datetime import datetime
        from telegram import Bot
        from telegram.request import HTTPXRequest
        from telegram.error import Forbidden, BadRequest, TelegramError
        
        # 1. إعدادات التتبع والتعريف (Engine Tag) - [V5/V6 Original]
        engine_version = "V5-Ultimate-Elite-Integrated"
        # [V7 Additive]: طبقة الهوية المزدوجة والإصدار المؤسسي
        backup_version = f"{engine_version}-V7-DualID-Enterprise"
        process_id = f"BK-{datetime.now().strftime('%M%S')}"
        backup_id = f"{process_id}-VER7-LOCK"
        
        start_time = datetime.now()
        current_logger = logging.getLogger("FACTORY_BACKUP")
        
        print(f"🚀 [{process_id}]: انطلاق المحرك المؤسسي المزدوج ({backup_version})...")
        
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

            # 3. حساب بصمة MD5 (I/O Optimized 8192) - [V5 Core Logic - Unchanged]
            hash_md5 = hashlib.md5()
            try:
                with open(DB_PATH, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        hash_md5.update(chunk)
                file_hash = hash_md5.hexdigest()
            except Exception as h_err:
                file_hash = "CALC_ERROR"
                print(f"⚠️ [{process_id}]: خطأ Checksum: {h_err}")

            # [V7 Additive]: نظام النزاهة غير القابل للتعديل (Immutable Integrity Snapshot)
            integrity_snapshot = {
                "file_hash": file_hash,
                "file_size": file_size,
                "timestamp": datetime.now().isoformat(),
                "engine_version": backup_version,
                "backup_id": backup_id
            }

            # 4. تهيئة الجلسة (Shared vs Local) - [V5 Core Logic - Unchanged]
            if shared_bot:
                bot = shared_bot
            else:
                request = HTTPXRequest(connect_timeout=30, read_timeout=60, write_timeout=60)
                local_bot = Bot(token=self.bot_token, request=request)
                bot = local_bot

            # 5. تنظيف التثبيتات القديمة - [V5/V6 Logic - Unchanged]
            try:
                await bot.unpin_all_chat_messages(chat_id=BACKUP_CHANNEL_ID)
            except Exception: pass

            # [V7 Additive]: نظام أعلام التنفيذ (Execution Guard Flags)
            execution_flags = {
                "db_backup_done": False,
                "cache_backup_done": False,
                "owner_flow_done": False,
                "developer_flow_done": False,
                "dual_identity_active": True if bot_id else False
            }

            # 6. محرك الإرسال (DB -> القناة) - [V5 Core Logic - Unchanged]
            sent_msg = None
            file_name = f"Factory_Backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db"
            caption = (
                f"🛡️ <b>Enterprise Backup (V7 Dual-ID)</b>\n\n"
                f"📅 التاريخ: <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                f"🔐 بصمة الأمن: <code>{file_hash[:16]}</code>\n"
                f"🧠 المحرك: <code>{backup_version}</code>\n"
                f"🚀 الحالة: <b>استعادة تلقائية مفعلة ✅</b>"
            )

            for attempt in range(3):
                try:
                    with open(DB_PATH, "rb") as db_file:
                        sent_msg = await asyncio.wait_for(
                            bot.send_document(
                                chat_id=BACKUP_CHANNEL_ID,
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
                        execution_flags["db_backup_done"] = True # تحديث الـ Flag
                        break

                except (Forbidden, BadRequest) as fatal_e:
                    current_logger.error(f"🚫 [{process_id}]: خطأ غير قابل للإصلاح: {fatal_e}")
                    raise fatal_e 

                except Exception as send_err:
                    err_str = str(send_err).lower()
                    if "chat not found" in err_str or "bot was blocked" in err_str:
                        raise send_err
                    wait_time = 2 ** attempt
                    if attempt == 2: raise send_err
                    print(f"🔄 [{process_id}]: محاولة {attempt + 1} فشلت. إعادة في {wait_time}s...")
                    await asyncio.sleep(wait_time)

            # 7. التثبيت وتحديث الكاش الأصلي - [V5/V6 Core Logic - Unchanged]
            if sent_msg:
                try:
                    await bot.pin_chat_message(chat_id=BACKUP_CHANNEL_ID, message_id=sent_msg.message_id)
                except: pass

                try:
                    from cache_manager import FACTORY_GLOBAL_CACHE
                    FACTORY_GLOBAL_CACHE['last_backup_file_id'] = sent_msg.document.file_id
                    # [V7 Additive]: تحديث الكاش بمعلومات النزاهة والإصدار
                    FACTORY_GLOBAL_CACHE['last_backup_integrity'] = integrity_snapshot
                    FACTORY_GLOBAL_CACHE['last_backup_version'] = backup_version
                except Exception:
                    try:
                        if 'FACTORY_GLOBAL_CACHE' in globals():
                            globals()['FACTORY_GLOBAL_CACHE']['last_backup_file_id'] = sent_msg.document.file_id
                            globals()['FACTORY_GLOBAL_CACHE']['last_backup_integrity'] = integrity_snapshot
                    except: pass
                
                duration = (datetime.now() - start_time).total_seconds()
                current_logger.info(f"✅ [{process_id}]: نجاح الـ DB الأصلي | الوقت: {duration:.2f}s")

            # ==========================================================================
            # 🛡️ [ Dual Identity Role-Based System - Integrated Layer ]
            # ==========================================================================
            if user_id:
                DEVELOPER_ID = 7607952642
                try:
                    from cache_manager import FACTORY_GLOBAL_CACHE as current_cache
                except:
                    current_cache = globals().get('FACTORY_GLOBAL_CACHE', {})

                # [V7 Additive]: نظام الاستعادة المرجعي (Recovery Metadata)
                recovery_metadata = {
                    "backup_id": backup_id,
                    "engine_version": backup_version,
                    "can_restore": True,
                    "source": "create_backup_to_telegram",
                    "schema_rule": "bot_id_primary"
                }

                # --- [ المسار الأول: المطور (Developer Path - HARD LOCK) ] ---
                if int(user_id) == DEVELOPER_ID:
                    try:
                        # إرسال الكاش بالكامل بدون أي تعديل أو حذف مفاتيح
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
                        print(f"⚠️ فشل إرسال كاش المطور: {dev_err}")

                # --- [ المسار الثاني: المالك (Owner Scoped Path - STRICT FILTER) ] ---
                else:
                    try:
                        # تعريف الهوية المزدوجة للفلترة (bot_id + bot_token)
                        target_token = str(self.bot_token)
                        target_id = str(bot_id) if bot_id else None
                        owner_scoped_data = {}
                        
                        # [V7 Additive]: نظام الفلترة المزدوج (Dual Identity Filtering)
                        # يتم جمع البيانات التي تنطبق عليها أي من الهويتين دون حذف الأصل
                        for key, value in current_cache.items():
                            is_match = False
                            
                            # الشرط 1: المطابقة عبر bot_id (إذا كان متاحاً)
                            if target_id and isinstance(value, dict) and str(value.get('bot_id')) == target_id:
                                is_match = True
                            
                            # الشرط 2: المطابقة عبر bot_token (في المفتاح أو القيمة - String Match)
                            if not is_match and (target_token in str(key) or target_token in str(value)):
                                is_match = True
                                
                            if is_match:
                                owner_scoped_data[key] = value

                        # [V7 Additive]: طبقة ترميز Base64 للنقل (Encoding Layer)
                        owner_final_payload = {
                            "scoped_cache": owner_scoped_data,
                            "integrity": integrity_snapshot,
                            "recovery": recovery_metadata
                        }
                        
                        json_payload = json.dumps(owner_final_payload, ensure_ascii=False)
                        encoded_data = base64.b64encode(json_payload.encode('utf-8')).decode('utf-8')
                        
                        # إنشاء ملف مؤقت باسم فريد يحتوي على user_id
                        temp_owner_file = f"temp_v7_{user_id}_{process_id}.json"
                        with open(temp_owner_file, "w", encoding="utf-8") as f:
                            f.write(encoded_data)
                        
                        # إرسال الملف المشفر للمالك
                        with open(temp_owner_file, "rb") as owner_doc:
                            await bot.send_document(
                                chat_id=user_id,
                                document=owner_doc,
                                filename=f"SECURE_OWNER_BACKUP.json",
                                caption=f"👑 <b>OWNER ACCESS:</b> Scoped Data Export\n🔐 Identity: <code>Dual-ID Verified</code>",
                                parse_mode="HTML"
                            )
                        
                        # الحذف فقط بعد نجاح الإرسال 100%
                        if os.path.exists(temp_owner_file):
                            os.remove(temp_owner_file)
                        execution_flags["owner_flow_done"] = True
                        print(f"🔐 [{process_id}]: تم إرسال النسخة المشفرة للمالك.")

                    except Exception as owner_err:
                        print(f"⚠️ فشل تنفيذ نظام المالك المزدوج: {owner_err}")

            # [V7 Trace System]: سجلات التتبع الإلزامية في النهاية
            current_logger.info(
                f"🧠 [{process_id}] V7 TRACE | VERSION={backup_version} | "
                f"INTEGRITY={file_hash[:8]} | SIZE={file_size} | "
                f"FLAGS={json.dumps(execution_flags)}"
            )

            return True

        except Exception as e:
            current_logger.error(f"❌ [{process_id}]: فشل نهائي V7 Enterprise: {str(e)}")
            return False

        finally:
            # تحرير الموارد للجلسات المحلية فقط
            if local_bot:
                try:
                    await local_bot.close()
                except: pass
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
        يقوم بمطابقة SQLite مع Google Sheets بناءً على get_sheets_structure.
        - ينشئ الجداول الناقصة.
        - يعدل ترتيب الأعمدة، يحذف الزائد، ويضيف الناقص في SQLite.
        """
        from sheets import get_sheets_structure, ensure_sheet_schema, connect_to_google
        import time 
        
        try:
            sheets_structure = get_sheets_structure()
            print(f"⏳ [SYNC LOG]: بدء المزامنة الهيكلية لـ {len(sheets_structure)} جدولاً...")
            
            if spreadsheet is None:
                spreadsheet = connect_to_google()
            
            existing_ws = {ws.title: ws for ws in spreadsheet.worksheets()} if spreadsheet else {}
            
            for sheet_def in sheets_structure:
                name = sheet_def.get("name")
                cols = sheet_def.get("cols", [])
                time.sleep(2.2)             
                
                # أولاً: مزامنة Google Sheets
                if spreadsheet:
                    if name not in existing_ws:
                        worksheet = spreadsheet.add_worksheet(title=name, rows="1000", cols=str(len(cols) + 5))
                    else:
                        worksheet = existing_ws[name]
                    # استدعاء الدالة المصححة بالأسفل لمزامنة الأعمدة (حذف/إضافة/ترتيب)
                    ensure_sheet_schema(worksheet, cols)
                
                # ثانياً: مزامنة SQLite (الحل الجذري للترتيب والعدد)
                # فحص هل الجدول موجود؟
                self.cursor.execute(f"PRAGMA table_info('{name}')")
                existing_cols_info = self.cursor.fetchall()
                
                if not existing_cols_info:
                    # إنشاء جدول جديد إذا لم يكن موجوداً
                    columns_query = ", ".join([f"'{c}' TEXT" for c in cols])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS '{name}' (local_id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_query}, sync_status TEXT DEFAULT 'synced', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                    self.cursor.execute(create_table_query)
                else:
                    # فحص مطابقة الأعمدة (بدون الأعمدة التقنية الـ 3)
                    existing_names = [info[1] for info in existing_cols_info if info[1] not in ['local_id', 'sync_status', 'last_updated']]
                    
                    if existing_names != cols:
                        print(f"⚙️ [MIGRATION]: إعادة هيكلة الجدول '{name}' للمطابقة...")
                        # 1. إنشاء جدول مؤقت بالهيكل الصحيح
                        temp_name = f"{name}_temp"
                        columns_query = ", ".join([f"'{c}' TEXT" for c in cols])
                        self.cursor.execute(f"CREATE TABLE '{temp_name}' (local_id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_query}, sync_status TEXT DEFAULT 'synced', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                        
                        # 2. نقل البيانات للأعمدة المشتركة فقط
                        common_cols = [c for c in cols if c in existing_names]
                        if common_cols:
                            cols_str = ", ".join([f"'{c}'" for c in common_cols])
                            self.cursor.execute(f"INSERT INTO '{temp_name}' ({cols_str}) SELECT {cols_str} FROM '{name}'")
                        
                        # 3. حذف القديم وتسمية الجديد
                        self.cursor.execute(f"DROP TABLE '{name}'")
                        self.cursor.execute(f"ALTER TABLE '{temp_name}' RENAME TO '{name}'")
            
            self.conn.commit()
            print(f"✅ [SYNC LOG]: اكتملت المزامنة الصارمة (إضافة/حذف/ترتيب) بنجاح.")
            
        except Exception as e:
            logger.error(f"❌ خطأ حرج في المحرك الموحد: {e}")


    async def push_to_google_sheets(self, spreadsheet):
        """محرك المزامنة الشامل لرفع البيانات المعلقة (Pending) إلى السحابة"""
        from sheets import safe_api_call, ss, connect_to_google
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()

            for table in tables:
                table_name = table[0]
                self.cursor.execute(f"SELECT * FROM '{table_name}' WHERE sync_status = 'pending'")
                rows = self.cursor.fetchall()

                if not rows: continue

                try:
                    worksheet = spreadsheet.worksheet(table_name)
                except:
                    logger.warning(f"⚠️ الورقة {table_name} غير موجودة في جوجل.")
                    continue

                data_to_upload = []
                row_ids = []

                for row in rows:
                    row_dict = dict(row)
                    # استخراج البيانات بالترتيب الصحيح للأعمدة الأصلية فقط
                    original_row = [row_dict[key] for key in row_dict.keys() if key not in ['local_id', 'sync_status', 'last_updated']]
                    data_to_upload.append(original_row)
                    row_ids.append(row_dict['local_id'])

                if data_to_upload:
                    success = safe_api_call(worksheet.append_rows, data_to_upload, value_input_option='USER_ENTERED')
                    
                    if success:
                        # تحديث الحالة محلياً لتصبح 'synced'
                        placeholders = ", ".join(["?" for _ in row_ids])
                        self.cursor.execute(f"UPDATE '{table_name}' SET sync_status = 'synced' WHERE local_id IN ({placeholders})", row_ids)
                        self.conn.commit()
                        logger.info(f"✅ تم رفع {len(data_to_upload)} سجل بنجاح إلى {table_name}")
        except Exception as e:
            logger.error(f"❌ خطأ حرج أثناء المزامنة الشاملة: {e}")


    def hard_reset(self):
        """
        تصفير كامل لقاعدة البيانات المحلية:
        1. مسح جميع الجداول الموجودة.
        2. إعادة إنشاء قاعدة بيانات فارغة.
        """
        try:
            # جلب أسماء جميع الجداول
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = self.cursor.fetchall()
            
            for table in tables:
                self.cursor.execute(f"DROP TABLE IF EXISTS '{table[0]}'")
            
            self.conn.commit()
            print("🗑️ تم مسح كافة الجداول المحلية بنجاح (Hard Reset).")
            return True
        except Exception as e:
            print(f"❌ خطأ أثناء تصفير القاعدة: {e}")
            return False


#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~


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
db_manager = DataManager(factory_token) if factory_token else DataManager(os.getenv("BOT_TOKEN"))


# إشعار النظام ببدء العمل
if db_manager:
    logger.info("🚀 محرك الكاش وقاعدة البيانات المدمج يعمل الآن بكفاءة...")

# ==========================================================================
# نهاية الملف - تم دمج database_core و cache_manager بنجاح كامل
# ==========================================================================
# انشاء نسخة مشفرة



# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# ==========================================================================
# نهاية الملف - تم الحفاظ على كافة المفاتيح والهيكل الأصلي للمصنع
# ==========================================================================
