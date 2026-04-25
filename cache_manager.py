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
# انشاء نسخة مشفرة



def generate_secure_backup(bot_id=None):
    """إنشاء نسخة احتياطية مشفرة: للمطور (الكل) أو للعميل (خاص ببوت معين)"""
    try:
        # إذا كان bot_id موجود، نسحب بياناته فقط، وإذا لم يوجد (مطور) نسحب الكل
        data_to_save = {}
        if bot_id:
            for sheet_name, records in FACTORY_GLOBAL_CACHE["data"].items():
                filtered = [r for r in records if str(r.get("bot_id")) == str(bot_id)]
                if filtered: data_to_save[sheet_name] = filtered
        else:
            data_to_save = FACTORY_GLOBAL_CACHE["data"]

        # تحويل البيانات إلى نص مشفر Base64 لضمان قبول الاستضافة وسهولة الرفع
        json_string = json.dumps(data_to_save, ensure_ascii=False, indent=2)
        encoded_data = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
        
        backup_content = {
            "backup_info": {
                "type": "FULL" if not bot_id else "CLIENT",
                "bot_id": bot_id,
                "timestamp": get_system_time()
            },
            "payload": encoded_data
        }
        
        file_path = os.path.join(CACHE_DIR, f"backup_{bot_id if bot_id else 'MASTER'}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(backup_content, f, ensure_ascii=False, indent=4)
        return file_path
    except Exception as e:
        logger.error(f"❌ خطأ في تشفير النسخة: {e}")
        return None
 



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
def fetch_full_factory_data():
    """سحب بيانات المصنع كاملة وتحديث الرام والقرص مع ضمان الحفظ الفوري لكل ورقة"""


    global FACTORY_GLOBAL_CACHE
    try:
        structures = get_sheets_structure()
        print(f"🚀 [المحرك]: بدء المزامنة الشاملة ({len(structures)} ورقة)...")

        for config in structures:
            sheet_name = config["name"]
            try:
                sheet = ss.worksheet(sheet_name)
                # سحب البيانات في طلب واحد Batch Read
                records = sheet.get_all_records()
                FACTORY_GLOBAL_CACHE["data"][sheet_name] = records
                
                # --- [ التصحيح الجذري: الحفظ الفيزيائي الفوري لكل ورقة ] ---
                # نضمن هنا كتابة الملف على القرص فور سحبه لضمان وجوده للتحميل
                try:
                    file_path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(records, f, ensure_ascii=False, indent=4)
                    print(f"✅ سحب وحفظ: {sheet_name} | سجلات: {len(records)}")
                except Exception as disk_err:
                    print(f"⚠️ فشل الكتابة الفيزيائية للورقة {sheet_name}: {disk_err}")
                
                time.sleep(2.8) # حماية API جوجل
            except Exception as e:
                logger.warning(f"⚠️ تخطي الورقة {sheet_name}: {e}")

        # تحديث الإصدارات
        try:
            sync_sheet = ss.worksheet("نظام_المزامنة")
            sync_data = sync_sheet.get_all_records()
            for row in sync_data:
                b_id = str(row.get("bot_id"))
                FACTORY_GLOBAL_CACHE["versions"][b_id] = int(row.get("رقم_الإصدار", 1))
        except:
            print("⚠️ تعذر جلب الإصدارات.")

        # تنفيذ الحفظ الفيزيائي الشامل (للمراجعة النهائية وخريطة الإصدارات)
        save_cache_to_disk()

        print("🎊 [المحرك]: اكتملت المزامنة الشاملة (رام + قرص).")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ حرج في المزامنة: {e}")
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
def update_global_version(bot_id):
    """تحديث الإصدار في نظام_المزامنة باستخدام المطابقة المباشرة للمصفوفة لضمان الدقة"""

    try:
        # التأكد من الاتصال
        if 'ss' not in globals() or ss is None:

            connect_to_google()

        sync_sheet = ss.worksheet("نظام_المزامنة")
        # جلب العمود الأول بالكامل (معرفات البوتات) لضمان المطابقة النصية الدقيقة
        all_ids = sync_sheet.col_values(1)
        
        search_id = str(bot_id).strip()
        target_row = None

        # البحث عن الصف المناسب بمطابقة النص حرفياً لتجنب مشاكل التنسيق في شيت
        for index, row_id in enumerate(all_ids):
            if str(row_id).strip() == search_id:
                target_row = index + 1
                break

        # تعريف الوقت الحالي في أعلى الكتلة لاستخدامه في الحالتين (تحديث أو إضافة)
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if target_row:
            # جلب القيمة الحالية من الخلية مباشرة للتأكد من الرقم الأخير
            current_val = sync_sheet.cell(target_row, 2).value
            try:
                current_v = int(current_val) if current_val else 0
            except:
                current_v = 0
                
            new_v = current_v + 1

            # تحديث الخلايا في جوجل شيت باستخدام الوسيط الآمن safe_api_call لمنع الحظر
            # إضافة سطر تحديث الرام قبل الرفع
            FACTORY_GLOBAL_CACHE["versions"][str(bot_id)] = new_v
            safe_api_call(sync_sheet.update_cell, target_row, 2, new_v)
            safe_api_call(sync_sheet.update_cell, target_row, 3, now_time)

            # تحديث الذاكرة المركزية RAM لضمان استجابة البوت الفورية بالبيانات الجديدة
            FACTORY_GLOBAL_CACHE["versions"][search_id] = new_v
            
            # حفظ الكاش فيزيائياً على القرص لضمان بقاء البيانات عند إعادة التشغيل
            save_cache_to_disk()

            print(f"🔄 [نظام المزامنة]: تم تحديث التوكن بنجاح في الصف {target_row} إلى الإصدار {new_v}")
            return new_v
        else:
            # 🆕 التسجيل التلقائي: إذا لم يجد التوكن يقوم بإضافته فوراً في نظام المزامنة
            # الترتيب المعتمد: [bot_id, رقم_الإصدار, آخر_تحديث, الحالة, ID_المالك, ID_المطور]
            new_row = [search_id, 1, now_time, "نشط", "تلقائي", str(DEVELOPER_ID)]
            safe_api_call(sync_sheet.append_row, new_row)
            
            # تحديث الذاكرة والقرص للبوت الجديد لضمان مزامنته فوراً
            new_v = 1
            FACTORY_GLOBAL_CACHE["versions"][search_id] = new_v
            save_cache_to_disk()
            
            print(f"✨ [نظام المزامنة]: تم تسجيل توكن جديد تلقائياً: {search_id}")
            return new_v
            
    except Exception as e:
        logger.error(f"❌ فشل رفع الإصدار: {e}")
        return None



# ==========================================================================
# 6. دالة التحميل الذكي (تُستدعى من main.py)
# ==========================================================================
async def download_mirror_files(bot, user_id):
    """إرسال نسخة احتياطية مشفرة وموحدة بناءً على صلاحية المستخدم"""
    # التحقق هل المستخدم مطور أم عميل
    is_developer = (str(user_id) == str(DEVELOPER_ID))
    bot_id_filter = None if is_developer else user_id

    await bot.send_message(chat_id=user_id, text="🔐 جاري تجهيز النسخة الاحتياطية...")

    # توليد الملف الموحد المشفر فوراً
    file_path = generate_secure_backup(bot_id_filter)

    if file_path and os.path.exists(file_path):
        try:
            caption = "👑 <b>نسخة المطور الشاملة</b>" if is_developer else "📦 <b>نسخة البوت الخاصة بك</b>"
            caption += f"\n📅 التاريخ: {get_system_time()}\n🛡️ الحالة: مشفرة وقابلة للاستعادة."
            
            with open(file_path, 'rb') as doc:
                await bot.send_document(
                    chat_id=user_id,
                    document=doc,
                    filename=f"BACKUP_{'MASTER' if is_developer else user_id}.json",
                    caption=caption,
                    parse_mode="HTML"
                )
            # حذف الملف المؤقت بعد الإرسال
            os.remove(file_path)
        except Exception as e:
            logger.error(f"❌ فشل إرسال النسخة: {e}")
    else:
        await bot.send_message(chat_id=user_id, text="⚠️ فشل إنشاء النسخة، تأكد من وجود بيانات في الكاش أولاً.")



# --------------------------------------------------------------------------
# دالة الاستعادة 
async def process_restore_logic(file_content, requester_id):
    """
    المحرك المرن: استعادة شاملة للمطور (المصنع) أو جزئية للبوت الفرعي
    """
    from sheets import ss
    import json
    import base64
    try:
        # 1. فك التشفير
        backup_data = json.loads(file_content)
        encoded_payload = backup_data.get("payload")
        # فك تشفير Base64 للحصول على البيانات الحقيقية
        decoded_data = json.loads(base64.b64decode(encoded_payload).decode('utf-8'))
        
        # معرفة هل المستعيد هو المطور الرئيسي
        is_developer = (str(requester_id) == str(DEVELOPER_ID))
        
        # 2. حلقة المزامنة لجميع الأوراق الموجودة في الملف
        for sheet_name, new_records in decoded_data.items():
            try:
                sheet = ss.worksheet(sheet_name)
                
                if is_developer:
                    # --- [ وضع المطور: استعادة المصنع الشاملة ] ---
                    sheet.clear()
                    if new_records:
                        headers = list(new_records[0].keys())
                        rows = [list(r.values()) for r in new_records]
                        sheet.append_row(headers, value_input_option='USER_ENTERED')
                        sheet.append_rows(rows, value_input_option='USER_ENTERED')
                    FACTORY_GLOBAL_CACHE["data"][sheet_name] = new_records
                else:
                    # --- [ وضع البوت الفرعي: استبدال أسطر العميل فقط ] ---
                    current_records = FACTORY_GLOBAL_CACHE["data"].get(sheet_name, [])
                    # الفلترة الذكية: البحث في الأعمدة المحتملة لـ ID العميل
                    updated_list = [
                        r for r in current_records 
                        if str(r.get("ID المالك")) != str(requester_id) and 
                           str(r.get("bot_id")) != str(requester_id)
                    ]
                    updated_list.extend(new_records)
                    
                    sheet.clear()
                    if updated_list:
                        headers = list(updated_list[0].keys())
                        rows = [list(r.values()) for r in updated_list]
                        sheet.append_row(headers, value_input_option='USER_ENTERED')
                        sheet.append_rows(rows, value_input_option='USER_ENTERED')
                    FACTORY_GLOBAL_CACHE["data"][sheet_name] = updated_list

                # تحديث المرآة (الملف الفيزيائي على القرص)
                file_path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(FACTORY_GLOBAL_CACHE["data"][sheet_name], f, ensure_ascii=False, indent=4)

            except Exception as e:
                print(f"⚠️ تخطي الورقة {sheet_name}: {e}")
        
        save_cache_to_disk() 
        return True
    except Exception as e:
        print(f"❌ خطأ حرج في محرك الاستعادة الشامل: {e}")
        return False

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

    async def restore_from_telegram(self):
        """جلب آخر نسخة احتياطية من التلجرام إذا لم يوجد ملف محلي"""
        if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0:
            logger.info("✅ القاعدة المحلية موجودة، لا داعي للاستعادة.")
            return

        try:
            bot = Bot(token=self.bot_token)
            # جلب آخر رسالة في القناة لاستعادة قاعدة البيانات
            messages = await bot.get_chat_history(chat_id=BACKUP_CHANNEL_ID, limit=1)
            
            if messages:
                backup_data = messages[0].text
                # فك تشفير Base64 وتحويله لملف DB
                with open(DB_PATH, "wb") as f:
                    f.write(base64.b64decode(backup_data))
                logger.info("🔄 تم استعادة قاعدة البيانات من تلجرام بنجاح!")
        except Exception as e:
            logger.error(f"❌ فشل في استعادة النسخة الاحتياطية: {e}")

    async def create_backup_to_telegram(self):
        """تحويل ملف SQL إلى Base64 وإرساله للقناة الخاصة لضمان الأمان القصوى"""
        try:
            if not os.path.exists(DB_PATH):
                logger.warning("⚠️ لا يوجد ملف قاعدة بيانات لعمل نسخة احتياطية.")
                return

            with open(DB_PATH, "rb") as f:
                encoded_string = base64.b64encode(f.read()).decode('utf-8')
            
            bot = Bot(token=self.bot_token)
            await bot.send_message(
                chat_id=BACKUP_CHANNEL_ID,
                text=encoded_string,
                disable_notification=True
            )
            logger.info("💾 تم إرسال نسخة احتياطية جديدة مشفرة إلى التلجرام.")
        except Exception as e:
            logger.error(f"❌ فشل في إنشاء نسخة احتياطية: {e}")

    def setup_sync_scheduler(self):
        """ضبط المزامنة والنسخ الاحتياطي التلقائي في الساعة 03:30 فجراً"""
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.create_backup_to_telegram, 'cron', hour=3, minute=30)
        
        scheduler.start()
        logger.info("⏰ تم تفعيل مجدول المزامنة التلقائية (03:30).")

    def sync_schema(self, spreadsheet, sheets_structure):
        """
        المحرك الموحد (Unified Schema Engine):
        1. يتأكد من وجود الأوراق في جوجل شيت (وإنشائها إذا نقصت).
        2. يتأكد من مطابقة الأعمدة في جوجل شيت (ترميم الأعمدة).
        3. ينشئ جداول SQLite مطابقة تماماً لما هو موجود في جوجل.
        """
        from sheets import ensure_sheet_schema # استيراد محلي لمنع التكرار
        try:
            # أولاً: جلب أسماء الأوراق الحالية في جوجل لتوفير طلبات الـ API
            existing_ws = {ws.title: ws for ws in spreadsheet.worksheets()}
            
            for sheet_def in sheets_structure:
                name = sheet_def.get("name")
                cols = sheet_def.get("cols", [])
                    # --- [ إضافة تأخير بسيط هنا ] ---
                time.sleep(2.2)             
                # --- أ: ضمان وجود الورقة في جوجل شيت ---
                if name not in existing_ws:
                    worksheet = spreadsheet.add_worksheet(title=name, rows="1000", cols=str(len(cols) + 5))
                    logger.info(f"🆕 تم إنشاء ورقة جديدة في جوجل: {name}")
                else:
                    worksheet = existing_ws[name]
                
                # --- ب: ضمان مطابقة الأعمدة في جوجل شيت (الترميم) ---
                ensure_sheet_schema(worksheet, cols)
                
                # --- ج: ضمان مطابقة الجدول في SQLite المحلي ---
                # نستخدم الكولومز الفعلية الآن لإنشاء الجدول محلياً
                columns_query = ", ".join([f"'{c}' TEXT" for c in cols])
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS '{name}' (
                    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns_query},
                    sync_status TEXT DEFAULT 'synced',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.cursor.execute(create_table_query)
            
            self.conn.commit()
            logger.info(f"✅ اكتملت المزامنة الموحدة لـ {len(sheets_structure)} جدولاً (جوجل + محلي).")
            
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



# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------

# --------------------------------------------------------------------------



# ==========================================================================
# نهاية الملف - تم الحفاظ على كافة المفاتيح والهيكل الأصلي للمصنع
# ==========================================================================
