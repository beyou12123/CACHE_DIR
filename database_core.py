import sqlite3
import os
import base64
import logging
import re
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
import asyncio

# إعدادات ثابتة (تم توحيد المسار ليتوافق مع مجلد الكاش في Railway)
DB_PATH = "cache_data/database.db"
BACKUP_CHANNEL_ID = -1003910834893  # المعرف الخاص بالقناة

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB_CORE")

class DataManager:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        # التأكد من وجود مجلد الكاش قبل الاتصال
        if not os.path.exists("cache_data"):
            os.makedirs("cache_data")
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
            # جلب آخر رسالة في القناة
            from telegram import Update
            # استخدام محرك البحث لجلب الرسائل (بافتراض صلاحيات البوت)
            messages = await bot.get_chat_history(chat_id=BACKUP_CHANNEL_ID, limit=1)
            
            if messages:
                backup_data = messages[0].text
                # فك تشفير Base64 وتحويله لملف
                with open(DB_PATH, "wb") as f:
                    f.write(base64.b64decode(backup_data))
                logger.info("🔄 تم استعادة قاعدة البيانات من تلجرام بنجاح!")
        except Exception as e:
            logger.error(f"❌ فشل في استعادة النسخة الاحتياطية: {e}")

    async def create_backup_to_telegram(self):
        """تحويل ملف SQL إلى Base64 وإرساله للقناة الخاصة"""
        try:
            with open(DB_PATH, "rb") as f:
                encoded_string = base64.b64encode(f.read()).decode('utf-8')
            
            bot = Bot(token=self.bot_token)
            await bot.send_message(
                chat_id=BACKUP_CHANNEL_ID,
                text=encoded_string,
                disable_notification=True
            )
            logger.info("💾 تم إرسال نسخة احتياطية جديدة إلى التلجرام.")
        except Exception as e:
            logger.error(f"❌ فشل في إنشاء نسخة احتياطية: {e}")

    def setup_sync_scheduler(self):
        """ضبط المزامنة الشاملة في الساعة 03:30"""
        scheduler = AsyncIOScheduler()
        # إضافة المهمة بشكل صحيح للمجدل
        scheduler.add_job(lambda: asyncio.run(self.create_backup_to_telegram()), 'cron', hour=3, minute=30)
        scheduler.start()

    def sync_schema(self, spreadsheet):
        """استكشاف الأوراق في جوجل وإنشاء جداول مطابقة لها محلياً"""
        try:
            # 1. جلب كافة الأوراق من ملف جوجل
            sheets = spreadsheet.worksheets()
            
            for sheet in sheets:
                sheet_name = sheet.title
                # جلب العناوين (الصف الأول)
                headers = sheet.row_values(1)
                
                if not headers:
                    # إذا كانت الورقة فارغة تماماً، نستخدم هيكل افتراضي (44 عموداً)
                    headers = [f"column_{i}" for i in range(1, 45)]
                
                # --- [ معالجة الأسماء العربية لـ SQLite ] ---
                clean_headers = []
                seen = set()
                for h in headers:
                    # تنظيف الاسم من المسافات والرموز مع الحفاظ على الكلمات
                    h_clean = h.strip()
                    if not h_clean or h_clean in seen:
                        # معالجة التكرار (duplicate column error)
                        suffix = 1
                        while f"{h_clean or 'col'}_{suffix}" in seen:
                            suffix += 1
                        h_clean = f"{h_clean or 'col'}_{suffix}"
                    
                    seen.add(h_clean)
                    clean_headers.append(h_clean)
                
                # إنشاء استعلام إنشاء الجدول (استخدام الاقتباس للاسماء العربية)
                columns_query = ", ".join([f"'{h}' TEXT" for h in clean_headers])
                
                # إضافة أعمدة التحكم التقنية
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS '{sheet_name}' (
                    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns_query},
                    sync_status TEXT DEFAULT 'synced',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.cursor.execute(create_table_query)
            
            self.conn.commit()
            logger.info(f"✅ تم فحص ومزامنة هيكلة {len(sheets)} جداول بنجاح.")
            
        except Exception as e:
            logger.error(f"❌ خطأ في مزامنة الهيكلة: {e}")

    async def push_to_google_sheets(self, spreadsheet):
        """
        محرك المزامنة الشامل (03:30):
        يرفع كافة البيانات المعلقة (Pending) من SQLite إلى Google Sheets 
        بدون حذف أو اختصار أي عمود.
        """
        try:
            # جلب قائمة بكافة الجداول المحلية
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()

            for table in tables:
                table_name = table[0]
                # جلب السجلات التي لم تُرفع بعد
                self.cursor.execute(f"SELECT * FROM '{table_name}' WHERE sync_status = 'pending'")
                rows = self.cursor.fetchall()

                if not rows:
                    continue

                # فتح الورقة المقابلة في جوجل شيت
                try:
                    worksheet = spreadsheet.worksheet(table_name)
                except:
                    logger.warning(f"⚠️ الورقة {table_name} غير موجودة في جوجل، سيتم تخطيها.")
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
                    # استخدام صمام الأمان للرفع (لحماية الكوتا) من ملف sheets.py
                    from sheets import safe_api_call
                    success = safe_api_call(worksheet.append_rows, data_to_upload, value_input_option='USER_ENTERED')
                    
                    if success:
                        # تحديث الحالة محلياً لتصبح 'synced'
                        placeholders = ", ".join(["?" for _ in row_ids])
                        self.cursor.execute(f"UPDATE '{table_name}' SET sync_status = 'synced' WHERE local_id IN ({placeholders})", row_ids)
                        self.conn.commit()
                        logger.info(f"✅ تم رفع {len(data_to_upload)} سجل بنجاح إلى ورقة {table_name}")

        except Exception as e:
            logger.error(f"❌ خطأ حرج أثناء المزامنة الشاملة: {e}")

# --------------------------------------------------------------------------
# أضف هذا في نهاية ملف database_core.py

factory_token = os.getenv("BOT_TOKEN")
# تعريف الكائن الموحد الذي يبحث عنه نظام sheets.py لضمان عدم حدوث ImportError
db_manager = DataManager(factory_token) if factory_token else None

# --------------------------------------------------------------------------
# [الفراغات المطلوبة كما في الملف الأصلي]
# --------------------------------------------------------------------------
