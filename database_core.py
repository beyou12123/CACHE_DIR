import sqlite3
import os
import base64
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
import asyncio

# إعدادات ثابتة (يفضل نقلها لـ .env لاحقاً)
DB_PATH = "database.db"
BACKUP_CHANNEL_ID = -1003910834893  # المعرف الذي زودتني به مع إضافة -100 للقنوات

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB_CORE")

class DataManager:
    def __init__(self, bot_token):
        self.bot_token = bot_token
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
        # هنا سنضع دالة المزامنة مع جوجل لاحقاً

        scheduler.add_job(lambda: asyncio.create_task(self.create_backup_to_telegram()), 'cron', hour=3, minute=30)
        scheduler.start()

# --------------------------------------------------------------------------



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
                
                # تنظيف أسماء الأعمدة لتناسب SQL (إزالة المسافات والرموز)
               
                safe_headers = [f"{h.strip()}" for h in headers]
                
                # إنشاء استعلام إنشاء الجدول
                columns_query = ", ".join([f"{h} TEXT" for h in safe_headers])
                
                
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
# --------------------------------------------------------------------------
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
                # تحويل السجل إلى قائمة (List) مع استبعاد الأعمدة التقنية (local_id, sync_status, last_updated)
                # نحافظ على الأعمدة الأصلية فقط (مثلاً من 1 إلى 44)
                row_dict = dict(row)
                original_row = [row_dict[key] for key in row_dict.keys() if key not in ['local_id', 'sync_status', 'last_updated']]
                
                data_to_upload.append(original_row)
                row_ids.append(row_dict['local_id'])

            if data_to_upload:
                # استخدام صمام الأمان للرفع (لحماية الكوتا)
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
import os
factory_token = os.getenv("BOT_TOKEN")
# تعريف الكائن الموحد الذي يبحث عنه نظام sheets.py
db_manager = DataManager(factory_token) if factory_token else None

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










# سيتم استدعاء هذا المحرك في main.py
