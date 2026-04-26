# SubscriptionManager.py

import json
import logging
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from cache_manager import db_manager, FACTORY_GLOBAL_CACHE
from sheets import update_global_version


# ==========================================================================
# إعداد Logger
# ==========================================================================
logger = logging.getLogger("SUB_MANAGER")


# ==========================================================================
# 1. تعريف الباقات (محسنة داخلياً بدون تغيير السلوك الخارجي)
# ==========================================================================
PLANS = {
    "FREE": {
        "label": "🆓 المجانية",
        "students": 50,
        "courses": 5,
        "sections": 2,
        "ai": "FALSE",
        "excel": "FALSE",
        "ai_model": "gpt-3.5-turbo",
        "price": "0",
        "support": "جماعي"
    },
    "PRO": {
        "label": "🚀 الاحترافية (PRO)",
        "students": 500,
        "courses": 30,
        "sections": 10,
        "ai": "TRUE",
        "excel": "TRUE",
        "ai_model": "gpt-4-o",
        "price": "15",
        "support": "سريع"
    },
    "VIP": {
        "label": "👑 الملكية (VIP)",
        "students": -1,  # Unlimited
        "courses": -1,
        "sections": -1,
        "ai": "TRUE",
        "excel": "TRUE",
        "ai_model": "claude-3-opus",
        "price": "50",
        "support": "شخصي مباشر"
    }
}


# ==========================================================================
# 2. Utilities (مساعدة)
# ==========================================================================

def _now():
    return datetime.now()


def _format_date(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _calculate_expiration(current_expiration, duration_days):
    """دعم التراكم بدلاً من الاستبدال"""
    now = _now()
    current_exp = _parse_date(current_expiration) if current_expiration else None

    if current_exp and current_exp > now:
        return current_exp + timedelta(days=duration_days)
    return now + timedelta(days=duration_days)


def _safe_limit(value):
    """تحويل -1 إلى Unlimited للعرض"""
    return "Unlimited" if value == -1 else value


# ==========================================================================
# 3. Data Access Layer (DB Layer)
# ==========================================================================
def _fetch_all_bots(limit=50, offset=0):
    """جلب سجلات البوتات من قاعدة البيانات وتحويلها لقواميس لضمان عمل الواجهة"""
    try:
        # التأكد من استخدام الترتيب الصارم للأعمدة كما هي في قاعدة البيانات
        query = '''
            SELECT * FROM "البوتات_المصنوعة"
            LIMIT ? OFFSET ?
        '''
        db_manager.cursor.execute(query, (limit, offset))
        rows = db_manager.cursor.fetchall()
        
        # تحويل الصفوف إلى قائمة قواميس لضمان الوصول للمفاتيح بأسماء الأعمدة (مثل 'التوكن')
        # هذا يضمن توافق الدالة مع نظام الـ Pagination ونظام عرض الأزرار
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ خطأ في تنفيذ الاستعلام _fetch_all_bots: {e}")
        return []




def _fetch_bot_by_token(bot_token):
    db_manager.cursor.execute(
        'SELECT * FROM "البوتات_المصنوعة" WHERE "التوكن" = ?',
        (bot_token,)
    )
    return db_manager.cursor.fetchone()


def _update_bot_subscription(bot_token, params):
    query = '''
        UPDATE "البوتات_المصنوعة" 
        SET "plan" = ?, 
            "expiration_date" = ?, 
            "الحد_الأقصى_للطلاب" = ?, 
            "الحد_الأقصى_للدوات" = ?, 
            "الحد_الأقصى_للاقسام" = ?, 
            "ميزة_الذكاء_الاصطناعي" = ?, 
            "ميزة_رفع_وتصدير_البيانات_اكسل" = ?,
            "حالة_الدفع" = 'Paid',
            "تاريخ_آخر_تجديد" = ?,
            "نموذج_الذكاء_الاصطناعي" = ?,
            sync_status = 'pending'
        WHERE "التوكن" = ?
    '''

    db_manager.cursor.execute(query, params)
    db_manager.conn.commit()

    return db_manager.cursor.rowcount


# ==========================================================================
# 4. Viewer (عرض البوتات)
# ==========================================================================
def get_all_bots_keyboard(page=0, limit=50):
    """عرض قائمة البوتات مع Pagination"""
    try:
        offset = page * limit

        cache_key = f"bots_page_{page}"
        # استخدام .get لجلب البيانات من القاموس (صحيح)
        bots = FACTORY_GLOBAL_CACHE.get(cache_key)

        if not bots:
            # استدعاء الدالة المساعدة لجلب البيانات من القاعدة
            bots = _fetch_all_bots(limit, offset)
            # التصحيح الحرج: استخدام الإسناد المباشر للقاموس بدلاً من .set
            FACTORY_GLOBAL_CACHE[cache_key] = bots

        keyboard = []

        for bot in bots:
            # الحفاظ على المفاتيح الأصلية كما هي في قاعدة البيانات
            btn_text = f"🤖 {bot['اسم البوت']} ({bot['plan']})"
            keyboard.append([
                InlineKeyboardButton(
                    btn_text,
                    callback_data=f"sub_view_{bot['التوكن']}"
                )
            ])

        # Navigation (نظام التنقل)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"bots_page_{page-1}"))
        
        # زر التالي يظهر دائماً حسب منطق الكود الأصلي المرسل منك
        nav.append(InlineKeyboardButton("➡️ التالي", callback_data=f"bots_page_{page+1}"))

        if nav:
            keyboard.append(nav)

        keyboard.append([
            InlineKeyboardButton("🔙 عودة للوحة التحكم", callback_data="open_admin_dashboard")
        ])

        return InlineKeyboardMarkup(keyboard)

    except Exception as e:
        # الحفاظ على نص السجل الأصلي
        logger.error(f"❌ خطأ في جلب قائمة البوتات: {e}")
        return None

# دالة مساعدة لضمان عمل الـ Pagination بشكل صحيح مع قاعدة البيانات
def _fetch_all_bots(limit, offset):
    """جلب سجلات البوتات من قاعدة البيانات المحلية"""
    import sqlite3
    from cache_manager import DB_PATH
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # جلب البيانات مع الالتزام بالـ limit والـ offset
        cursor.execute(f"SELECT * FROM 'البوتات_المصنوعة' LIMIT ? OFFSET ?", (limit, offset))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"❌ Error fetching bots for pagination: {e}")
        return []




# ==========================================================================
# 5. Upgrade Logic (محرك الترقية)
# ==========================================================================

async def upgrade_bot_plan(bot_token, plan_key, duration_days=30):
    """
    ترقية البوت مع:
    - دعم التراكم
    - تحسين التتبع
    - تحقق من النجاح
    """
    try:
        plan_key = plan_key.upper()
        plan_data = PLANS.get(plan_key)

        if not plan_data:
            logger.warning(f"خطة غير معروفة: {plan_key}")
            return False

        days = int(duration_days)

        bot = _fetch_bot_by_token(bot_token)
        if not bot:
            logger.error("البوت غير موجود")
            return False

        new_exp = _calculate_expiration(bot['expiration_date'], days)

        now_str = _format_date(_now())
        exp_str = _format_date(new_exp)

        params = (
            plan_key.lower(),
            exp_str,
            plan_data["students"],
            plan_data["courses"],
            plan_data["sections"],
            plan_data["ai"],
            plan_data["excel"],
            now_str,
            plan_data.get("ai_model", "gpt-3.5-turbo"),
            bot_token
        )

        updated = _update_bot_subscription(bot_token, params)

        if updated == 0:
            logger.warning("لم يتم تحديث أي صف")
            return False

        update_global_version(bot_token)

        logger.info(
            f"[UPGRADE] bot={bot_token[:6]} plan={plan_key} days={days}"
        )

        return True

    except Exception as e:
        logger.error(f"❌ فشل الترقية: {e}")
        return False

# ==========================================================================
# 6. Subscription Interface (واجهة الاشتراك)
# ==========================================================================

def get_bot_subscription_interface(bot_token):
    """عرض تفاصيل الاشتراك مع تحسينات UX"""

    bot = _fetch_bot_by_token(bot_token)

    if not bot:
        return "⚠️ لم يتم العثور على بيانات البوت.", None

    exp_date = bot['expiration_date']
    exp_dt = _parse_date(exp_date)

    remaining = "غير محدد"
    if exp_dt:
        delta = exp_dt - _now()
        remaining = f"{delta.days} يوم" if delta.days > 0 else "منتهي"

    text = (
        f"💳 **إدارة اشتراك البوت**\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🤖 **الاسم:** {bot['اسم البوت']}\n"
        f"📊 **الباقة الحالية:** `{bot['plan']}`\n"
        f"📅 **ينتهي في:** `{exp_date if exp_date else 'غير محدد'}`\n"
        f"⏳ **المدة المتبقية:** {remaining}\n"
        f"💰 **حالة الدفع:** {bot['حالة_الدفع']}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"✨ **المميزات المفعلة:**\n"
        f"• ذكاء اصطناعي: {'✅' if bot['ميزة_الذكاء_الاصطناعي'] == 'TRUE' else '❌'}\n"
        f"• تصدير إكسل: {'✅' if bot['ميزة_رفع_وتصدير_البيانات_اكسل'] == 'TRUE' else '❌'}\n"
        f"• حد الطلاب: `{_safe_limit(bot['الحد_الأقصى_للطلاب'])}`\n\n"
        f"إختر الباقة الجديدة:"
    )

    keyboard = [
        [
            InlineKeyboardButton("🚀 PRO (شهر)", callback_data=f"exec_sub_{bot_token}_PRO_30"),
            InlineKeyboardButton("🚀 PRO (3 شهر)", callback_data=f"exec_sub_{bot_token}_PRO_90")
        ],
        [
            InlineKeyboardButton("🚀 PRO (6 شهر)", callback_data=f"exec_sub_{bot_token}_PRO_180"),
            InlineKeyboardButton("🚀 PRO (سنة)", callback_data=f"exec_sub_{bot_token}_PRO_365")
        ],
        [
            InlineKeyboardButton("👑 VIP (شهر)", callback_data=f"exec_sub_{bot_token}_VIP_30"),
            InlineKeyboardButton("👑 VIP (3 اشهر)", callback_data=f"exec_sub_{bot_token}_VIP_90")
        ],
        [
            InlineKeyboardButton("👑 VIP (6 اشهر)", callback_data=f"exec_sub_{bot_token}_VIP_180"),
            InlineKeyboardButton("👑 VIP (سنة)", callback_data=f"exec_sub_{bot_token}_VIP_365")
        ],
        [
            InlineKeyboardButton("🔄 تمديد الاشتراك الحالي", callback_data=f"extend_sub_{bot_token}")
        ],
        [
            InlineKeyboardButton("🆓 تحويل للمجاني (تجميد)", callback_data=f"exec_sub_{bot_token}_FREE_0")
        ],
        [
            InlineKeyboardButton("🔙 عودة للقائمة", callback_data="manage_subscriptions")
        ]
    ]

    return text, InlineKeyboardMarkup(keyboard)
   
  #======= دالة النسخ الاحتياطي السحابي ==========
def export_subscriptions_backup():
    """تصدير كافة بيانات الاشتراكات بصيغة جيسون"""
    try:
        # 1. جلب البيانات من الكاش
        all_bots = FACTORY_GLOBAL_CACHE.get("all_bots", [])
        
        # 2. إيقاف تصدير ملف فارغ: إذا كان الكاش فارغاً، نجلب البيانات من القاعدة فوراً
        if not all_bots:
            # استخدام الدالة المساعدة التي تم توحيدها لجلب كافة السجلات
            all_bots = _fetch_all_bots(limit=1000, offset=0)
            FACTORY_GLOBAL_CACHE["all_bots"] = all_bots

        backup_data = {
            "backup_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_bots": len(all_bots),
            "subscriptions": []
        }
        
        for bot in all_bots:
            # التصحيح: ربط المفاتيح بأسماء الأعمدة الحقيقية الموجودة في جدول "البوتات_المصنوعة"
            # (التوكن، plan، expiration_date، ID المالك) لضمان استخراج البيانات فعلياً
            backup_data["subscriptions"].append({
                "token": bot.get("التوكن"),
                "plan": bot.get("plan"),
                "expiry": bot.get("expiration_date"),
                "owner_id": bot.get("ID المالك")
            })
            
        return json.dumps(backup_data, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error creating sub backup: {e}")
        return None

 

#=============== دالة استيراد الإشتراك للبوتات ==============
async def import_subscriptions_from_backup(json_content):
    """استعادة الاشتراكات من نص جيسون"""
    try:
        data = json.loads(json_content)
        subscriptions = data.get("subscriptions", [])
        
        for sub in subscriptions:
            token = sub.get("token")
            plan = sub.get("plan")
            expiry = sub.get("expiry")
            
            if token and plan:
                # تحديث الكاش العالمي (FACTORY_GLOBAL_CACHE)
                # التصحيح: استخدام مفتاح "التوكن" بدلاً من "توكن_البوت" ليتطابق مع أعمدة قاعدة البيانات
                all_bots = FACTORY_GLOBAL_CACHE.get("all_bots", [])
                for bot in all_bots:
                    if bot.get("التوكن") == token:
                        # التصحيح: استخدام المفاتيح الأصلية (plan) و (expiration_date) كما هي في ملف SQLite
                        bot["plan"] = plan
                        bot["expiration_date"] = expiry
                        break
                
                # تحديث قاعدة البيانات الفعلية (Sheets + Local DB)
                # التصحيح: إزالة المعامل "override_expiry" من الاستدعاء لأنه غير موجود في تعريف الدالة الأصلي
                # وذلك لمنع انهيار البرنامج بخطأ (TypeError) عند الاستيراد
                await upgrade_bot_plan(token, plan, duration_days=0)
                
                # التصحيح: تمرير التوكن للدالة لضمان تحديث نسخة المزامنة لكل بوت يتم استيراده
                update_global_version(token)
        
        return True
    except Exception as e:
        logger.error(f"Error during import: {e}")
        return False




