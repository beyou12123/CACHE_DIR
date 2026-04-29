# ملف خاص بابازرار ولوحات المفاتيح
# ui_keyboards.py
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def get_coach_panel_keyboard(): # غيرنا الاسم قليلاً ليكون واضحاً أنه للأزرار فقط
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

def get_tech_settings_keyboard(m_status):
    """لوحة إعدادات الإدارة الأكاديمية والشؤون التعليمية"""
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
           InlineKeyboardButton("📊 استيراد Excel", callback_data="excel_import_start"),
           InlineKeyboardButton("📊 تصدير Excel", callback_data="excel_export_start")
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
            InlineKeyboardButton("ضبط درجة النجاح", callback_data="passing_grade"),            
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
    return InlineKeyboardMarkup(keyboard)
    
    
    
   
#لوحة تحكم المصنع
def get_owner_dashboard_keyboard(user_id, developer_id, m_status):
    # 1. أزرار متاحة لجميع الإداريين والمطور
    keyboard = [
        [InlineKeyboardButton("📊 إحصائيات البوتات", callback_data="stats_all")],
        [InlineKeyboardButton("📢 إذاعة للمشتركين", callback_data="broadcast_owners")],
        [InlineKeyboardButton("📥 تحميل نسخة", callback_data="download_cache_files")]
    ]

    # 2. أزرار حصرية للمطور فقط
    if user_id == developer_id:
        keyboard.extend([
            [InlineKeyboardButton("─── المحرك الهجين (SQLite) ───", callback_data="none")],
            [
                InlineKeyboardButton("📤 نسخة احتياطية للقناة", callback_data="backup_to_channel"),
                InlineKeyboardButton("🔄 استعادة من القناة", callback_data="restore_from_channel")
            ],
            [InlineKeyboardButton("─── عمليات النظام الحساسة ───", callback_data="none")],
            [InlineKeyboardButton("💳 إدارة الاشتراكات والترقيات", callback_data="manage_coaches")],
            [InlineKeyboardButton(f"🛠 وضع الصيانة {m_status}", callback_data="toggle_maintenance")],
            [InlineKeyboardButton("⚙️ تهيئة الجداول", callback_data="run_setup_db_now")],
            [
                InlineKeyboardButton("📤 رفع نسخة", callback_data="start_restore_request"),
                InlineKeyboardButton("⏳ بدء المزامنة اليدوية", callback_data="start_sync_shet")
            ],
            [
                InlineKeyboardButton("🔄 تحديث السيرفر", callback_data="restart_factory"),
                InlineKeyboardButton("♻️ إعادة تشغيل", callback_data="reboot_system")
            ],
            [InlineKeyboardButton("👨‍💼 قسم الأدمن", callback_data="admin_section")],
            [InlineKeyboardButton("⚠️ تصفير النظام بالكامل", callback_data="confirm_hard_reset")]
        ])

    # 3. زر العودة الدائم
    keyboard.append([
        InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_to_main")
    ])

    return InlineKeyboardMarkup(keyboard)



