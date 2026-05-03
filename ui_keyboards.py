from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def get_keyboard(kays: int, m_status="OFF"):
    
    keyboards = {}

    # 0️⃣ لوحة الإعدادات التقنية
    keyboards[0] = [
        [InlineKeyboardButton("📝 كليشة الترحيب", callback_data="manage_welcome_texts"),
         InlineKeyboardButton("🏢 اسم المؤسسة", callback_data="set_org_name"),
         InlineKeyboardButton("💬 الرسالة الترحيبية", callback_data="set_welcome_msg")],

        [InlineKeyboardButton("📜 القوانين", callback_data="set_rules"),
         InlineKeyboardButton("🤖 الرد التلقائي", callback_data="set_auto_reply")],

        [InlineKeyboardButton("🧠 تشغيل AI", callback_data="toggle_ai"),
         InlineKeyboardButton("⚙️ تعليمات AI", callback_data="set_ai_prompt")],

        [InlineKeyboardButton("🌅 الصباحية", callback_data="edit_welcome_morning"),
         InlineKeyboardButton("☀️ الظهرية", callback_data="edit_welcome_noon"),
         InlineKeyboardButton("🌆 المسائية", callback_data="edit_welcome_evening")],

        [InlineKeyboardButton("🌃 الليلية", callback_data="edit_welcome_night"),
         InlineKeyboardButton("🌆 ترحيب المساء", callback_data="set_welcome_evening"),
         InlineKeyboardButton("🌙 ترحيب الليل", callback_data="set_welcome_night")],

        [InlineKeyboardButton("🚫 المحظورات", callback_data="set_banned_words"),
         InlineKeyboardButton("👮‍♂️ الأدمن", callback_data="manage_admins")],

        [InlineKeyboardButton("🔘 الأزرار", callback_data="set_buttons"),
         InlineKeyboardButton("⌨️ الأوامر", callback_data="set_commands")],

        [InlineKeyboardButton("💳 معلومات الدفع", callback_data="set_payment"), InlineKeyboardButton("إعدادات هامة", callback_data="important_settings")],

        [InlineKeyboardButton("🌐 اللغة", callback_data="set_language"),
         InlineKeyboardButton("⏱️ وقت الرد", callback_data="set_delay")],

        [InlineKeyboardButton("🔙 عودة", callback_data="tech_settings")]
    ]

    # 1️⃣ لوحة المدربين
    keyboards[1] = [
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

    # 2️⃣ لوحة الإدارة
    keyboards[2] = [
        
         [InlineKeyboardButton("🔄 المزامنة", callback_data="manual_cache_sync")],

        [InlineKeyboardButton(f"🛠 وضع الصيانة {m_status}", callback_data="toggle_maintenance")],

        [InlineKeyboardButton("إدارة الفروع", callback_data="manage_branches"),
         InlineKeyboardButton("الإدارة المالية", callback_data="manage_financial"),
         InlineKeyboardButton("الكنترول", callback_data="manage_control")],

        [InlineKeyboardButton("📊 استيراد Excel", callback_data="excel_import_start"),
         InlineKeyboardButton("📊 تصدير Excel", callback_data="excel_export_start")],

        [InlineKeyboardButton("الأوسمة والإنجازات", callback_data="honors_achievements")],

        [InlineKeyboardButton("👨‍🏫 الصلاحيات", callback_data="manage_personnel"),
         InlineKeyboardButton("تكويد الكادر", callback_data="manage_coaches"),
         InlineKeyboardButton("المهام الإدارية", callback_data="administrative_tasks")],

        [InlineKeyboardButton("📁 إدارة الأقسام", callback_data="manage_cats"),
         InlineKeyboardButton("جداول المحاضرات", callback_data="schedules_lectures"),
         InlineKeyboardButton("📚 إدارة الدورات", callback_data="manage_courses")],

        [InlineKeyboardButton("إدارة المجموعات", callback_data="manage_group"),
         InlineKeyboardButton("المكتبة الشاملة", callback_data="manage_library"),
         InlineKeyboardButton("الأسئلة الشائعة", callback_data="frequently_guestions")],

        [InlineKeyboardButton("🎟 الكوبونات", callback_data="manage_coupons"),
         InlineKeyboardButton("📢 الإعلانات", callback_data="manage_ads"),
         InlineKeyboardButton("أكواد الخصم", callback_data="discount_codes")],

        [InlineKeyboardButton("🔙 عودة", callback_data="back_to_admin")]
    ]


    keyboards[3] = [
        [InlineKeyboardButton("🔗 نقاط الإحالة", callback_data="ref_points_join"),
         InlineKeyboardButton("💰 نقاط الشراء", callback_data="ref_points_purchase")],

        [InlineKeyboardButton("🎁 نقاط الاستبدال", callback_data="min_points_redeem"),
         InlineKeyboardButton("💱 وحدة العملة", callback_data="currency_unit")],

        [InlineKeyboardButton("📝 درجة الواجبات", callback_data="homework_grade"),
         InlineKeyboardButton("📉 الحد الأدنى للسحب", callback_data="maximum_withdrawal_marketers")],

        [InlineKeyboardButton("📊 نسبة المسوق", callback_data="marketers_commission"),
         InlineKeyboardButton("🏆 قناة الأوسمة", callback_data="honors_channel_id")],

        [InlineKeyboardButton("🎯 درجة النجاح الصغرى", callback_data="minimum_passing_gradee"),
         InlineKeyboardButton("🎯 درجة النجاح الكبرى", callback_data="greatest_success_gradee")],

        [InlineKeyboardButton("📢 القناة العامة", callback_data="public_channel_id"),
         InlineKeyboardButton("🔗 رابط الإحالة", callback_data="referral_link")],

        [InlineKeyboardButton("🔙 رجوع", callback_data="tech_settings")]
    ]

    keyboards[4] = [
        [InlineKeyboardButton("🔗 نقاط الإحالة", callback_data="ref_points_join"),
         InlineKeyboardButton("💰 نقاط الشراء", callback_data="ref_points_purchase")],

        [InlineKeyboardButton("🎁 نقاط الاستبدال", callback_data="min_points_redeem"),
         InlineKeyboardButton("💱 وحدة العملة", callback_data="currency_unit")],

        [InlineKeyboardButton("📝 درجة الواجبات", callback_data="homework_grade"),
         InlineKeyboardButton("📉 الحد الأدنى للسحب", callback_data="maximum_withdrawal_marketers")],

        [InlineKeyboardButton("📊 نسبة المسوق", callback_data="marketers_commission"),
         InlineKeyboardButton("🏆 قناة الأوسمة", callback_data="honors_channel_id")],

        [InlineKeyboardButton("🎯 درجة النجاح الصغرى", callback_data="minimum_passing_gradee"),
         InlineKeyboardButton("🎯 درجة النجاح الكبرى", callback_data="greatest_success_gradee")],

        [InlineKeyboardButton("📢 القناة العامة", callback_data="public_channel_id"),
         InlineKeyboardButton("🔗 رابط الإحالة", callback_data="referral_link")],

        [InlineKeyboardButton("🔙 رجوع", callback_data="tech_settings")]
    ]
    
    keyboards[5] = [
        # """لوحة تحكم المالك بوت المنصة التعليمية """
        [InlineKeyboardButton("📊 الإحصائيات الذكية", callback_data="admin_stats"), 
         InlineKeyboardButton("📡 الإذاعة المستهدفة", callback_data="smart_broadcast")],
        [InlineKeyboardButton("🛠 الإعدادات العامة وتجهيز النظام", callback_data="tech_settings")], 
        [InlineKeyboardButton("معلومات تجهيز النظام", callback_data="system_setup_information"), InlineKeyboardButton("ضبط الهوية", callback_data="contentcanager")],
        [InlineKeyboardButton("📥 تحميل نسخة احتياطية ", callback_data="export_data_json"),
         InlineKeyboardButton("📤 رفع نسخة بيانات", callback_data="import_data_json")],

        [InlineKeyboardButton("❌ إغلاق", callback_data="close_panel")] 
    ]
    
    keyboards[6] = [
        # """لوحة تحكم الموظف بوت المنصة التعليمية """
        [InlineKeyboardButton("📁 إدارة الأقسام", callback_data="manage_cats"), 
         InlineKeyboardButton("📚 إدارة الدورات", callback_data="manage_courses")],
        [InlineKeyboardButton("المكتبة الشاملة", callback_data="manage_library"),
         InlineKeyboardButton("الأوسمة والإنجازات", callback_data="honors_achievements")],
        [InlineKeyboardButton("إدارة المجموعات", callback_data="manage_group"), 
         InlineKeyboardButton("الأسئلة الشائعة", callback_data="frequently_guestions")],
        [InlineKeyboardButton("جداول المحاضرات", callback_data="schedules_lectures"), 
         InlineKeyboardButton("🎟 الكوبونات", callback_data="manage_coupons")],
        [InlineKeyboardButton("الكنترول", callback_data="manage_control")],
        [InlineKeyboardButton("🔙 عودة", callback_data="main_menu")]
    ]    
    
    keyboards[7] = [
        # """لوحة تحكم المدرب بوت المنصة التعليمية """
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
    
    keyboards[8] = [
        # """ لوحة تحكم الطالب بوت المنصة التعليمية """
        [InlineKeyboardButton("📚 استعراض الدورات", callback_data="view_categories")],
        [InlineKeyboardButton("👤 ملفي الدراسي", callback_data="my_profile"), 
         InlineKeyboardButton("🎟 تفعيل دورة", callback_data="activate_course")],
        [InlineKeyboardButton("💰 اربح دورات مجانية", callback_data="referral_system")],
        [InlineKeyboardButton("❓ الأسئلة الشائعة", callback_data="view_faq"),
         InlineKeyboardButton("💬 الدعم الفني", callback_data="contact_admin")]
    ]
    
    keyboards[9] = [
        # """لوحة تحكم الزائر بوت المنصة التعليمية """
        [InlineKeyboardButton("📚 استعراض الدورات", callback_data="view_categories")],
        [InlineKeyboardButton("💰 اربح دورات مجانية", callback_data="referral_system")],
        [InlineKeyboardButton("🎟 تفعيل دورة", callback_data="activate_course")],
        [InlineKeyboardButton("❓ الأسئلة الشائعة", callback_data="view_faq"),
         InlineKeyboardButton("💬 الدعم الفني", callback_data="contact_admin")]
    ]

    # 🔐 fallback (مهم جداً)
    keyboard = keyboards.get(kays, [[InlineKeyboardButton("⚠️ لوحة غير معروفة", callback_data="main_menu")]])

    return InlineKeyboardMarkup(keyboard)

def get_coach_panel_keyboard(owner_id=None):
    return get_keyboard(5)

def get_tech_settings_keyboard(owner_id=None):
    return get_keyboard(5)
