# استخدام نسخة بايثون مستقرة وخفيفة
FROM python:3.11-slim

# ضبط مجلد العمل داخل الحاوية
WORKDIR /app

# تثبيت أدوات النظام الضرورية (تم إضافة ffmpeg لدعم بوت التحميل)
RUN apt-get update && apt-get install -y \
    build-essential \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# نسخ ملف المتطلبات أولاً لتسريع البناء (Caching)
COPY requirements.txt .

# تثبيت المكتبات البرمجية
# تغيير بسيط لإجبار الدوكر على إعادة التثبيت
RUN pip install --no-cache-dir --force-reinstall -r requirements.txt

# التأكد من وجود مجلد الموديولات
RUN mkdir -p modules

# نسخ كافة ملفات المشروع إلى الحاوية
COPY . .



# الأمر المشغل للبوت الرئيسي
CMD ["python", "main.py"]
