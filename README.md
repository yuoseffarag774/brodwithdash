# 📡 Broadcast Bot — Web Dashboard

لوحة تحكم ويب للبوت، تسمح بتنفيذ كل الأوامر من المتصفح مع تسجيل دخول عبر Discord OAuth2.

## المميزات
- 🔐 تسجيل دخول عبر Discord OAuth2
- 🔑 إدارة التوكنات (إضافة / حذف / عرض الحالة)
- 📦 إدارة المجموعات (إنشاء / حذف)
- 👑 إدارة الـ Owners
- 📢 بث رسائل مع إحصائيات مباشرة (WebSocket)
- 🤖 تغيير أفاتار وأسماء البوتات

## الإعداد

### 1. تثبيت المكتبات
```bash
cd dashboard
npm install
```

### 2. إعداد OAuth2 في Discord Developer Portal
1. اذهب إلى https://discord.com/developers/applications
2. افتح التطبيق → **OAuth2**
3. أضف Redirect URI:
   - Local: `http://localhost:3000/auth/callback`
   - Production: `https://yourdomain.com/auth/callback`
4. انسخ **Client ID** و **Client Secret**

### 3. إعداد ملف `.env`
```bash
cp .env.example .env
# عدّل القيم في .env
```

يجب أن تكون ملفات `.env` و `data.json` في المجلد الأب (نفس مكان ملف بوت الأوامر).

الهيكل:
```
project/
├── index.js          ← بوت الأوامر
├── data.json         ← البيانات المشتركة
├── .env              ← متغيرات البيئة
└── dashboard/
    ├── server.js
    ├── package.json
    └── public/
        └── index.html
```

### 4. تشغيل الداشبورد
```bash
npm start
# أو للتطوير:
npm run dev
```

ثم افتح: http://localhost:3000

### 5. إعداد الصلاحيات
أول مرة يسجل فيها شخص دخوله عبر Discord، يجب أن يكون **Admin** في السيرفر أو يُضاف عبر أمر البوت أولاً:
```
$add-owner YOUR_DISCORD_USER_ID
```

## تشغيل البوت والداشبورد معاً
```bash
# Terminal 1 — البوت
node index.js

# Terminal 2 — الداشبورد
cd dashboard && npm start
```
