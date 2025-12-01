@echo off
chcp 65001 >nul
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    🌱 Farm IoT Dashboard - تشغيل المشروع الكامل
echo ═══════════════════════════════════════════════════════════════
echo.
echo سيتم تشغيل المشروع بالكامل مع:
echo   ✅ Kafka + Zookeeper
echo   ✅ PostgreSQL Database
echo   ✅ InfluxDB
echo   ✅ Grafana
echo   ✅ IoT Producer (مولد البيانات)
echo   ✅ Spark Consumer (معالج البيانات)
echo   ✅ Backend API (FastAPI)
echo   ✅ Frontend Dashboard (الواجهة + Chatbot)
echo.
echo ═══════════════════════════════════════════════════════════════
echo.

REM Check if Docker is running
docker ps >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ❌ خطأ: Docker غير مشغل!
    echo الرجاء تشغيل Docker Desktop ثم المحاولة مرة أخرى.
    pause
    exit /b 1
)

echo ✅ Docker يعمل بشكل صحيح
echo.
echo 🧹 تنظيف الحاويات القديمة...
docker-compose down -v 2>nul

echo.
echo 🚀 بناء وتشغيل جميع الخدمات...
echo (قد يستغرق هذا بضع دقائق في المرة الأولى)
echo.

docker-compose up -d --build

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ فشل في تشغيل الحاويات
    echo الرجاء التحقق من الأخطاء أعلاه
    pause
    exit /b 1
)

echo.
echo ✅ تم تشغيل جميع الحاويات بنجاح!
echo.
echo ⏳ انتظار تهيئة الخدمات...
timeout /t 15 /nobreak >nul

echo.
echo ═══════════════════════════════════════════════════════════════
echo    ✅ المشروع جاهز للاستخدام!
echo ═══════════════════════════════════════════════════════════════
echo.
echo    🎨 لوحة التحكم الرئيسية:  http://localhost:3000
echo    🤖 يحتوي على Chatbot تفاعلي
echo    📊 يحتوي على Dashboard مباشر
echo.
echo    🔌 Backend API:              http://localhost:8000
echo    📈 Grafana:                  http://localhost:3001
echo    💾 InfluxDB:                 http://localhost:8086
echo.
echo للتحقق من حالة الخدمات:
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.
echo 🌐 فتح لوحة التحكم في المتصفح...
timeout /t 3 /nobreak >nul
start http://localhost:3000

echo.
echo ═══════════════════════════════════════════════════════════════
echo لإيقاف المشروع: docker-compose down
echo لمشاهدة السجلات: docker-compose logs -f [service-name]
echo ═══════════════════════════════════════════════════════════════
echo.
pause
