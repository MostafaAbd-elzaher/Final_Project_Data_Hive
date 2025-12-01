@echo off
chcp 65001 >nul
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    ⏹️ إيقاف Farm IoT Dashboard
echo ═══════════════════════════════════════════════════════════════
echo.

echo هل تريد:
echo   [1] إيقاف الخدمات فقط (الاحتفاظ بالبيانات)
echo   [2] إيقاف وحذف جميع البيانات (تنظيف كامل)
echo   [0] إلغاء
echo.

set /p choice="اختر رقم: "

if "%choice%"=="1" (
    echo.
    echo ⏹️ إيقاف الخدمات...
    docker-compose down
    echo.
    echo ✅ تم إيقاف جميع الخدمات
    echo 💾 البيانات محفوظة - يمكنك إعادة التشغيل لاحقاً
) else if "%choice%"=="2" (
    echo.
    echo ⚠️ تحذير: سيتم حذف جميع البيانات!
    echo.
    set /p confirm="هل أنت متأكد؟ (yes/no): "
    if /i "!confirm!"=="yes" (
        echo.
        echo 🧹 إيقاف وحذف جميع البيانات...
        docker-compose down -v
        echo.
        echo ✅ تم حذف جميع الخدمات والبيانات
        echo 🔄 المشروع جاهز للتشغيل من جديد
    ) else (
        echo.
        echo ❌ تم الإلغاء
    )
) else if "%choice%"=="0" (
    echo.
    echo ❌ تم الإلغاء
) else (
    echo.
    echo ❌ اختيار غير صحيح!
)

echo.
pause
