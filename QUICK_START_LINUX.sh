#!/bin/bash
# 🌱 Farm IoT System - Quick Start Guide for Linux
# دليل البدء السريع على Linux

set -e

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║     🌱 Farm IoT System - Linux Quick Start (Arabic Guide)        ║"
echo "║     نظام مراقبة الصوبة الزراعية الذكية - دليل البدء السريع      ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# Function to display a step
display_step() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📌 الخطوة $1: $2"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
}

# Step 1: Check prerequisites
display_step "1" "فحص المتطلبات الأساسية"

echo "🔍 فحص Docker..."
if command -v docker &> /dev/null; then
    docker_version=$(docker --version)
    echo "✅ Docker مثبت: $docker_version"
else
    echo "❌ Docker غير مثبت. يرجى تثبيته من: https://docs.docker.com/engine/install/"
    exit 1
fi

echo ""
echo "🔍 فحص Docker Compose..."
if command -v docker-compose &> /dev/null; then
    compose_version=$(docker-compose --version)
    echo "✅ Docker Compose مثبت: $compose_version"
else
    echo "❌ Docker Compose غير مثبت. يرجى تثبيته من: https://docs.docker.com/compose/install/"
    exit 1
fi

# Step 2: Navigate to project directory
display_step "2" "الذهاب إلى مجلد المشروع"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"
echo "📁 المجلد الحالي: $SCRIPT_DIR"

# Step 3: Load environment variables
display_step "3" "تحميل متغيرات البيئة"

if [ -f ".env" ]; then
    echo "✅ ملف .env موجود"
    export $(cat "$SCRIPT_DIR/.env" | grep -v '#' | xargs)
    echo "   - KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"
    echo "   - POSTGRES_HOST: $POSTGRES_HOST"
    echo "   - PARQUET_BASE_PATH: $PARQUET_BASE_PATH"
else
    echo "⚠️  ملف .env غير موجود - سيتم استخدام القيم الافتراضية"
fi

# Step 4: Create required directories
display_step "4" "إنشاء المجلدات المطلوبة"

dirs=(
    "$HOME/spark_project_data/output"
    "$HOME/spark_project_data/farm_iot_parquet"
    "$HOME/spark_project_data/checkpoints/farm_iot_full_pipeline"
)

for dir in "${dirs[@]}"; do
    if mkdir -p "$dir"; then
        echo "✅ تم إنشاء: $dir"
    else
        echo "❌ فشل إنشاء: $dir"
    fi
done

# Step 5: Verify Linux compatibility
display_step "5" "التحقق من توافقية نظام Linux"

if [ -f "verify_linux_compatibility.sh" ]; then
    echo "🔍 تشغيل برنامج التحقق..."
    if ./verify_linux_compatibility.sh; then
        echo "✅ جميع الفحوصات نجحت!"
    else
        echo "⚠️  بعض الفحوصات قد تحتاج إلى مراجعة"
    fi
else
    echo "⚠️  برنامج التحقق غير موجود"
fi

# Step 6: Start the system
display_step "6" "بدء نظام Docker"

echo "📥 سحب أحدث صور Docker..."
docker-compose pull

echo ""
echo "🚀 بدء خدمات Docker Compose..."
docker-compose up -d

echo "⏳ الانتظار لمدة 10 ثوان حتى تبدأ الخدمات..."
sleep 10

# Step 7: Check service status
display_step "7" "فحص حالة الخدمات"

echo "📊 حالة الخدمات:"
docker-compose ps

# Step 8: Display access information
display_step "8" "معلومات الوصول"

echo "🌐 يمكنك الآن الوصول للخدمات على العناوين التالية:"
echo ""
echo "┌─────────────────────────────────────────────────────┐"
echo "│ الخدمة          │ العنوان          │ البيانات الاعتماد │"
echo "├─────────────────────────────────────────────────────┤"
echo "│ Grafana         │ http://localhost:3001  │ admin/admin      │"
echo "│ Backend API     │ http://localhost:8000  │ -                │"
echo "│ Frontend        │ http://localhost:3000  │ -                │"
echo "│ InfluxDB        │ http://localhost:8086  │ admin/admin      │"
echo "│ PostgreSQL      │ localhost:5432         │ spark_user/pw    │"
echo "│ Kafka           │ localhost:9092         │ -                │"
echo "└─────────────────────────────────────────────────────┘"
echo ""

# Step 9: Display useful commands
display_step "9" "أوامر مفيدة"

echo "📝 أوامر مفيدة لإدارة النظام:"
echo ""
echo "  🛑 إيقاف النظام:"
echo "     $ ./stop_system.sh"
echo ""
echo "  📊 عرض السجلات:"
echo "     $ docker-compose logs -f"
echo ""
echo "  📊 عرض سجلات خدمة محددة:"
echo "     $ docker-compose logs -f [service_name]"
echo ""
echo "  🔄 إعادة تشغيل النظام:"
echo "     $ ./stop_system.sh && ./start_system.sh"
echo ""
echo "  🧹 تنظيف كامل (حذف البيانات):"
echo "     $ docker-compose down -v"
echo ""
echo "  📋 عرض معلومات التعديلات:"
echo "     $ cat LINUX_MODIFICATIONS_SUMMARY.md"
echo ""

# Step 10: Final status
display_step "10" "الحالة النهائية"

echo "✨ تم البدء بنجاح!"
echo ""
echo "الخطوات التالية:"
echo "  1. افتح متصفح الويب وانتقل إلى: http://localhost:3001"
echo "  2. سجل الدخول باستخدام: admin / admin"
echo "  3. تحقق من لوحة التحكم والبيانات الواردة"
echo "  4. استخدم أوامر السجلات لمراقبة الخدمات"
echo ""
echo "📞 للحصول على المساعدة:"
echo "  • اطلع على README_LINUX.md"
echo "  • اطلع على LINUX_MODIFICATIONS_SUMMARY.md"
echo "  • عرض السجلات: docker-compose logs -f"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ تم إكمال دليل البدء السريع بنجاح! 🎉"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
