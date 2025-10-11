import json
import time
import random
import math
from datetime import datetime
import csv
import os
from kafka import KafkaProducer  # --- تعديل Kafka --- : استيراد المكتبة


class GreenhouseSensorSimulator:
    def __init__(self, location="Cairo, Egypt", output_dir=".", kafka_topic='IotRealItimeSys'):
        self.location = location
        self.output_dir = output_dir

        os.makedirs(self.output_dir, exist_ok=True)
        
        base_filename = f"greenhouse_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.csv_filename = os.path.join(self.output_dir, f"{base_filename}.csv")
        self.json_filename = os.path.join(self.output_dir, f"{base_filename}.json")

        self.previous_values = {
            'soil_temperature': 25.0, 'air_temperature': 28.0, 'soil_humidity': 60.0,
            'air_humidity': 65.0, 'soil_ph': 6.5, 'soil_salinity': 2.0,
            'light_intensity': 50000, 'water_level': 75.0
        }
        
        with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'timestamp', 'date', 'time', 'season', 'day_period',
                'soil_temperature_c', 'air_temperature_c', 'soil_humidity_percent',
                'air_humidity_percent', 'soil_ph', 'soil_salinity_ds_m',
                'light_intensity_lux', 'water_level_percent', 'location'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # --- تعديل Kafka --- : تهيئة الـ Producer عند إنشاء الكائن
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.kafka_topic = kafka_topic
            print("✅ تم الاتصال بـ Kafka بنجاح.")
        except Exception as e:
            print(f"❌ فشل الاتصال بـ Kafka: {e}")
            self.kafka_producer = None

    # ... (جميع الدوال الأخرى تبقى كما هي بدون تغيير) ...

    def get_season_and_period(self, dt):
        """تحديد الفصل وفترة اليوم بناءً على التاريخ والوقت"""
        month = dt.month
        hour = dt.hour
        # تحديد الفصل
        if month in [12, 1, 2]:
            season = "winter"
        elif month in [3, 4, 5]:
            season = "spring"
        elif month in [6, 7, 8]:
            season = "summer"
        else:
            season = "autumn"
        # تحديد فترة اليوم
        if 5 <= hour < 12:
            day_period = "morning"
        elif 12 <= hour < 18:
            day_period = "afternoon"
        elif 18 <= hour < 22:
            day_period = "evening"
        else:
            day_period = "night"
        return season, day_period

    def get_base_temperatures(self, season, day_period):
        """الحصول على درجات الحرارة الأساسية حسب الفصل والوقت (مناخ القاهرة)"""
        base_temps = {
            "winter": {
                "morning": {"air": 18, "soil": 16},
                "afternoon": {"air": 25, "soil": 22},
                "evening": {"air": 20, "soil": 19},
                "night": {"air": 12, "soil": 15}
            },
            "spring": {
                "morning": {"air": 25, "soil": 22},
                "afternoon": {"air": 32, "soil": 28},
                "evening": {"air": 28, "soil": 26},
                "night": {"air": 20, "soil": 23}
            },
            "summer": {
                "morning": {"air": 30, "soil": 28},
                "afternoon": {"air": 38, "soil": 35},
                "evening": {"air": 35, "soil": 33},
                "night": {"air": 26, "soil": 30}
            },
            "autumn": {
                "morning": {"air": 22, "soil": 20},
                "afternoon": {"air": 28, "soil": 25},
                "evening": {"air": 24, "soil": 23},
                "night": {"air": 18, "soil": 21}
            }
        }
        return base_temps[season][day_period]

    def get_light_intensity(self, hour, season):
        """حساب شدة الإضاءة بناءً على الوقت والفصل"""
        if 6 <= hour <= 18:  # ساعات النهار
            # منحنى جيبي لمحاكاة شروق وغروب الشمس
            day_progress = (hour - 6) / 12  # من 0 إلى 1
            base_intensity = math.sin(day_progress * math.pi) * 80000
            # تعديل حسب الفصل
            season_multiplier = {
                "summer": 1.2,
                "spring": 1.0,
                "autumn": 0.9,
                "winter": 0.7
            }
            intensity = base_intensity * season_multiplier[season]
            # إضافة تغيير عشوائي طفيف
            intensity += random.uniform(-5000, 5000)
            return max(0, min(100000, intensity))
        else:
            # ليلاً - إضاءة اصطناعية في الصوبة
            return random.uniform(100, 1000)

    def smooth_transition(self, current_value, target_value, max_change=0.2):
        """انتقال سلس جداً بين القيم لمحاكاة واقعية أكثر"""
        difference = target_value - current_value
        if abs(difference) <= max_change:
            return target_value
        else:
            # تغيير تدريجي جداً
            change = max_change * 0.3 if abs(difference) > max_change else difference * 0.1
            return current_value + (change if difference > 0 else -change)

    def generate_sensor_data(self):
        """توليد بيانات الأجهزة الاستشعار"""
        now = datetime.now()
        season, day_period = self.get_season_and_period(now)
        base_temps = self.get_base_temperatures(season, day_period)
        # درجة حرارة الهواء (مع انتقال سلس جداً)
        target_air_temp = base_temps["air"] + random.uniform(-0.5, 0.5)
        air_temperature = self.smooth_transition(
            self.previous_values['air_temperature'],
            target_air_temp,
            max_change=0.1
        )
        # درجة حرارة التربة (أكثر استقراراً من الهواء)
        target_soil_temp = base_temps["soil"] + random.uniform(-0.3, 0.3)
        soil_temperature = self.smooth_transition(
            self.previous_values['soil_temperature'],
            target_soil_temp,
            max_change=0.05
        )
        # رطوبة الهواء (تتأثر بالفصل ودرجة الحرارة)
        base_air_humidity = {
            "winter": 70, "spring": 60, "summer": 50, "autumn": 65
        }[season]
        humidity_adjustment = (air_temperature - 25) * -0.5
        target_air_humidity = base_air_humidity + humidity_adjustment + random.uniform(-1, 1)
        air_humidity = self.smooth_transition(
            self.previous_values['air_humidity'],
            max(30, min(90, target_air_humidity)),
            max_change=0.3
        )
        # رطوبة التربة (أكثر استقراراً)
        target_soil_humidity = self.previous_values['soil_humidity'] + random.uniform(-0.5, 0.5)
        soil_humidity = self.smooth_transition(
            self.previous_values['soil_humidity'],
            max(40, min(85, target_soil_humidity)),
            max_change=0.2
        )
        # حموضة التربة (مستقرة جداً)
        target_ph = self.previous_values['soil_ph'] + random.uniform(-0.05, 0.05)
        soil_ph = self.smooth_transition(
            self.previous_values['soil_ph'],
            max(5.5, min(7.5, target_ph)),
            max_change=0.02
        )
        # ملوحة التربة (مستقرة جداً)
        target_salinity = self.previous_values['soil_salinity'] + random.uniform(-0.05, 0.05)
        soil_salinity = self.smooth_transition(
            self.previous_values['soil_salinity'],
            max(1.0, min(3.0, target_salinity)),
            max_change=0.01
        )
        # شدة الإضاءة
        light_intensity = self.get_light_intensity(now.hour, season)
        # مستوى الماء (يقل ببطء شديد)
        target_water = self.previous_values['water_level'] - random.uniform(0.01, 0.05)
        if target_water < 15:
            target_water = random.uniform(85, 95)
        water_level = max(0, min(100, target_water))
        # تحديث القيم السابقة
        self.previous_values.update({
            'soil_temperature': soil_temperature,
            'air_temperature': air_temperature,
            'soil_humidity': soil_humidity,
            'air_humidity': air_humidity,
            'soil_ph': soil_ph,
            'soil_salinity': soil_salinity,
            'light_intensity': light_intensity,
            'water_level': water_level
        })
        sensor_data = {
            'timestamp': now.isoformat(),
            'date': now.strftime('%Y-%m-%d'),
            'time': now.strftime('%H:%M:%S'),
            'season': season,
            'day_period': day_period,
            'soil_temperature_c': round(soil_temperature, 2),
            'air_temperature_c': round(air_temperature, 2),
            'soil_humidity_percent': round(soil_humidity, 2),
            'air_humidity_percent': round(air_humidity, 2),
            'soil_ph': round(soil_ph, 2),
            'soil_salinity_ds_m': round(soil_salinity, 2),
            'light_intensity_lux': round(light_intensity, 2),
            'water_level_percent': round(water_level, 2),
            'location': self.location
        }
        return sensor_data

    def save_to_csv(self, data):
        """حفظ البيانات في ملف CSV"""
        with open(self.csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = list(data.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)

    def save_to_json(self, data):
        """حفظ البيانات في ملف JSON"""
        try:
            with open(self.json_filename, 'r', encoding='utf-8') as jsonfile:
                existing_data = json.load(jsonfile)
        except FileNotFoundError:
            existing_data = []
        existing_data.append(data)
        with open(self.json_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(existing_data, jsonfile, indent=2, ensure_ascii=False)

    def display_data(self, data):
        """عرض البيانات على الشاشة مع تنسيق موحد للأرقام"""
        print("\n" + "="*70)
        print("🌱 بيانات أجهزة الاستشعار - الصوبة الزراعية")
        print("="*70)
        print(f"📅 التاريخ والوقت: {data['date']} | {data['time']}")
        print(f"🌍 الموقع: {data['location']}")
        print(f"🗓️ الفصل: {data['season']} | فترة اليوم: {data['day_period']}")
        print("-"*70)
        prev_soil_temp = self.previous_values.get('soil_temperature', data['soil_temperature_c'])
        prev_air_temp = self.previous_values.get('air_temperature', data['air_temperature_c'])
        soil_temp_change = data['soil_temperature_c'] - prev_soil_temp
        air_temp_change = data['air_temperature_c'] - prev_air_temp
        soil_temp_arrow = "📈" if soil_temp_change > 0.01 else "📉" if soil_temp_change < -0.01 else "➡️"
        air_temp_arrow = "📈" if air_temp_change > 0.01 else "📉" if air_temp_change < -0.01 else "➡️"
        print(f"🌡️ درجة حرارة التربة: {data['soil_temperature_c']:.2f}°C {soil_temp_arrow} ({soil_temp_change:+.2f})")
        print(f"🌡️ درجة حرارة الهواء: {data['air_temperature_c']:.2f}°C {air_temp_arrow} ({air_temp_change:+.2f})")
        print(f"💧 رطوبة التربة: {data['soil_humidity_percent']:.2f}%")
        print(f"💨 رطوبة الهواء: {data['air_humidity_percent']:.2f}%")
        print(f"⚗️ حموضة التربة (pH): {data['soil_ph']:.2f}")
        print(f"🧂 ملوحة التربة: {data['soil_salinity_ds_m']:.2f} dS/m")
        print(f"☀️ شدة الإضاءة: {data['light_intensity_lux']:,.2f} lux")
        print(f"🚰 مستوى الماء: {data['water_level_percent']:.2f}%")
        alerts = []
        if data['soil_temperature_c'] > 35:
            alerts.append("⚠️ تحذير: درجة حرارة التربة مرتفعة!")
        if data['air_temperature_c'] > 40:
            alerts.append("⚠️ تحذير: درجة حرارة الهواء مرتفعة جداً!")
        if data['soil_humidity_percent'] < 40:
            alerts.append("⚠️ تحذير: رطوبة التربة منخفضة!")
        if data['water_level_percent'] < 20:
            alerts.append("🚨 تنبيه: مستوى الماء منخفض - يحتاج إعادة ملء!")
        if data['soil_ph'] < 6.0 or data['soil_ph'] > 7.5:
            alerts.append("⚠️ تحذير: مستوى حموضة التربة خارج النطاق المثالي!")
        if alerts:
            print("-"*70)
            for alert in alerts:
                print(alert)

    # --- تعديل Kafka --- : دالة جديدة لإرسال البيانات إلى Kafka
    def send_to_kafka(self, data):
        if self.kafka_producer:
            try:
                self.kafka_producer.send(self.kafka_topic, value=data)
                self.kafka_producer.flush()  # نضمن إرسال الرسالة فوراً
                print(f"📬 تم إرسال البيانات إلى Kafka Topic: '{self.kafka_topic}'")
            except Exception as e:
                print(f"🔥 خطأ أثناء الإرسال إلى Kafka: {e}")

    def run_infinite(self, interval_seconds=5):
        """تشغيل المحاكاة بشكل لا نهائي"""
        print("🚀 بدء محاكاة أجهزة الاستشعار...")
        print(f"⏱️ التشغيل: مستمر | فترة القراءة: {interval_seconds} ثانية")
        print(f"💾 حفظ البيانات في: {os.path.abspath(self.output_dir)}")
        print("⏹️ للإيقاف: اضغط Ctrl+C")
        reading_count = 0
        try:
            while True:
                reading_count += 1
                sensor_data = self.generate_sensor_data()
                self.display_data(sensor_data)
                self.save_to_csv(sensor_data)
                self.save_to_json(sensor_data)
                # --- تعديل Kafka --- : استدعاء دالة الإرسال إلى Kafka
                self.send_to_kafka(sensor_data)
                print(f"\n📊 القراءة رقم: {reading_count} | الوقت: {datetime.now().strftime('%H:%M:%S')}")
                print(f"💾 تم حفظ البيانات | التالية خلال {interval_seconds} ثوان...")
                print("="*80)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n\n⏹️ تم إيقاف المحاكاة بواسطة المستخدم")
            if self.kafka_producer:  # --- تعديل Kafka --- : إغلاق الاتصال عند إيقاف التشغيل
                self.kafka_producer.close()
                print("🔒 تم إغلاق الاتصال بـ Kafka.")
            print(f"✅ إجمالي القراءات: {reading_count}")
            print("📁 البيانات محفوظة في:")
            print(f"  - {self.csv_filename}")
            print(f"  - {self.json_filename}")


def main():

    print("🌱 مرحباً بك في محاكي أجهزة الاستشعار للصوبة الزراعية")
    print("="*60)
        # Use a save path that exists in the workspace (Producer folder)
        save_path = r"/mnt/E/MyCareer/DepiData/DataHive/FinalProject/Producer/output"
    # --- تعديل Kafka --- : تحديد اسم الـ Topic
    kafka_topic_name = 'IotRealItimeSys'

    simulator = GreenhouseSensorSimulator(
        location="القاهرة، مصر",
        output_dir=save_path,
        kafka_topic=kafka_topic_name
    )
    simulator.run_infinite(interval_seconds=5)


if __name__ == "__main__":
    main()
