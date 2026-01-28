# Airflow Enterprise Toolkit - เอกสารภาษาไทย

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

เครื่องมือสำหรับองค์กรในการแปลงงานจาก Control-M ไปเป็น Apache Airflow DAGs โดยรองรับคุณสมบัติเฉพาะของ Control-M เช่น order dates และเงื่อนไขการ hold งาน

## คุณสมบัติหลัก

### ฟังก์ชันพื้นฐาน
- **การแยกวิเคราะห์ Control-M XML**: แยกวิเคราะห์นิยามงาน Control-M จากไฟล์ XML
- **การสร้าง Airflow DAG**: สร้าง Airflow DAGs ที่เทียบเท่ากับ task dependencies ที่เหมาะสม
- **การจัดการ Order Date**: รองรับกลยุทธ์ order date ของ Control-M (ปัจจุบัน, วันทำการที่แล้ว, offset แบบกำหนดเอง)
- **เงื่อนไขการ Hold งาน**: แปลงเงื่อนไขการ hold ของ Control-M (file holds, time holds, resource holds) ไปเป็น Airflow sensors
- **ประเภทงานหลายรูปแบบ**: รองรับ Command, Script, Database, และ File Transfer jobs
- **Clean Architecture**: โครงสร้างโค้ดมืออาชีพที่มีการแยกส่วนที่ชัดเจน

### คุณสมบัติขั้นสูง
- **การจัดการ Dependencies**: การแปลง dependencies ของงาน Control-M ไปเป็น task dependencies ของ Airflow โดยอัตโนมัติ
- **ตัวแปรสภาพแวดล้อม**: รักษาและแปลงตัวแปร Control-M ไปเป็นการกำหนดค่า task ของ Airflow
- **ความต้องการทรัพยากร**: แปลงข้อกำหนดทรัพยากร Control-M ไปเป็นการตั้งค่า task ของ Airflow
- **การตรวจสอบ**: การตรวจสอบงาน Control-M อย่างครอบคลุมก่อนการแปลง
- **การตรวจจับ Dependencies วงกลม**: ตรวจจับและป้องกัน dependencies วงกลมในนิยามงาน

## สถาปัตยกรรม

เครื่องมือนี้ใช้หลักการ clean architecture พร้อมการแยกเลเยอร์ที่ชัดเจน:

```
┌─────────────────┐
│  Presentation   │  ← CLI interface และการโต้ตอบกับผู้ใช้
├─────────────────┤
│   Application   │  ← Use cases และ business logic
├─────────────────┤
│     Domain      │  ← Core entities และ business rules
├─────────────────┤
│ Infrastructure  │  ← XML parsing, file I/O, code generation
└─────────────────┘
```

## การติดตั้ง

### ข้อกำหนดเบื้องต้น
- Rust 1.70 หรือสูงกว่า
- Python 3.8 หรือสูงกว่า
- Apache Airflow 2.0 หรือสูงกว่า (สำหรับ DAGs ที่สร้างขึ้น)

### การสร้างจาก Source

```bash
# Clone repository
git clone https://github.com/yourusername/airflow-enterprise-toolkit.git
cd airflow-enterprise-toolkit

# สร้าง Rust CLI tool
cargo build --release

# ติดตั้ง Python dependencies (ไม่จำเป็น, สำหรับ base classes)
pip install -r requirements.txt
```

## เริ่มต้นใช้งานอย่างรวดเร็ว

### 1. แปลง Control-M XML เป็น Airflow DAG

```bash
# การแปลงพื้นฐาน
./target/release/aet-cli convert --input control_m_jobs.xml --output-name my_dag

# พร้อมการตรวจสอบและ output JSON
./target/release/aet-cli convert \
    --input control_m_jobs.xml \
    --output-name my_dag \
    --validate \
    --json \
    --include-order-date

# ไดเรกทอรี output แบบกำหนดเอง
./target/release/aet-cli convert \
    --input control_m_jobs.xml \
    --output ./generated_dags \
    --output-name my_pipeline
```

### 2. ตรวจสอบ Control-M XML

```bash
# ตรวจสอบไฟล์ XML
./target/release/aet-cli validate --input control_m_jobs.xml

# โหมดการตรวจสอบแบบเข้มงวด
./target/release/aet-cli validate --input control_m_jobs.xml --strict
```

### 3. สร้างตัวอย่าง Control-M XML

```bash
# สร้างตัวอย่างพื้นฐาน
./target/release/aet-cli generate-example --output basic_example.xml --example-type basic

# สร้างตัวอย่างขั้นสูงพร้อม holds และ dependencies
./target/release/aet-cli generate-example --output advanced_example.xml --example-type advanced

# สร้างตัวอย่างเงื่อนไขการ hold
./target/release/aet-cli generate-example --output hold_example.xml --example-type hold
```

## ตัวอย่างการใช้งาน

### ตัวอย่าง Control-M XML

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CONTROLM>
    <FOLDER>
        <FOLDER_NAME>DataPipeline</FOLDER_NAME>
        <JOB>
            <JOBNAME>DataExtraction</JOBNAME>
            <DESCRIPTION>ดึงข้อมูลจากแหล่งที่มา</DESCRIPTION>
            <COMMAND>python3 extract.py --date={{ORDER_DATE}}</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <SCHEDULE>
                <DAYS>DAILY</DAYS>
                <ORDER_DATE>PreviousBusinessDay</ORDER_DATE>
            </SCHEDULE>
            <INCOND>
                <NAME>FILE_HOLD:/data/input.txt</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
        <JOB>
            <JOBNAME>DataTransformation</JOBNAME>
            <COMMAND>python3 transform.py --input={{ORDER_DATE}}_raw.csv</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <INCOND>
                <NAME>DataExtraction</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
    </FOLDER>
</CONTROLM>
```

### Airflow DAG ที่สร้างขึ้น

```python
"""
สร้างจาก Control-M โดย Airflow Enterprise Toolkit

งานต้นฉบับ: 2
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': 2023, 1, 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'control_m_datapipeline',
    default_args=default_args,
    description='Control-M migration DAG',
    schedule_interval='@daily',
    catchup=False,
    tags=['control-m-migration'],
    max_active_runs=1,
    is_paused=False,
)

# นิยาม task
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.txt',
    poke_interval=60,
    timeout=3600,
    mode='reschedule',
    dag=dag,
)

data_extraction = BashOperator(
    task_id='data_extraction',
    dag=dag,
    bash_command="python3 extract.py --date={{ ds }}",
)

data_transformation = BashOperator(
    task_id='data_transformation',
    dag=dag,
    bash_command="python3 transform.py --input={{ ds }}_raw.csv",
)

# Dependencies ของ task
wait_for_file >> data_extraction >> data_transformation

# การกำหนดค่า order date สำหรับ data_extraction
# กลยุทธ์: PreviousBusinessDay
# Template: {{ macros.ds_add(ds, -1) }}
```

## Base Classes ของ Python

เครื่องมือนี้มี base classes ของ Python สำหรับสร้าง DAGs ที่เข้ากันได้กับ Control-M:

```python
from airflow_enterprise_toolkit.dag_base import create_control_m_dag

# สร้าง DAG ที่เข้ากันได้กับ Control-M
dag = create_control_m_dag(
    dag_id="my_control_m_pipeline",
    schedule_interval="@daily",
    order_date_strategy="previous_business_day"
)

# สร้าง tasks พร้อมคุณสมบัติของ Control-M
task = dag.create_control_m_task(
    task_id="my_task",
    command="python3 script.py --date={{ order_date }}",
    order_date_param=True,
    hold_conditions=[{"type": "file", "value": "/data/input.txt"}]
)
```

## การกำหนดค่า

### การกำหนดค่า CLI

เครื่องมือ CLI สามารถกำหนดค่าได้โดยใช้ command-line arguments:

```bash
./target/release/aet-cli --help
```

ตัวเลือกหลัก:
- `--output`: ไดเรกทอรี output สำหรับไฟล์ที่สร้างขึ้น
- `--config`: เส้นทางไฟล์การกำหนดค่า
- `--verbose`: เปิดใช้งาน logging แบบละเอียด

## การพัฒนา

### การรัน Tests

```bash
# รัน Rust tests
cargo test

# รัน tests พร้อม coverage
cargo test --coverage

# รัน Python tests (ถ้ามี)
python -m pytest tests/
```

### รูปแบบโค้ด

โปรเจคนี้ปฏิบัติตามมาตรฐานการเขียนโค้ดมืออาชีพ:

- **Rust**: ใช้ `rustfmt` และ `clippy` สำหรับการจัดรูปแบบและ linting
- **Python**: ปฏิบัติตาม PEP 8 พร้อม type hints และ docstrings
- **เอกสาร**: คอมเมนต์โค้ดอย่างครอบคลุมและเอกสาร API

## การสนับสนุนคุณสมบัติ Control-M

### ✅ คุณสมบัติที่รองรับ

| คุณสมบัติ | Control-M | Airflow ที่เทียบเท่า | สถานะ |
|---------|-----------|-------------------|---------|
| Order Dates | `ORDER_DATE` | `{{ ds }}`, `ds_add` | ✅ เต็มรูปแบบ |
| File Holds | `FILE_HOLD` | `FileSensor` | ✅ เต็มรูปแบบ |
| Time Holds | `TIME_HOLD` | `TimeSensor` | ✅ เต็มรูปแบบ |
| Job Dependencies | `INCOND` | Task dependencies | ✅ เต็มรูปแบบ |
| Environment Variables | `VARIABLE` | Task environment | ✅ เต็มรูปแบบ |
| Resource Requirements | Timeout, memory | Task configuration | ✅ เต็มรูปแบบ |
| Multiple Job Types | Command, Script, DB | Various operators | ✅ เต็มรูปแบบ |

### 🚧 รองรับบางส่วน

| คุณสมบัติ | Control-M | Airflow ที่เทียบเท่า | สถานะ |
|---------|-----------|-------------------|---------|
| Complex Schedules | Calendars, holidays | Custom schedules | 🚧 พื้นฐาน |
| Conditional Logic | IF/THEN conditions | Branching | 🚧 พื้นฐาน |
| Sub-folders | Folder hierarchy | Task groups | 🚧 พื้นฐาน |

### ❌ ไม่รองรับ

| คุณสมบัติ | Control-M | Airflow ที่เทียบเท่า | สถานะ |
|---------|-----------|-------------------|---------|
| GUI-based configuration | Control-M client | Code-based | ❌ ไม่เกี่ยวข้อง |
| Real-time monitoring | Control-M monitoring | Airflow UI | ❌ ต่างกัน |
| Job execution engine | Control-M agent | Airflow executor | ❌ ต่างกัน |

## การมีส่วนร่วม

1. Fork repository
2. สร้าง feature branch (`git checkout -b feature/amazing-feature`)
3. Commit การเปลี่ยนแปลง (`git commit -m 'Add amazing feature'`)
4. Push ไปยัง branch (`git push origin feature/amazing-feature`)
5. เปิด Pull Request

### แนวทางการพัฒนา

- ปฏิบัติตามรูปแบบโค้ดและสถาปัตยกรรมที่มีอยู่
- เพิ่ม tests อย่างครอบคลุมสำหรับคุณสมบัติใหม่
- อัปเดตเอกสารสำหรับการเปลี่ยนแปลง API
- ตรวจสอบให้แน่ใจว่า tests ทั้งหมดผ่านก่อนส่ง PR

## ใบอนุญาต

โปรเจคนี้ได้รับอนุญาตภายใต้ใบอนุญาต MIT - ดูที่ไฟล์ [LICENSE](LICENSE)

## การสนับสนุน

- **เอกสาร**: [เอกสารฉบับเต็ม](docs/)
- **ปัญหา**: [GitHub Issues](https://github.com/yourusername/airflow-enterprise-toolkit/issues)
- **การสนทนา**: [GitHub Discussions](https://github.com/yourusername/airflow-enterprise-toolkit/discussions)

## แผนงาน

### เวอร์ชัน 0.2.0 (วางแผน)
- [ ] การสนับสนุนการกำหนดเวลาขั้นสูงพร้อม calendars
- [ ] การจัดการเงื่อนไขการ hold ที่ซับซ้อนมากขึ้น
- [ ] Web UI สำหรับการจัดการการแปลง
- [ ] การผสานรวมกับฐานข้อมูลยอดนิยม

### เวอร์ชัน 0.3.0 (วางแผน)
- [ ] ความสามารถในการแปลงเป็นชุด
- [ ] การจัดการข้อผิดพลาดและการกู้คืนขั้นสูง
- [ ] การปรับปรุงประสิทธิภาพสำหรับชุดงานขนาดใหญ่
- [ ] ระบบปลั๊กอินสำหรับประเภทงานแบบกำหนดเอง

---

**Airflow Enterprise Toolkit** - โซลูชันการย้ายข้อมูลจาก Control-M ไปยัง Airflow แบบมืออาชีพ
