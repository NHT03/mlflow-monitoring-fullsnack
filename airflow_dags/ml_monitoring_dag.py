"""
============================================
ML MODEL & DRIFT MONITORING AIRFLOW DAG
============================================
Periodically inspects:
1. Model Serving API health
2. Evidently Drift status
3. Sends monitoring summary and alerts to Telegram
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import sys

# Include dags directory in python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from telegram_alert import send_telegram_message, task_failure_alert

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_failure_alert,
}

def check_api_health(**context):
    """Check FastAPI Model Serving status."""
    url = "http://api:8000/health"
    response = requests.get(url, timeout=10)
    data = response.json()
    
    if response.status_code != 200 or not data.get("model_loaded"):
        error_msg = f"FastAPI serving status is unhealthy: {data}"
        raise RuntimeError(error_msg)
        
    context['task_instance'].xcom_push(key='api_status', value=data)
    return data

def check_evidently_drift(**context):
    """Check Evidently service status and trigger drift check."""
    url = "http://evidently:8001/health"
    response = requests.get(url, timeout=10)
    data = response.json()
    
    if response.status_code != 200:
        raise RuntimeError(f"Evidently service health check failed: {response.status_code}")
        
    context['task_instance'].xcom_push(key='evidently_status', value=data)
    return data

def send_telegram_monitoring_summary(**context):
    """Send summary status to Telegram."""
    ti = context['task_instance']
    api_data = ti.xcom_pull(task_ids='check_model_api_health', key='api_status') or {}
    evidently_data = ti.xcom_pull(task_ids='check_evidently_drift', key='evidently_status') or {}
    
    model_name = api_data.get("model_name", "unknown")
    model_version = api_data.get("model_version", "unknown")
    uptime = round(api_data.get("uptime_seconds", 0), 1)
    
    reports_count = evidently_data.get("reports_count", 0)
    last_analysis = evidently_data.get("last_analysis") or "Chưa có"
    
    msg = (
        "📊 *BÁO CÁO GIÁM SÁT MÔ HÌNH ML*\n\n"
        f"• *Mô hình*: `{model_name}` (v{model_version})\n"
        f"• *Model Serving API*: 🟢 Healthy (Uptime: {uptime}s)\n"
        f"• *Evidently Drift Service*: 🟢 Active\n"
        f"• *Số lượng báo cáo Drift*: `{reports_count}`\n"
        f"• *Lần phân tích gần nhất*: `{last_analysis}`\n\n"
        f"🔗 [Truy cập Grafana](http://localhost:3000) | [MLflow](http://localhost:5000)\n"
        f"🕒 *Thời gian*: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}`"
    )
    
    send_telegram_message(msg)

with DAG(
    'ml_model_monitoring_pipeline',
    default_args=default_args,
    description='Pipeline định kỳ kiểm tra sức khỏe Model API & Drift và gửi Telegram',
    schedule_interval='@hourly',  # Chạy mỗi giờ
    catchup=False,
    tags=['monitoring', 'mlops', 'drift', 'telegram'],
) as dag:

    t1 = PythonOperator(
        task_id='check_model_api_health',
        python_callable=check_api_health,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='check_evidently_drift',
        python_callable=check_evidently_drift,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='send_telegram_monitoring_summary',
        python_callable=send_telegram_monitoring_summary,
        provide_context=True,
    )

    [t1, t2] >> t3
