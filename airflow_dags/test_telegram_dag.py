"""
============================================
TEST TELEGRAM NOTIFICATION DAG
============================================
Allows testing Telegram connection directly
from Airflow UI by triggering this DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Include dags directory in python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from telegram_alert import send_telegram_message, task_failure_alert

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': task_failure_alert,
}

def send_test_notification():
    """Send a test message to Telegram."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    
    if not bot_token or not chat_id or bot_token == "your_telegram_bot_token_here":
        raise ValueError(
            "TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa được điền trong file .env!\n"
            "Vui lòng cập nhật TELEGRAM_BOT_TOKEN và TELEGRAM_CHAT_ID trong file .env."
        )

    msg = (
        "🎉 *KẾT NỐI TELEGRAM THÀNH CÔNG!*\n\n"
        "• *Dịch vụ*: Apache Airflow Monitoring\n"
        "• *Trạng thái*: Hoạt động bình thường\n"
        f"• *Thời gian kiểm tra*: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}`\n\n"
        "Sẵn sàng nhận các báo cáo Data Drift và cảnh báo từ hệ thống MLOps! 🚀"
    )
    success = send_telegram_message(msg)
    if not success:
        raise RuntimeError("Không gửi được tin nhắn tới Telegram. Vui lòng kiểm tra lại Bot Token và Chat ID.")

with DAG(
    'test_telegram_notification',
    default_args=default_args,
    description='DAG kiểm tra gửi thông báo tới Telegram',
    schedule_interval=None,  # Chạy thủ công
    catchup=False,
    tags=['monitoring', 'telegram', 'test'],
) as dag:

    test_task = PythonOperator(
        task_id='send_test_telegram_message',
        python_callable=send_test_notification,
    )
