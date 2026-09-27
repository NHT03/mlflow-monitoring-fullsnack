"""
============================================
TELEGRAM NOTIFICATION MODULE FOR AIRFLOW
============================================
Provides utility functions and Airflow callbacks
to send alerts and status updates to Telegram.
"""

import os
import requests
import logging

logger = logging.getLogger(__name__)

def get_telegram_config():
    """Retrieve Telegram credentials from environment variables."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    return bot_token, chat_id

def send_telegram_message(message: str, parse_mode: str = "Markdown") -> bool:
    """
    Send a message to the configured Telegram chat.
    Returns True if sent successfully, False otherwise.
    """
    bot_token, chat_id = get_telegram_config()
    
    if not bot_token or not chat_id or bot_token == "your_telegram_bot_token_here":
        logger.warning(
            "Telegram credentials not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing/default). "
            "Skipping notification."
        )
        return False
        
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Successfully sent Telegram notification.")
            return True
        else:
            logger.error(f"Failed to send Telegram message: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Exception while sending Telegram message: {e}")
        return False

def task_failure_alert(context):
    """Airflow on_failure_callback to notify on task failure."""
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

    message = (
        f"🚨 *AIRFLOW TASK FAILED!*\n\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Time*: `{execution_date}`\n"
        f"• *Error*: `{str(exception)[:300]}`\n\n"
        f"[View Task Logs]({log_url})"
    )
    send_telegram_message(message)

def task_success_alert(context):
    """Airflow on_success_callback to notify on task success."""
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')

    message = (
        f"✅ *AIRFLOW TASK SUCCEEDED*\n\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Time*: `{execution_date}`\n"
    )
    send_telegram_message(message)
