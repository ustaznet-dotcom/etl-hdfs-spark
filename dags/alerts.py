import os
import requests


def send_telegram_alert(context):
    """
    Эта функция вызывается автоматически когда любой таск падает.
    context — это словарь Airflow с информацией о запуске.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("Telegram credentials not set, skipping alert")
        return

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = (
        f"Pipeline Failed!\n\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {execution_date}\n"
        f"Logs: {log_url}"
    )

    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
    )

    if response.status_code == 200:
        print("Telegram alert sent successfully")
    else:
        print(f"Failed to send alert: {response.text}")
