from datetime import timedelta

default_args = {
    'email': ['matiaspariente@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    "schedule_interval": '@hourly'
}
