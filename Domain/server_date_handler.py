from datetime import date, datetime

def get_current_date():
    return date.today()

def get_current_time():
    return datetime.now().time().replace(microsecond=0)