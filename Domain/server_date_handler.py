from datetime import datetime

# TODO: Implement this function with actual logic
def get_current_date():
    return "2024-05-11"

def get_current_time():
    return datetime.now().time().replace(microsecond=0)