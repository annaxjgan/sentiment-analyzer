from datetime import datetime

def parse_date(timestamp):
    """
    Function to parse the date string and return a formatted date string
    :param timestamp: Timestamp string in ISO format
    :return: Formatted date string in 'YYYY-MM-DD HH:MM-HH:MM' format
    """
    try:
        # Parse the timestamp and convert to date string
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        datetime_str = dt.date().isoformat()
        hour = dt.hour
        date_str = datetime_str + " " + f"{hour}:00-{hour+1}:00"
        return date_str
    except ValueError:
        # Skip processing for invalid timestamps
        return None