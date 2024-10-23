# This file is to store the functions of this project

def convert_time_to_seconds(time_str):
    """This function is to correctly transform the time metrics in strings cotaining colons in between into their respective unit"""
    try:
        # Split the time string into parts
        parts = list(map(int, time_str.split(':')))

        if len(parts) == 3:
            hours, minutes, seconds = parts
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:
            minutes, seconds = parts
            return minutes + seconds / 60
        else:
            return None
    except Exception:
        return None