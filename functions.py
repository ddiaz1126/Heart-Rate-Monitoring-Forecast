# This file is to store the functions of this project
from datetime import datetime, timezone

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
    
def convert_nanoseconds_to_datetime(timestamp):
    try:
        # Ensure the input is a valid number
        timestamp = int(timestamp)
        # Convert nanoseconds to seconds and create a naive datetime object
        return datetime.utcfromtimestamp(timestamp / 1e9)
    except (ValueError, TypeError):
        # Return None for invalid or null inputs
        return None
    
def convert_speed(elapsed_seconds, distance_meters):
    """Function to create speed based on elapsed seconds and distance in meters."""
    if elapsed_seconds is None or distance_meters is None:
        return None

    try:
        # Convert distance from meters to miles
        distance_miles = distance_meters * 0.000621371
    except (ValueError, TypeError):
        return None

    if elapsed_seconds <= 0:
        return None

    speed = distance_miles / (elapsed_seconds / 3600)  # convert seconds to hours
    return round(speed, 2)  # Output speed rounded to two decimal places