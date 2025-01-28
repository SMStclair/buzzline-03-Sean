import pandas as pd
from datetime import datetime, timedelta

# Constants
start_time = datetime(2025, 1, 1, 15, 0, 0)  # Start at 3:00 PM
readings_per_minute = 1  # Realistic readings: 1 per minute
total_minutes = 240  # 4 hours (can adjust to 300 for 5 hours)
stall_start_temp = 150.0
stall_end_temp = 170.0
final_temp = 205.0
stall_start_time = 180  # Stall starts after ~3 hours (180 minutes)
stall_duration = 60  # Stall lasts for 1 hour (60 minutes)

# Generate time intervals
timestamps = [start_time + timedelta(minutes=i) for i in range(total_minutes)]

# Temperature progression
temperatures = []
current_temp = 70.0  # Start at room temperature
for i in range(total_minutes):
    if i < stall_start_time:  # Before the stall
        current_temp += 0.4  # Gradual increase
    elif stall_start_time <= i < stall_start_time + stall_duration:  # During the stall
        current_temp += (0.2 if i % 2 == 0 else -0.2)  # Fluctuation
    else:  # After the stall
        current_temp += 0.5  # Gradual post-stall rise
    temperatures.append(round(current_temp, 1))

# Create the DataFrame
data = pd.DataFrame({"timestamp": timestamps, "temperature": temperatures})

# Save to CSV
csv_file_path = "smoker_temps.csv"
data.to_csv(csv_file_path, index=False)
