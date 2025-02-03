"""
csv_consumer_jaya.py
Consume json messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
import polars as pl
#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

def detect_temp_spike(current_temp: float, rolling_window: deque) -> None:
    """Detect if the temperature change exceeds 3°F from the last reading."""
    if len(rolling_window) > 0:
        last_temp = rolling_window[-1]  # Get the most recent temperature
        temp_change = abs(current_temp - last_temp)
        if temp_change > 3:
            logger.info(f"TEMPERATURE SPIKE DETECTED! Change of {temp_change}°F detected.")

#####################################
# Define a function to detect a stall
#####################################


def detect_stall(rolling_window_deque: deque) -> bool:
    
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        # We don't have a full deque yet
        # Keep reading until the deque is full
        logger.debug(
            f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}."
        )
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int, df: pl.DataFrame) -> pl.DataFrame:
    """Process a single message and check for stalls or sudden temperature changes."""
    try:
        logger.debug(f"Raw message: {message}")
        data = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return df  # Return the unmodified DataFrame

        # Detect temperature spikes
        detect_temp_spike(float(temperature), rolling_window)

        # Add the new temperature reading to the rolling window
        rolling_window.append(temperature)

        # Append the new reading to the Polars DataFrame
        new_row = pl.DataFrame({"timestamp": [str(timestamp)], "temperature": [float(temperature)]})
        df = df.vstack(new_row)  # Create a new DataFrame with the added row

        # Check for a stall
        if detect_stall(rolling_window):
            logger.info(f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings.")

        return df  # Return the updated DataFrame

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

    return df  # Ensure function always returns a DataFrame

#####################################
# Define main function for this module
#####################################


def main() -> None:
    
    """Main entry point for the consumer."""
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)
    df = pl.DataFrame(
    {"timestamp": [], "temperature": []}, 
    schema={"timestamp": pl.Utf8, "temperature": pl.Float64}  # Explicit schema
    )

    # Create the Kafka consumer using the utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            df = process_message(message_str, rolling_window, window_size, df)  # Update df

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
