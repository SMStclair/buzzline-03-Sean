"""
csv_consumer_case.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

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


#####################################
# Define a function to detect a stall
#####################################


def detect_stall(rolling_window_deque: deque) -> bool:
    """
    Detect a temperature stall based on the rolling window.

    Args:
        rolling_window_deque (deque): Rolling window of temperature readings.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        # We don't have a full deque yet
        # Keep reading until the deque is full
        logger.debug(
            f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}."
        )
        return False

    # Once the deque is full we can calculate the temperature range
    # Use Python's built-in min() and max() functions
    # If the range is less than or equal to the threshold, we have a stall
    # And our food is ready :)
    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Check for a stall
        if detect_stall(rolling_window):
            logger.info(
                f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
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
