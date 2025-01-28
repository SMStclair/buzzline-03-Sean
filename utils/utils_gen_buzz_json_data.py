import json
import random

# Define the base messages and authors
messages = [
    "I love Python!",
    "Kafka is awesome.",
    "Streaming data is fun.",
    "This is a buzz message.",
    "Have a great day!",
    "Data engineering is my passion.",
    "JSON makes life easier.",
    "Distributed systems are fascinating.",
    "Hello, Kafka world!",
    "Buzz messages for everyone."
]
authors = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

# Generate 100 random messages with authors
buzz_data = [
    {"message": random.choice(messages), "author": random.choice(authors)}
    for _ in range(100)
]

# Save to a JSON file
output_file = "buzz.json"
with open(output_file, "w") as file:
    json.dump(buzz_data, file, indent=4)

output_file
