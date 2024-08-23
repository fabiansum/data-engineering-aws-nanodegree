import logging
import os

# Ensure the logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a file handler to log messages to a file (append mode)
file_handler = logging.FileHandler('./logs/project.log', mode='a')
file_handler.setLevel(logging.INFO)

# Create a stream handler to log messages to the terminal
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Define a common log format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
