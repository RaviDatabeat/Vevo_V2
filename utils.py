import logging.config
import os
import yaml
from dotenv import load_dotenv
import os

# Load variables from .env into os.environ
load_dotenv() 


def setup_logging():
    # Ensure the `logs` directory exists before configuring file handlers

    # Load configuration safely and apply it
    with open("logging_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)


def get_env(key: str) -> str:
    value = os.getenv(key)

    if not value:
        raise ValueError(f"Env variable {key} not found")

    return value


def name_shortner(name_str: str, max_length: int = 60):
    if len(name_str) > max_length:
        return f"{name_str[:max_length]} ..."
    else:
        return name_str