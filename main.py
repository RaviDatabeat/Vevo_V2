import logging.config
import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

load_dotenv() 

def setup_logging():
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
        # Load configuration safely and apply it
    config_path = script_dir / "logging_config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Update log file paths to be relative to script directory
    # This ensures logs are created in a predictable location regardless of working directory
    if "handlers" in config:
        for handler_name, handler_config in config["handlers"].items():
            if "filename" in handler_config:
                # Convert relative log file path to absolute path relative to script directory
                log_filename = handler_config["filename"]
                handler_config["filename"] = str(script_dir / log_filename)

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
