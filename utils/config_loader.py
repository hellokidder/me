import os
import yaml
from dotenv import load_dotenv


def load_config(config_dir: str = "config") -> dict:
    env_path = os.path.join(config_dir, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    yaml_path = os.path.join(config_dir, "config.yaml")
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config
