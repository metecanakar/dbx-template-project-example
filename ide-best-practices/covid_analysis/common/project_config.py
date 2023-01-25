import json

def create_config(conf_path: str):
    with open(conf_path, "r") as f:
        config_dict = json.load(f)
    return config_dict