import os
import yaml
from pydantic import BaseModel


class SchdsConfig(BaseModel):
    db_url: str = "sqlite:///./data/schds.sqlite3"


def read_config(filepath:str=None) -> SchdsConfig:
    if filepath is None:
        filepath = 'conf/schds.yaml'
    
    if not os.path.exists(filepath):
        return SchdsConfig()

    with open(filepath, "r") as f:
        config_dict = yaml.safe_load(f)

    config = SchdsConfig(**config_dict)
    return config
