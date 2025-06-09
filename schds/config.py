import yaml
from pydantic import BaseModel


class SchdsConfig(BaseModel):
    db_url: str = "sqlite:///./schds.sqlite3"


def read_config(filepath:str=None) -> SchdsConfig:
    if filepath is None:
        filepath = 'conf/schds.yaml'

    with open(filepath, "r") as f:
        config_dict = yaml.safe_load(f)

    config = SchdsConfig(**config_dict)
    return config
