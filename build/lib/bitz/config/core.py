from pathlib import Path
from typing import Dict, List, Sequence

from pydantic import BaseModel
from strictyaml import YAML, load

import bitz

# Project Directories
PACKAGE_ROOT = Path(bitz.__file__).resolve().parent
CONFIG_FILE_PATH = PACKAGE_ROOT / "config.yml"


class AppConfig(BaseModel):
    """
    Application-level config.
    """
    package_name: str


class RFVClassificatorConfig(BaseModel):
    """
    All configuration relevant to RFVClassificator.
    """
    transactions_columns: List[str]
    group_order: List[str]
    mapper: Dict[int:str]


class Config(BaseModel):
    """Master config object."""
    app_config: AppConfig
    RFVClassificator_config: RFVClassificatorConfig


def find_config_file() -> Path:
    """Locate the configuration file."""
    if CONFIG_FILE_PATH.is_file():
        return CONFIG_FILE_PATH
    raise Exception(f"Config not found at {CONFIG_FILE_PATH!r}")


def fetch_config_from_yaml(cfg_path: Path = None) -> YAML:
    """Parse YAML containing the package configuration."""

    if not cfg_path:
        cfg_path = find_config_file()

    if cfg_path:
        with open(cfg_path, "r") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the data attribute from the strictyaml YAML type.
    _config = Config(
        app_config=AppConfig(**parsed_config.data),
        RFVClassificator_config=RFVClassificatorConfig(**parsed_config.data),
    )

    return _config


config = create_and_validate_config()
