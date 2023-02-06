# Databricks notebook source
import yaml

with open('./config.yml', "r") as conf_file:
    parsed_config = yaml.safe_load(conf_file)

from pydantic import BaseModel
from typing import Dict, List, Sequence

class MyModel(BaseModel):
    package_name: str
    group_order: List[str]
    mapper: Dict[int,str]
    
my_vars = MyModel(**parsed_config)

# COMMAND ----------

my_vars.mapper

# COMMAND ----------

with open('./config.yml', "r") as conf_file:
    parsed_config = yaml.safe_load(conf_file)
    
parsed_config

# COMMAND ----------

yaml.__with_libyaml__

# COMMAND ----------

my_vars.group_order

# COMMAND ----------

my_dict = {'package_name': 'bitz', 'group_order': ['stucked','hibernated','bonus hunter','sleepy, unengaged','sleepy, median','sleepy, high','awaken, unengaged','awaken, median','awaken, high','Hero!']}
