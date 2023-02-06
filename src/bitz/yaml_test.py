# Databricks notebook source
email_message = """\
message:
  date: 2022-01-16 12:46:17Z
  from: john.doe@domain.com
  to:
    - bobby@domain.com
    - molly@domain.com
  cc:
    - jane.doe@domain.com
  subject: Friendly reminder
  content: |
    Dear XYZ,

    Lorem ipsum dolor sit amet...
  attachments:
    image1.gif: !!binary
        R0lGODdhCAAIAPAAAAIGAfr4+SwAA
        AAACAAIAAACDIyPeWCsClxDMsZ3CgA7
"""

import yaml

with open('./config.yml', "r") as conf_file:
    parsed_config = yaml.safe_load(conf_file)

from pydantic import BaseModel
from typing import Dict, List, Sequence

class MyModel(BaseModel):
    package_name: str
    group_order: List[str]
    
my_vars = MyModel(**parsed_config)

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
