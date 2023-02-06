from pydantic import BaseModel, validator
from pyspark.pandas import DataFrame

from bitz.config.core import config


class TransactionsValidator(BaseModel):
    transactions: DataFrame
    
    class Config:
        arbitrary_types_allowed = True
    
    @validator('transactions')
    @classmethod
    def transactions_validator(cls, value):
        if not all(value.columns.isin(config.RFVClassificator_config.transactions_columns)):
            raise ValueError('"transactions" must have columns "aid", "tdate", "value", "ttype"')
        elif value['aid'].dtype != 'int32':
            raise ValueError('Column "aid" must be of type "int32"')
        elif value['tdate'].dtype != 'datetime64[ns]':
            raise ValueError('Column "tdate" must be of type "datetime64[ns]"')
        elif value['value'].dtype != 'float32':
            raise ValueError('Column "value" must be of type "float32"')
        elif value['ttype'].dtype != 'object':
            raise ValueError('Column "ttype" must be of type "object"')
        else:
            return value