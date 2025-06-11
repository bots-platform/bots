from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    handle_null_values_spark
)

def preprocess_380(df):
    # Convert date columns to timestamp
    df = df.withColumn('startdate', F.to_timestamp('startdate', 'dd/MM/yyyy'))
    df = df.withColumn('enddate', F.to_timestamp('enddate', 'dd/MM/yyyy'))
    
    # Handle null values using PySpark version
    df = handle_null_values_spark(df)
    
    return df

