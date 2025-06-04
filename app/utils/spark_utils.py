from typing import List, Dict, Any, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import json

def read_excel_to_spark(spark, file_path: str, sheet_name: str = None) -> DataFrame:
    """
    Read Excel file into Spark DataFrame using spark-excel package.
    """
    return spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sheetName", sheet_name) \
        .load(file_path)

def read_csv_to_spark(spark, file_path: str, delimiter: str = ",") -> DataFrame:
    """
    Read CSV file into Spark DataFrame.
    """
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", delimiter) \
        .load(file_path)

def spark_df_to_json(df: DataFrame) -> List[Dict[str, Any]]:
    """
    Convert Spark DataFrame to JSON-serializable list of dictionaries.
    Only collect to driver when necessary for API response.
    """
    return [row.asDict() for row in df.collect()]

def rename_columns(df: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
    """
    Rename columns in Spark DataFrame using a mapping dictionary.
    """
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def filter_null_values(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Filter out rows where any of the specified columns contain null values.
    """
    return df.na.drop(subset=columns)

def join_dataframes(df1: DataFrame, df2: DataFrame, 
                   join_type: str = "inner",
                   join_columns: Union[str, List[str]] = None) -> DataFrame:
    """
    Join two Spark DataFrames with specified join type and columns.
    """
    if isinstance(join_columns, str):
        join_columns = [join_columns]
    return df1.join(df2, on=join_columns, how=join_type)

def group_by_agg(df: DataFrame, 
                group_columns: List[str],
                agg_columns: Dict[str, str]) -> DataFrame:
    """
    Group by specified columns and apply aggregations.
    agg_columns should be a dict mapping column names to aggregation functions.
    Example: {"amount": "sum", "count": "count"}
    """
    return df.groupBy(group_columns).agg(*[
        f"{func}({col}) as {col}_{func}" 
        for col, func in agg_columns.items()
    ])

def create_schema_from_dict(schema_dict: Dict[str, str]) -> StructType:
    """
    Create Spark schema from dictionary mapping column names to types.
    Supported types: "string", "integer", "double", "date"
    """
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": DateType()
    }
    
    fields = [
        StructField(name, type_mapping[type_name])
        for name, type_name in schema_dict.items()
    ]
    
    return StructType(fields)

def validate_schema(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains all required columns.
    """
    return all(col in df.columns for col in required_columns)

def cast_columns(df: DataFrame, type_mapping: Dict[str, str]) -> DataFrame:
    """
    Cast columns to specified types.
    type_mapping should be a dict mapping column names to types.
    Supported types: "string", "integer", "double", "date"
    """
    for col_name, type_name in type_mapping.items():
        df = df.withColumn(
            col_name,
            col(col_name).cast(type_name)
        )
    return df 