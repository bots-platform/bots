from typing import List
from fastapi import HTTPException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def validate_required_columns_from_excel(path_excel: str, required_columns: List[str], skipfooter: int = 0) -> DataFrame:
    """
    Reads an Excel file and validates that it contains the required columns, optionally
    skipping the last `skipfooter` rows. Uses PySpark for efficient distributed processing.

    Parameters
    ----------
    path_excel : str
        Path to the .xlsx file.
    required_columns : List[str]
        List of column names that must exist.
    skipfooter : int, optional
        Number of rows at the end of the file to ignore (default 0).

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with only the required columns, without footer rows.
    
    Raises
    ------
    ValueError
        If the file cannot be read or columns are missing.
    """
    try:
        # Create SparkSession if not exists
        spark = SparkSession.builder.getOrCreate()
        
        # Read Excel file using Spark's Excel reader
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("skipFooter", str(skipfooter)) \
            .load(path_excel)
            
    except Exception as e:
        raise ValueError(f"Error 400: No se pudo leer el Excel: {e}")

    # Clean column names
    df = df.select([F.col(c).alias(c.strip()) for c in df.columns])

    # Check for missing columns
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Faltan columnas requeridas: {missing}"
        )

    # Select only required columns
    df_clean = df.select(required_columns)

    # Get string columns
    string_cols = [field.name for field in df_clean.schema.fields 
                  if isinstance(field.dataType, StringType)]

    # Clean text columns
    for col_name in string_cols:
        df_clean = df_clean.withColumn(
            col_name,
            F.regexp_replace(
                F.regexp_replace(
                    F.trim(F.col(col_name).cast(StringType())),
                    '\r', ' '
                ),
                '_x000D_', ''
            )
        )

    # Fill null values in string columns
    for col_name in string_cols:
        df_clean = df_clean.withColumn(
            col_name,
            F.coalesce(F.col(col_name), F.lit("No disponible"))
        )

    return df_clean
