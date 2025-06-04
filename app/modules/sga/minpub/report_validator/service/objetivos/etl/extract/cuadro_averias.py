# word_extractor.py

from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from docx import Document

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("CuadroAveriasProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_schema(headers: List[str]) -> StructType:
    """
    Creates a dynamic schema based on the table headers.
    
    Args:
        headers (List[str]): List of column headers from the table
        
    Returns:
        StructType: PySpark schema for the DataFrame
    """
    # Add 'responsable' field to the schema
    fields = [StructField(header, StringType(), True) for header in headers]
    fields.append(StructField("responsable", StringType(), True))
    return StructType(fields)

def process_table(table, headers: List[str], responsable: str, spark: SparkSession) -> DataFrame:
    """
    Process a single table and convert it to a PySpark DataFrame.
    
    Args:
        table: Word document table
        headers (List[str]): List of column headers
        responsable (str): Responsible party (CLARO, CLIENTE, TERCEROS)
        spark (SparkSession): Active SparkSession
        
    Returns:
        DataFrame: PySpark DataFrame containing the table data
    """
    records: List[Dict] = []
    for row in table.rows[1:]:
        vals = [cell.text.strip() for cell in row.cells]
        
        # Pad values if row has fewer cells than headers
        if len(vals) < len(headers):
            vals += [""] * (len(headers) - len(vals))
            
        # Create record with all headers and add responsable
        record = dict(zip(headers, vals))
        record['responsable'] = responsable
        records.append(record)
    
    # Create DataFrame with the appropriate schema
    return spark.createDataFrame(records, schema=create_schema(headers))

@log_exceptions
def extract_averias_table(path_docx: str) -> DataFrame:
    """
    Load the specified tables from a .docx and return them as a PySpark DataFrame.
    index = 4 (CLARO)
    index = 5 (CLIENTE)
    index = 6 (TERCEROS)
    We assume the first row is the header.
    
    Args:
        path_docx (str): Path to the Word document
        
    Returns:
        DataFrame: PySpark DataFrame containing all tables data
    """
    table_index_list = [4, 5, 6]
    doc = Document(path_docx)
    
    # Create SparkSession
    spark = get_spark_session()
    
    try:
        all_dfs: List[DataFrame] = []
        
        for table_index in table_index_list:
            if table_index >= len(doc.tables):
                raise IndexError(f"Document only has {len(doc.tables)} tables.")
            
            table = doc.tables[table_index]
            headers = [cell.text.strip() for cell in table.rows[0].cells]
            
            # Determine responsable based on table index
            responsable = 'CLARO' if table_index == 4 else 'CLIENTE' if table_index == 5 else 'TERCEROS'
            
            # Process table and create DataFrame
            df_part = process_table(table, headers, responsable, spark)
            all_dfs.append(df_part)
        
        # Union all DataFrames
        result = all_dfs[0]
        for df in all_dfs[1:]:
            result = result.unionByName(df)
        
        # Cache the result for better performance if it will be used multiple times
        result.cache()
        
        return result
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error processing Word document: {str(e)}")
    finally:
        spark.stop()
