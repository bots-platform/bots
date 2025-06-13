from datetime import datetime
import os
from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, lit, concat_ws, when, first
import pandas as pd
from pyspark.sql import SparkSession

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.corte_excel import extract_corte_excel
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.run import run_objetivo_1
from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_2.run import run_objetivo_2
from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_3.run import run_objetivo_3

# EXTRACT IMPORTS
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.cuadro_averias import (
    extract_averias_table
) 
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.informe_tecnico import (
    extract_tecnico_reports_without_hours_last_dates
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.anexo_indisponibilidad import (
    extract_indisponibilidad_anexos
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.sga_335 import extract_sga_335

# TRANSFORM IMPORTS
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.averias import ( 
    preprocess_df_word_averias
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.informe_tecnico import ( 
    preprocess_df_word_informe_tecnico
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.anexos import ( 
    preprocess_df_word_anexos_indisponibilidad
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.sga_335 import ( 
    preprocess_335
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.sga_380 import ( 
    preprocess_380
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.cuismp_sharepoint import ( 
    preprocess_df_cid_cuismp_sharepoint
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.transform.corte_excel import ( 
    preprocess_corte_excel
)

# MERGE IMPORTS
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.excel_sga.excel_sga import ( 
    merge_sga_335_corte_excel_sharepoint_cuismp_sga380
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.cuadro_averias.datos import ( 
    merge_word_datos_averias_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.cuadro_averias.telefonia import ( 
    merge_word_telefonia_averias_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.informe_tecnico.datos import ( 
    merge_word_datos_informe_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.informe_tecnico.telefonia import ( 
    merge_word_telefonia_informe_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.anexo_indisponibilidad.datos import ( 
    merge_word_datos_anexos_disponibilidad_df_merged_sga
)
from app.modules.sga.minpub.report_validator.service.objetivos.etl.merge.anexo_indisponibilidad.telefonia import ( 
    merge_word_telefonia_anexos_disponibilidad_df_merged_sga
)

def create_empty_schema() -> StructType:
    """
    Creates an empty schema for DataFrames when no data is available.
    """
    return StructType([
        StructField("ticket", StringType(), True),
        StructField("indisponibilidad_header", StringType(), True),
        StructField("indisponibilidad_periodos", StringType(), True),
        StructField("indisponibilidad_footer", StringType(), True),
        StructField("indisponibilidad_total", StringType(), True)
    ])

@log_exceptions
def all_objetivos(
    path_corte_excel: str, 
    path_sga_dinamico_335: str, 
    path_sga_dinamico_380: str,
    path_cid_cuismp_sharepoint: str,
    word_datos_file_path: str,
    word_telefonia_file_path: str
) -> List[Dict]:
    """
    Calls each objective's validation function and combines the results.
    Each objective function returns a Pandas DataFrame with the columns:
      - 'numero de incidencia'
      - 'mensaje'
      - 'objetivo'
    
    Returns:
      A list of dictionaries (one per incident that fails at least one validation)
      across all objectives.
    """
    try:
        with spark_manager.get_session_context():
            # Read Excel files using pandas
            df_sga_dinamico_380_pd = pd.read_excel(path_sga_dinamico_380)
            # Convert pandas DataFrame to Spark DataFrame
            df_sga_dinamico_380 = spark_manager.get_session().createDataFrame(df_sga_dinamico_380_pd)
            
            # Read other Excel files
            df_cid_cuismp_sharepoint_pd = pd.read_excel(path_cid_cuismp_sharepoint)
            df_cid_cuismp_sharepoint = spark_manager.get_session().createDataFrame(df_cid_cuismp_sharepoint_pd)
            
            df_corte_excel = extract_corte_excel(path_corte_excel, skipfooter=0)
            df_sga_dinamico_335 = extract_sga_335(path_sga_dinamico_335)
            df_word_datos_averias = extract_averias_table(word_datos_file_path)
            df_word_telefonia_averias = extract_averias_table(word_telefonia_file_path)
            df_word_datos_informe_tec = extract_tecnico_reports_without_hours_last_dates(word_datos_file_path)
            df_word_telefonia_informe_tec = extract_tecnico_reports_without_hours_last_dates(word_telefonia_file_path)

            raw_datos_anexos = extract_indisponibilidad_anexos(word_datos_file_path)
            raw_tel_anexos = extract_indisponibilidad_anexos(word_telefonia_file_path)
            
            df_word_datos_anexos_indis = (
                raw_datos_anexos
                if raw_datos_anexos is not None
                else spark_manager.get_session().createDataFrame([], create_empty_schema())
            )
            df_word_telefonia_anexos_indis = (
                raw_tel_anexos
                if raw_tel_anexos is not None
                else spark_manager.get_session().createDataFrame([], create_empty_schema())
            )

            # TRANSFORM INVOQUE
            df_word_datos_averias = preprocess_df_word_averias(df_word_datos_averias)
            df_word_telefonia_averias = preprocess_df_word_averias(df_word_telefonia_averias)
            
            df_word_datos_informe_tec = preprocess_df_word_informe_tecnico(df_word_datos_informe_tec)
            df_word_telefonia_informe_tec = preprocess_df_word_informe_tecnico(df_word_telefonia_informe_tec)

            df_word_datos_anexos_indis = preprocess_df_word_anexos_indisponibilidad(df_word_datos_anexos_indis)
            df_word_telefonia_anexos_indis = preprocess_df_word_anexos_indisponibilidad(df_word_telefonia_anexos_indis)

            df_sga_dinamico_335 = preprocess_335(df_sga_dinamico_335)
            df_sga_dinamico_380 = preprocess_380(df_sga_dinamico_380)
            df_corte_excel = preprocess_corte_excel(df_corte_excel)
            df_cid_cuismp_sharepoint = preprocess_df_cid_cuismp_sharepoint(df_cid_cuismp_sharepoint)

            # Cache frequently used DataFrames
            df_corte_excel.cache()
            df_sga_dinamico_335.cache()
            df_sga_dinamico_380.cache()
            df_cid_cuismp_sharepoint.cache()

            # MERGE INVOQUE
            # SGA 335 - 380 - SHAREPOINT - CORTE - BOTH
            df_matched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
                df_corte_excel, df_sga_dinamico_335,
                df_cid_cuismp_sharepoint, df_sga_dinamico_380,
                'both'
            )
            
            # SGA 335 - 380 - SHAREPOINT - CORTE - LEFT ONLY
            df_unmatched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
                df_corte_excel,
                df_sga_dinamico_335,
                df_cid_cuismp_sharepoint,
                df_sga_dinamico_380,
                'left_only'
            )
            
            # AVERIAS - DATOS - EXCEL
            df_matched_word_datos_averias_corte_excel = merge_word_datos_averias_corte_excel(
                df_word_datos_averias,
                df_corte_excel,
                'both'
            )
           
            # AVERIAS - TELEFONIA - EXCEL
            df_matched_word_telefonia_averias_corte_excel = merge_word_telefonia_averias_corte_excel(
                df_word_telefonia_averias,
                df_corte_excel,
                'both'
            )
            
            # INFORME TECNICO - DATOS - EXCEL
            df_matched_word_datos_informe_tecnico_corte_excel = merge_word_datos_informe_corte_excel(
                df_word_datos_informe_tec,
                df_corte_excel,
                'both'
            )
            
            # INFORME TECNICO - TELEFONIA - EXCEL
            df_matched_word_telefonia_informe_tecnico_corte_excel = merge_word_telefonia_informe_corte_excel(
                df_word_telefonia_informe_tec,
                df_corte_excel,
                'both'
            )

            # Filter components using Spark SQL
            df_componente_ii = df_matched_corte_sga335_Sharepoint_cuismp_sga380.filter(
                col("COMPONENTE") == "COMPONENTE II"
            )

            # ANEXOS INDISPONIBILIDAD - DATOS - EXCEL
            df_matched_word_datos_anexo_indisponibilidad_df_merged_sga = merge_word_datos_anexos_disponibilidad_df_merged_sga(
                df_componente_ii,
                df_word_datos_anexos_indis,
            )

            df_componente_iv = df_matched_corte_sga335_Sharepoint_cuismp_sga380.filter(
                col("COMPONENTE") == "COMPONENTE IV"
            )
            
            # ANEXOS INDISPONIBILIDAD - TELEFONIA - EXCEL
            df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga = merge_word_telefonia_anexos_disponibilidad_df_merged_sga(
                df_componente_iv,
                df_word_telefonia_anexos_indis,
            )

            # RUN OBJETIVOS 
            obj1_df = run_objetivo_1(
                df_matched_corte_sga335_Sharepoint_cuismp_sga380,
                df_unmatched_corte_sga335_Sharepoint_cuismp_sga380
            )

            obj2_df = run_objetivo_2(
                df_matched_word_datos_averias_corte_excel,
                df_matched_word_telefonia_averias_corte_excel,
                df_matched_word_datos_informe_tecnico_corte_excel,
                df_matched_word_telefonia_informe_tecnico_corte_excel
            )
            
            obj3_df = run_objetivo_3(
                df_matched_word_datos_anexo_indisponibilidad_df_merged_sga,
                df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga
            )   

            # Combine results using pandas
            df_all = pd.concat([obj1_df, obj2_df, obj3_df], ignore_index=True)

            # Group and aggregate using pandas
            df_grouped = df_all.groupby("nro_incidencia").agg({
                "mensaje": lambda x: " | ".join(x),
                "objetivo": lambda x: " | ".join(x),
                "TIPO REPORTE": "first"
            }).reset_index()

            # Save results
            output_dir = 'media/minpub/validator_report/extract/final/'
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            path_reporte_sga = os.path.join(output_dir, f'validator_report_{timestamp}.xlsx')
            
            # Save to Excel
            df_grouped.to_excel(path_reporte_sga, index=False, engine='openpyxl')
            print(f"validator reporte guardado en: {path_reporte_sga}")

            # Cleanup
            df_corte_excel.unpersist()
            df_sga_dinamico_335.unpersist()
            df_sga_dinamico_380.unpersist()
            df_cid_cuismp_sharepoint.unpersist()

            return df_grouped.to_dict('records')
    except Exception as e:
        print(f"Error in all_objetivos: {e}")
        return []
    finally:
        # Clean up resources
        if 'df_sga_dinamico_380' in locals() and df_sga_dinamico_380.is_cached:
            df_sga_dinamico_380.unpersist()
        if 'df_cid_cuismp_sharepoint' in locals() and df_cid_cuismp_sharepoint.is_cached:
            df_cid_cuismp_sharepoint.unpersist()




