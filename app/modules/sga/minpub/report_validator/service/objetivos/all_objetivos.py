
from typing import List, Dict
import pandas as pd

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.objetivo_1 import validation_objetivo_1
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.objetivo_2 import validation_objetivo_2
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_3.objetivo_3 import validation_objetivo_3

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.word_averias_extractor import extract_averias_table
from app.modules.sga.minpub.report_validator.service.objetivos.calculations import extract_tecnico_reports_without_hours_last_dates
from app.modules.sga.minpub.report_validator.service.objetivos.calculations import extract_indisponibilidad_anexos

from app.modules.sga.minpub.report_validator.service.objetivos.preprocessing import ( 
    preprocess_335, preprocess_380, preprocess_corte_excel,
    preprocess_df_cid_cuismp_sharepoint, preprocess_df_word_datos_averias,
    preprocess_df_word_telefonia_averias, preprocess_df_word_datos_informe_tecnico,
    preprocess_df_word_telefonia_informe_tecnico ,preprocess_df_word_datos_anexos_indis,
    preprocess_df_word_telefonia_anexos_indis,
)

from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_sga_335_corte_excel_sharepoint_cuismp_sga380 import ( 
merge_sga_335_corte_excel_sharepoint_cuismp_sga380
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_datos_averias_corte_excel import ( 
merge_word_datos_averias_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_telefonia_averias_corte_excel import ( 
merge_word_telefonia_averias_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_datos_informe_corte_excel import ( 
merge_word_datos_informe_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_telefonia_informe_corte_excel import ( 
merge_word_telefonia_informe_corte_excel
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_datos_anexos_disponibilidad_dfs_merged_sga import ( 
merge_word_datos_anexos_disponibilidad_df_merged_sga
)
from app.modules.sga.minpub.report_validator.service.objetivos.mergers.merge_word_telefonia_anexos_disponibilidad_dfs_merged_sga import ( 
merge_word_telefonia_anexos_disponibilidad_df_merged_sga
)


@log_exceptions
def all_objetivos(
    path_corte_excel, 
    path_sgq_dinamico_335, 
    path_sga_dinamico_380,
    path_cid_cuismp_sharepoint,
    word_datos_file_path,
    word_telefonia_file_path

) -> List[Dict]:
    """
    Calls each objective's validation function and combines the results.
    Each objective function returns a DataFrame with the columns:
      - 'numero de incidencia'
      - 'mensaje'
      - 'objetivo'
    
    Returns:
      A list of dictionaries (one per incident that fails at least one validation)
      across all objectives.
    """
    results = []
    
    # extract
    df_corte_excel = pd.read_excel(path_corte_excel, skipfooter=2, engine="openpyxl")
    df_sga_dinamico_335 = pd.read_excel(path_sgq_dinamico_335) 
    df_sga_dinamico_380 = pd.read_excel(path_sga_dinamico_380)
    df_cid_cuismp_sharepoint = pd.read_excel(path_cid_cuismp_sharepoint)
    df_word_datos_averias =  extract_averias_table(word_datos_file_path)
    df_word_telefonia_averias = extract_averias_table(word_telefonia_file_path)
    df_word_datos_informe_tec =  extract_tecnico_reports_without_hours_last_dates(word_datos_file_path)
    df_word_telefonia_informe_tec = extract_tecnico_reports_without_hours_last_dates(word_telefonia_file_path)
    df_word_datos_anexos_indis =  extract_indisponibilidad_anexos(word_datos_file_path)
    df_word_telefonia_anexos_indis = extract_indisponibilidad_anexos(word_telefonia_file_path)


    # preprocess
    df_word_datos_averias = preprocess_df_word_datos_averias(df_word_datos_averias)
    df_word_telefonia_averias = preprocess_df_word_telefonia_averias(df_word_telefonia_averias)
    
    df_word_datos_informe_tec =  preprocess_df_word_datos_informe_tecnico(df_word_datos_informe_tec)
    df_word_telefonia_informe_tec = preprocess_df_word_telefonia_informe_tecnico(df_word_telefonia_informe_tec)

    df_word_datos_anexos_indis = preprocess_df_word_datos_anexos_indis(df_word_datos_anexos_indis)
    df_word_telefonia_anexos_indis = preprocess_df_word_telefonia_anexos_indis(df_word_telefonia_anexos_indis)

    df_sga_dinamico_335 = preprocess_335(df_sga_dinamico_335)
    df_sga_dinamico_380 = preprocess_380(df_sga_dinamico_380)
    df_corte_excel = preprocess_corte_excel(df_corte_excel)
    df_cid_cuismp_sharepoint = preprocess_df_cid_cuismp_sharepoint(df_cid_cuismp_sharepoint)
    

    # mergers 
    #  SGA 335 - 380 - SHAREPOINT - CORTE - BOTH
    df_matched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel, df_sga_dinamico_335,
        df_cid_cuismp_sharepoint, df_sga_dinamico_380,
        'both'
        )
    
    #  SGA 335 - 380 - SHAREPOINT - CORTE - LEFT ONLY
    df_unmatched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel,
        df_sga_dinamico_335,
        df_cid_cuismp_sharepoint,
        df_sga_dinamico_380,
        'left_only'
        )
    
    # AVERIAS - DATOS - EXCEL
    df_matched_word_datos_averias_corte_excel = merge_word_datos_averias_corte_excel(
        df_corte_excel,
        df_word_datos_averias,
        'both'
        )
   
    # AVERIAS - TELEFONIA - EXCEL
    df_matched_word_telefonia_averias_corte_excel = merge_word_telefonia_averias_corte_excel(
        df_word_telefonia_averias,
        df_corte_excel,
        'both'
        )
    
    #INFORME TECNICO - DATOS - EXCEL
    df_matched_word_datos_informe_tecnico_corte_excel = merge_word_datos_informe_corte_excel(
        df_word_datos_informe_tec,
        df_corte_excel,
        'both'
        )
    
    #INFORME TECNICO - TELEFONIA - EXCEL
    df_matched_word_telefonia_informe_tecnico_corte_excel = merge_word_telefonia_informe_corte_excel(
        df_word_telefonia_informe_tec,
        df_corte_excel,
        'both'
        )
    

    #ANEXOS INDISPONIBILIDAD - DATOS - EXCEL
    df_matched_word_datos_anexo_indisponibilidad_df_merged_sga = merge_word_datos_anexos_disponibilidad_df_merged_sga(
        df_word_datos_anexos_indis,
        df_matched_corte_sga335_Sharepoint_cuismp_sga380,
        'both'
        )
    
    #ANEXOS INDISPONIBILIDAD - TELEFONIA - EXCEL
    df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga = merge_word_telefonia_anexos_disponibilidad_df_merged_sga(
        df_word_telefonia_anexos_indis,
        df_matched_corte_sga335_Sharepoint_cuismp_sga380,
        'both'
        )
    

# OBJETIVOS 

    obj1_df = validation_objetivo_1(
        df_matched_corte_sga335_Sharepoint_cuismp_sga380,
        df_unmatched_corte_sga335_Sharepoint_cuismp_sga380
        )

    obj2_df = validation_objetivo_2(
        df_matched_word_datos_averias_corte_excel,
        df_matched_word_telefonia_averias_corte_excel,
        df_matched_word_datos_informe_tecnico_corte_excel,
        df_matched_word_telefonia_informe_tecnico_corte_excel
        )
    
    obj3_df = validation_objetivo_3(
        df_matched_word_datos_anexo_indisponibilidad_df_merged_sga,
        df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga
    )   

    results.extend(obj1_df.to_dict(orient='records'))
    results.extend(obj2_df.to_dict(orient='records'))
    results.extend(obj3_df.to_dict(orient='records'))

    df_all = pd.DataFrame(results)

    df_grouped = (
        df_all
        .groupby('nro_incidencia', as_index=False)
        .agg({'mensaje': lambda msgs: ' | '.join(msgs),
              'objetivo': lambda objs: ' | '.join(objs), 
              'TIPO REPORTE': 'first', 
              })
    )

    return df_grouped.to_dict(orient='records')





