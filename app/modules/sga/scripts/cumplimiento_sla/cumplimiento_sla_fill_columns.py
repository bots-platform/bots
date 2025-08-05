import pandas as pd
import os
import numpy as np
from datetime import datetime
timestamp= datetime.now().strftime("%Y%m%d_%H%M%S")

from utils.logger_config import get_sga_logger
logger = get_sga_logger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path_mtx = os.path.abspath(os.path.join(BASE_DIR, "..","..", "..",".." ,"..", "media", "sga", "data", "mtx.xlsx"))
output_file = os.path.abspath(os.path.join(BASE_DIR, "..","..", "..",".." ,"..", "media", "sga", "reporte_SGA_formulas", f"reporte_actualizado_{timestamp}.xlsx"))

def fill_mediotx_column(file_path_reporte):

    try:
        logger.info(f"Initialization function fill_mediotx_column with file. : {file_path_reporte}")

        if not os.path.exists(file_path_mtx):
            raise FileNotFoundError(f" file not found {file_path_mtx}")
        
        if not os.path.exists(file_path_reporte):
            raise FileNotFoundError(f"file not found {file_path_reporte}")
                                    
        df_mtx = pd.read_excel(file_path_mtx)
        df_reporte = pd.read_excel(file_path_reporte)

        df_reporte = df_reporte.merge(df_mtx, on="cid" , how="left")
        df_reporte["mediotx"] = df_reporte["medio"]
        df_reporte.drop(columns=["medio"], inplace=True)
        
        logger.info("Función fill_mediotx_column finalizada correctamente.")
 
        return df_reporte
    
    except FileNotFoundError as e:
        logger.error(f"Error en fill_mediotx_column (file not found): {e}")
        raise
    except KeyError as e:
        logger.error(f"Error in fill_mediotx_column (column not found): {e} ")
        raise
    except Exception as e:
        logger.error(f"Error inesperado en fill_mediotx_column: {e}")
        raise


def fill_fo_column(df_reporte):

    try:
        logger.info("Trying to fill fo column fo")
        name_client_map = {
            "UNIVERSIDAD NACIONAL MAYOR DE SAN MARCOS": 0.25,
            "ASOCIACION DE BANCOS DEL PERU": 2,
            "SUPERINT. NAC. DE LOS REGISTROS PUBLICOS": 1,
            "REGISTRO NACIONAL DE IDENTIFICACION Y ESTADO CIVIL": 0.5,
            "CAJA MUNICIPAL DE CREDITO POPULAR DE LIM A": round(43 / 60, 2),
            }

        df_reporte["fo"] = df_reporte["nombre_cliente"].map(name_client_map)
        df_reporte["fo"] = df_reporte["fo"].combine_first(df_reporte["compute_0044"].str.split("|").str[1])
        df_reporte["fo"] = pd.to_numeric(df_reporte["fo"], errors="coerce").fillna(0)

        fiber_cut_cases = [
            "CORTE DE FIBRA",
            "CORTE DE FIBRA OPTICA / COBRE- CASO FORTUITO",
            "CORTE DE FIBRA OPTICA / COBRE DE ULTIMA MILLA - CASO FORTUITO",
            "CORTE DE FIBRA OPTICA TRONCAL DE RED - CASO FORTUITO"
        ]
        df_reporte.loc[
            df_reporte["tipificacion_problema"].isin(fiber_cut_cases) & df_reporte["fo"].notna(),
            "fo"
            ]+= 8
        
        logger.info("Successfully filled fo column")
        
    except KeyError as e:
        logger.error(f"KeyError en fill_fo_column, column not found: {e}")
        raise
    except AttributeError as e:
        logger.error(f"Attribute error en fill_fo_column, issue of type o attribute {e}")
        raise
    except ValueError as e:
        logger.error(f"ValueError in fill_fo_column, error to convert types: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_fo_column: {e}")
        raise
    return df_reporte


def fill_mo_column(df_reporte):
    try:
        logger.info("Trying to fill mo column.")
        name_client_map = {
         "UNIVERSIDAD NACIONAL MAYOR DE SAN MARCOS": 0.25,
        "ASOCIACION DE BANCOS DEL PERU": 2,
        "SUPERINT. NAC. DE LOS REGISTROS PUBLICOS": 1,
        "REGISTRO NACIONAL DE IDENTIFICACION Y ESTADO CIVIL": 0.5,
        "CAJA MUNICIPAL DE CREDITO POPULAR DE LIMA": round(43 / 60, 2),
        }

        df_reporte["mo"] = df_reporte["nombre_cliente"].map(name_client_map)
        df_reporte["mo"] = df_reporte["mo"].combine_first(df_reporte["compute_0044"].str.split("|").str[2])
        df_reporte["mo"] = pd.to_numeric(df_reporte["mo"], errors="coerce").fillna(0)

        logger.info("Successfully filled mo column")
    
    except KeyError as e:
        logger.error(f"KeyError en fill_mo_column, column not found: {e}")
        raise
    except ValueError as e:
        logger.error(f"ValueError in fill_mo_column, error to convert types: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_mo_column: {e}")
        raise

    return df_reporte

def fill_tip_interr_filtrado_column(df_reporte):

    try:
        logger.info("Trying to fill columm called tip_interr_filtrado ")

        excluded_customers = {
        "UNIVERSIDAD NACIONAL MAYOR DE SAN MARCOS",
        "ASOCIACION DE BANCOS DEL PERU",
        "REGISTRO NACIONAL DE IDENTIFICACION Y ESTADO CIVIL",
        "SUPERINT. NAC. DE LOS REGISTROS PUBLICOS",
        "CAJA MUNICIPAL DE CREDITO POPULAR DE LIM A"
        }
        df_reporte["tiempo_interrupcion"] = pd.to_numeric(df_reporte["tiempo_interrupcion"], errors="coerce").fillna(0)
        condition = (~df_reporte["nombre_cliente"].isin(excluded_customers)) & (df_reporte["nombre_cliente"]!="") & (df_reporte["tiempo_interrupcion"] < 1)
        df_reporte["tip_interr_filtrado"] = np.where(condition, 1, df_reporte["tiempo_interrupcion"])
        df_reporte["tip_interr_filtrado"] = pd.to_numeric(df_reporte["tip_interr_filtrado"], errors="coerce").fillna(0)

        logger.info("Successfully filled tip_interr_filtrado column")

    except KeyError as e:
        logger.error(f"KeyError en fill_tip_interr_filtrado_column, column not found: {e}")
        raise
    except ValueError as e:
        logger.error(f"ValueError in fill_tip_interr_filtrado_column, error to convert types: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_tip_interr_filtrado_column: {e}")
        raise

    return df_reporte


def fill_slacomp_column(df_reporte):
    try:
        logger.info("Trying to fill slacomp column")

        condition_solicitud_ft = df_reporte["caso"].isin(["SOLICITUD", "FT"])
        df_reporte["slacomp"] = np.where(condition_solicitud_ft, df_reporte["caso"], np.nan)

        condition_valid_mediotx = df_reporte["mediotx"].isin(["Fibra", "Microondas", "Cobre"])
        df_reporte["slacomp"] = np.where(~condition_valid_mediotx & df_reporte["slacomp"].isna(), df_reporte["mediotx"], df_reporte["slacomp"])

        condition_sin_servicio = df_reporte["caso"].isin(["SIN SERVICIO", "Sin Servicio-Monitoreo"])
        condition_fibra_corte = df_reporte["mediotx"].isin(["Fibra", "Cobre"])
        condition_microondas = df_reporte["mediotx"] =="Microondas"

        df_reporte["slacomp"] = np.where(
            condition_sin_servicio & condition_fibra_corte & (df_reporte["tip_interr_filtrado"] <= df_reporte["fo"]),
            1,
            df_reporte["slacomp"]
        )

        df_reporte["slacomp"] = np.where(
            condition_sin_servicio & condition_microondas,
            1,
            df_reporte["slacomp"]
        )

        condition_degradacion = df_reporte["caso"].isin(["DEGRADACION", "Degradacion-Monitoreo"])

        df_reporte["slacomp"] = np.where(
            condition_degradacion & condition_fibra_corte & (df_reporte["tip_interr_filtrado"] <= df_reporte["fo"]),
            1,
            df_reporte["slacomp"]
        )

        df_reporte["slacomp"] = np.where(
            condition_degradacion & condition_microondas,
            1,
            df_reporte["slacomp"]
        )
        df_reporte["tipificacion_tipo"] = df_reporte["tipificacion_tipo"].fillna("")

    
        df_reporte["slacomp"] = df_reporte["slacomp"].fillna(0)

        logger.info("Successfully filled slacomp column")


    except KeyError as e:
        logger.error(f"KeyError en fill_slacomp_column, column not found: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_slacompo_column: {e}")
        raise

    return df_reporte

def  calculate_slacomp_column(df_reporte):
    if df_reporte["caso"] in ["SOLICITUD", "FT"]:
        return df_reporte["caso"]
    if df_reporte["mediotx"] in ["Fibra", "Microondas", "Cobre"]:
        if df_reporte["caso"] in ["SIN SERVICIO", "Sin Servicio-Monitoreo"]:
            if (df_reporte["mediotx"] in ["Fibra", "Cobre"]) and (df_reporte["tip_interr_filtrado"] <= df_reporte["fo"]):
                return 1
            if (df_reporte["mediotx"] in ["Microondas"]) and (df_reporte["tip_interr_filtrado"] <= df_reporte["tip_interr_filtrado"]):
                return 1
            return 0
        if df_reporte["caso"] in ["DEGRADACION", "Degradacion-Monitoreo"]:
            if (df_reporte["mediotx"] in ["Fibra", "Cobre"]) and (df_reporte["tip_interr_filtrado"] <= df_reporte["fo"]):
                return 1
            if (df_reporte["mediotx"] in ["Microondas"]) and (df_reporte["tip_interr_filtrado"] <= df_reporte["tip_interr_filtrado"]):
                return 1
            return 0
    if pd.isna(df_reporte["slacomp"]) and (pd.isna(df_reporte["tipificacion_tipo"]) or df_reporte["tipificacion_tipo"] == ""):
        return "SinTip"
    
def fill_tipo_usuario_cierra_column(df_reporte):
    try:
        logger.info("Trying to fill tipo_usuario_cierra column ")

        condition_c2 = df_reporte["usuario_cierra"].str[:2] == "C2"

        special_users = {"NLEONARDO", "FROSALES", "LDELACRUZ", "WGARCIA", "WGAMERO"}

        condition_special_users = df_reporte["usuario_cierra"].isin(special_users) | (df_reporte["usuario_cierra"].str[:1] == "T")

        default_value= df_reporte["usuario_cierra"].str[:1]

        df_reporte["tipo_usuario_cierra"] = np.select(
            [condition_c2, condition_special_users],
            ["P", "C"],
            default=default_value
        )
        logger.info("Successfully filled tipo_usuario_cierra column")
        

    except KeyError as e:
        logger.error(f"KeyError en fill_tipo_usuario_cierra_column, column not found: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_tipo_usuario_cierra_column: {e}")
        raise

    return df_reporte

def fill_desp_column(df_reporte):

    try:
        logger.info("Trying to fill desp column")

        condition_sotpintcorp= df_reporte["otpint"].notna() | df_reporte["sotpint"].notna()

        condition_pintwimax = df_reporte["sotwimax"].notna()

        condition_sotpext = df_reporte["otpext"].notna() | df_reporte["sotpext"].notna()

        condition_remedy= df_reporte["remedy"].notna()

        df_reporte["desp"] = "ATCORP"

        df_reporte["desp"] += np.where(condition_sotpintcorp, " SOTPINTCORP", "")
        df_reporte["desp"] += np.where(condition_pintwimax, " PINTWIMAX", "")
        df_reporte["desp"] += np.where(condition_sotpext, " SOTPEXT", "")
        df_reporte["desp"] += np.where(condition_remedy, " REMEDY", "")

        df_reporte["desp"] = df_reporte["desp"].str.replace(" ", "+", regex=False)

        logger.info("Successfully filled desp column")

    except KeyError as e:
        logger.error(f"KeyError en fill_desp_column, column not found: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_desp_column: {e}")
        raise

    return df_reporte


def fill_desplazamiento_column(df_reporte):

    try:
        logger.info("Trying to fill desplazamiento column ")

        condition_1 = df_reporte["desp"] == "ATCORP+SOTPEXT"
        condition_2 = df_reporte["desp"] == "ATCORP+SOTPEXT+REMEDY"
        condition_3 = df_reporte["desp"] == "ATCORP+SOTPINTCORP+SOTPEXT+REMEDY"
        condition_4 = df_reporte["desp"] == "ATCORP+SOTPINTCORP+REMEDY"

        values = [
            "ATCORP+SOTPINTCORP+SOTPEXT",
            "ATCORP+SOTPINTCORP+SOTPEXT",
            "ATCORP+SOTPINTCORP+SOTPEXT",
            "ATCORP+SOTPINTCORP",
        ]

        df_reporte["desplazamiento"] = np.select(
            [condition_1, condition_2, condition_3, condition_4],
            values,
            default=df_reporte["desp"]
        )
        df_reporte.to_excel(output_file, index=False)

        logger.info("Successfully filled desplazamiento column")

    except KeyError as e:
        logger.error(f"KeyError en fill_desplazamiento_column, column not found: {e}")
        raise
    except Exception as e:
        logger.error(f"error unexpected in fill_desplazamiento_column: {e}")
        raise

    return df_reporte
    

def completar_columnas_faltantes_con_python(file_path_reporte, fecha_inicio, fecha_fin):
    try:
        logger.info("Trying to fill columns SLA")
        df = fill_mediotx_column(file_path_reporte)
        df = fill_fo_column(df)
        df = fill_mo_column(df)
        df = fill_tip_interr_filtrado_column(df)
        #df["slacomp"] = df.apply(calculate_slacomp_column, axis=1)
        df = fill_slacomp_column(df)
        df = fill_tipo_usuario_cierra_column(df)
        df = fill_desp_column(df)
        df = fill_desplazamiento_column(df)

        logger.info("Succesfully completed all SLA column processing.")
        path_reporte_sla = save_info_obtained(df, fecha_inicio, fecha_fin)
        return path_reporte_sla

    except Exception as e:
        logger.error(f"Error processing SLA columns: {e}")
        raise


def save_info_obtained(df, fecha_inicio, fecha_fin):


    output_dir = 'media/sga/reporte_SGA_formulas/'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    fecha_inicio_formato = fecha_inicio.replace("/", "-")
    fecha_fin_formato = fecha_fin.replace("/", "-")

    path_reporte_sla = os.path.join(output_dir, f'reporte_SGA_formulas_{fecha_inicio_formato}_to_{fecha_fin_formato}_{timestamp}.xlsx')
    df.to_excel(path_reporte_sla, index=False, engine='openpyxl')
    logger.info(f"Reporte SGA Formulas guardadas en: {path_reporte_sla}")

    return path_reporte_sla

    