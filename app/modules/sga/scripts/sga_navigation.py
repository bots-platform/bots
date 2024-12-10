
from time import sleep
from utils.logger_config import get_sga_logger
 
logger = get_sga_logger()



def navegar_sistema_tecnico(main_window):
    try:
        logger.info("Intentando selecionar 'Sistema Técnico'.")
        tecnico = main_window.child_window(
            title="Sistema Técnico",
            control_type="TreeItem"
            )
        tecnico.click_input()
        sleep(2)
        logger.info("'Sistema Técnico' seleccionado con éxito.")
    except Exception as e:
        logger.error(f"Error al seleccionar 'Sistema Técnico': {e}")
        raise

def seleccionar_opcion_sga(main_window, opcion):
    try:
        logger.info(f"Intentando seleccionar la opción: {opcion}")
        opcion_window = main_window.child_window(
            title=opcion,
            control_type="ListItem"
            )
        opcion_window.double_click_input()
        logger.info(f"Opcion '{opcion} seleccionada con éxito.")
    except Exception as e:
        logger.error(f"Error al seleccionar la opción '{opcion}': {e}")
        raise