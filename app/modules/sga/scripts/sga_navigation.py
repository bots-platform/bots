import os
import logging
from time import sleep

if not os.path.exists('logs/sga'):
    os.makedirs('logs/sga')

    logging.basicConfig(level=logging.INFO, filename="logs/sga/sga.log", 
                        format="%(asctime)s - %(levelname)s - %(message)s")

def navegar_sistema_tecnico(main_window):
    try:
        logging.info("Intentando selecionar 'Sistema Técnico'.")
        tecnico = main_window.child_window(
            title="Sistema Técnico",
            control_type="TreeItem"
            )
        tecnico.click_input()
        sleep(2)
        logging.info("'Sistema Técnico' seleccionado con éxito.")
        return True
    except Exception as e:
        logging.error(f"Error al seleccionar 'Sistema Técnico': {e}")

def seleccionar_opcion_sga(main_window, opcion):
    try:
        logging.info(f"Intentando seleccionar la opción: {opcion}")
        opcion_window = main_window.child_window(
            title=opcion,
            control_type="ListItem"
            )
        opcion_window.double_click_input()
        logging.info(f"Opcion '{opcion} seleccionada con éxito.")
    except Exception as e:
        logging.error(f"Error al seleccionar la opción '{opcion}': {e}")
        raise