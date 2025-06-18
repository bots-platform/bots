import threading

global_lock = threading.Lock()

# Variable global compartida para el estado de actividad humana
actividad_humana = {"mouse": False, "teclado": False}