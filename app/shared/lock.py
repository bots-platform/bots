import threading

global_lock = threading.Lock()

actividad_humana = {"mouse": False, "teclado": False}