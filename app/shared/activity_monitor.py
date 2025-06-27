import logging
from pynput import mouse, keyboard
import json
import os
import tempfile
import time

logger = logging.getLogger(__name__)

# Archivo temporal para compartir estado entre procesos
ACTIVITY_FILE = os.path.join(tempfile.gettempdir(), 'actividad_humana.json')

class ActivityMonitor:
    def __init__(self):
        self.mouse_listener = None
        self.keyboard_listener = None
        self.is_running = False
    
    def _save_activity(self, mouse_active, teclado_active):
        """Guarda el estado de actividad en un archivo temporal"""
        try:
            activity_data = {
                "mouse": mouse_active,
                "teclado": teclado_active,
                "timestamp": time.time()
            }
            with open(ACTIVITY_FILE, 'w') as f:
                json.dump(activity_data, f)
        except Exception as e:
            logger.error(f"Error guardando actividad: {e}")
    
    def on_mouse_move(self, x, y):
        self._save_activity(True, False)
        logger.info("Actividad de mouse detectada")
    
    def on_key_press(self, key):
        self._save_activity(False, True)
        logger.info("Actividad de teclado detectada")
    
    def start(self):
        """Inicia los listeners de mouse y teclado"""
        if self.is_running:
            logger.warning("ActivityMonitor ya está ejecutándose")
            return
        
        self.mouse_listener = mouse.Listener(on_move=self.on_mouse_move)
        self.keyboard_listener = keyboard.Listener(on_press=self.on_key_press)
        
        self.mouse_listener.start()
        self.keyboard_listener.start()
        self.is_running = True
        
        logger.info("ActivityMonitor iniciado - monitoreando actividad humana")
    
    def stop(self):
        """Detiene los listeners"""
        if not self.is_running:
            return
        
        if self.mouse_listener:
            self.mouse_listener.stop()
        if self.keyboard_listener:
            self.keyboard_listener.stop()
        
        self.is_running = False
        logger.info("ActivityMonitor detenido")

def get_activity_status():
    """Obtiene el estado de actividad desde el archivo temporal"""
    try:
        if os.path.exists(ACTIVITY_FILE):
            # Verificar si el archivo es muy antiguo (más de 60 segundos)
            file_time = os.path.getmtime(ACTIVITY_FILE)
            if time.time() - file_time > 60:
                # Archivo muy antiguo, resetear
                return {"mouse": False, "teclado": False}
            
            with open(ACTIVITY_FILE, 'r') as f:
                activity_data = json.load(f)
                return {"mouse": activity_data.get("mouse", False), 
                       "teclado": activity_data.get("teclado", False)}
        else:
            return {"mouse": False, "teclado": False}
    except Exception as e:
        logger.error(f"Error obteniendo estado de actividad: {e}")
        return {"mouse": False, "teclado": False}

def reset_activity_status():
    """Resetea el estado de actividad en el archivo temporal"""
    try:
        activity_data = {
            "mouse": False,
            "teclado": False,
            "timestamp": time.time()
        }
        with open(ACTIVITY_FILE, 'w') as f:
            json.dump(activity_data, f)
    except Exception as e:
        logger.error(f"Error reseteando estado de actividad: {e}")

# Instancia global
activity_monitor = ActivityMonitor() 