import logging
from pynput import mouse, keyboard
from app.shared.lock import actividad_humana

logger = logging.getLogger(__name__)

class ActivityMonitor:
    def __init__(self):
        self.mouse_listener = None
        self.keyboard_listener = None
        self.is_running = False
    
    def on_mouse_move(self, x, y):
        actividad_humana["mouse"] = True
        logger.info("Actividad de mouse detectada")
    
    def on_key_press(self, key):
        actividad_humana["teclado"] = True
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

# Instancia global
activity_monitor = ActivityMonitor() 