from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from app.modules.oplogin.config.settings  import BROWSER_SETTINGS

def setup_chrome_driver():
    """Setup Chrome driver with window staying open"""
    options = Options()
    

    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")

  
    # Mantener la ventana abierta
    options.add_experimental_option("detach", True)
    
    # Modo headless si está configurado
    if BROWSER_SETTINGS['headless']:
        options.add_argument("--headless")
    
    # Anti-detección de automatización
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Manejo de certificados y SSL
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--ignore-certificate-errors-spki-list')
    
    # Opciones adicionales para estabilidad
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_experimental_option("prefs", {
        # Desactivar diálogos de guardar contraseña
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        # Desactivar otras notificaciones
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.media_stream_mic": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        "profile.default_content_setting_values.geolocation": 2
    })
    
    # Crear el driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(
        service=service,
        options=options
    )
    
    # Configurar timeouts
    driver.implicitly_wait(BROWSER_SETTINGS['timeout'])
    driver.set_page_load_timeout(BROWSER_SETTINGS['page_load_timeout'])
    
    # Ocultar webdriver
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    
    return driver