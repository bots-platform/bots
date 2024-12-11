from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from app.modules.web_bots.config.settings import BROWSER_SETTINGS
import os

def setup_edge_driver(download_directory=None):
    """Setup Edge driver with window staying open"""
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")

        # options.add_argument('--safebrowsing-disable-download-protection')
        # options.add_argument('--safebrowsing-disable-extension-blacklist')
        # options.add_argument('--disable-web-security')
        # options.add_argument("--allow-running-insecure-content")

    # Mantener la ventana abierta (en Edge se usa diferente)
    options.add_experimental_option("detach", True)
    
    # Modo headless si está configurado
    if BROWSER_SETTINGS['headless']:
        options.add_argument("--headless=new")
    
    # Anti-detección de automatización
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Manejo de certificados y SSL
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--ignore-certificate-errors-spki-list')
    
    # Opciones adicionales para estabilidad
    options.add_argument("--disable-extensions")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')

    prefs = {
        # Desactivar diálogos de guardar contraseña
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        # Desactivar otras notificaciones
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.media_stream_mic": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        "profile.default_content_setting_values.geolocation": 2,
        # Configuraciones por defecto para descargas
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "download.default_directory": download_directory if download_directory else "",
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }

    # Si se especifica un directorio de descarga, actualizarlo en las preferencias
    if download_directory:
        prefs["download.default_directory"] = os.path.abspath(download_directory)

    options.add_experimental_option("prefs", prefs)
    
    # Crear el servicio y driver de Edge
    service = Service(EdgeChromiumDriverManager().install())
    driver = webdriver.Edge(  # Cambiado de Chrome a Edge
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

    # Configuración de ventana
    driver.set_window_position(0, 0)
    driver.maximize_window()

    from selenium.webdriver.common.action_chains import ActionChains
    ActionChains(driver).move_by_offset(100, 100).click().perform()
    
    return driver