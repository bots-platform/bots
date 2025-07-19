from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from app.modules.web_bots.config.settings  import BROWSER_SETTINGS
import os

def setup_chrome_driver(download_directory=None):
    """Setup Chrome driver with window staying open"""
    options = Options()
    options.debugger_address = "127.0.0.1:9222"  # Asegúrate de que Chrome esté corriendo con este puerto
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("user-data-dir=C:/Users/katana/AppData/Local/Google/Chrome/User Data/Profile 2")
    options.add_argument("--profile-directory=Profile 2")
    options.add_argument("--remote-debugging-port=9222")
    options.debugger_address = "127.0.0.1:9222"  # Asegúrate de que Chrome esté corriendo con este puerto

      # Usar un perfil de usuario específico para mantener la sesión iniciada
    # options.add_argument("user-data-dir=C:\\Users\\katana\\AppData\\Local\\Google\\Chrome\\User Data")
    # options.add_argument("--profile-directory=Default")

    # profile_path = r"C:\Users\katana\AppData\Local\Google\Chrome\User Data\Profile 2"
    # options.add_argument(f"user-data-dir={profile_path}")

    # options.add_argument('--safebrowsing-disable-download-protection')
    # options.add_argument('--safebrowsing-disable-extension-blacklist')
    # options.add_argument('--disable-web-security')
    # options.add_argument("--allow-running-insecure-content")
  
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
     "download.default_directory": download_directory,
     "safebrowsing.enabled": True,
     "safebrowsing.disable_download_protection": True,

    }

     # Si se especifica un directorio de descarga, agregarlo a las preferencias
    if download_directory:
        prefs["download.default_directory"] = os.path.abspath(download_directory)

    options.add_experimental_option("prefs",prefs)
    
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

     # Agregar aquí la configuración para mantener la ventana en primer plano
    driver.set_window_position(0, 0)
    driver.maximize_window()

    from selenium.webdriver.common.action_chains import ActionChains
    ActionChains(driver).move_by_offset(100, 100).click().perform()
    
    return driver