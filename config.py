from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

URL_OPLOGIN = os.getenv("URL_OPLOGIN")
OPLOGIN_USER= os.getenv("OPLOGIN_USER")
OPLOGIN_PASSWORD= os.getenv("OPLOGIN_PASSWORD")

NEW_CALL_CENTER_USER = os.getenv("NEW_CALL_CENTER_USER")
NEW_CALL_CENTER_PASSWORD = os.getenv("NEW_CALL_CENTER_PASSWORD")
URL_NEW_CALL_CENTER = os.getenv("URL_NEW_CALL_CENTER")

SEMAFORO_USER = os.getenv("SEMAFORO_USER")
SEMAFORO_PASSWORD = os.getenv("SEMAFORO_PASSWORD")
URL_SEMAFORO = os.getenv("URL_SEMAFORO")

URL_SHAREPOINT = os.getenv("URL_SHAREPOINT")
SHAREPOINT_USER= os.getenv("SHAREPOINT_USER")
SHAREPOINT_PASSWORD= os.getenv("SHAREPOINT_PASSWORD")

URL_DJANGO = os.getenv("URL_DJANGO")
AUTH_USERNAME = os.getenv("AUTH_USERNAME")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")

EXCEL_FILENAME = os.getenv("EXCEL_FILENAME")
EXCEL_CONTENT_TYPE = os.getenv("EXCEL_CONTENT_TYPE")

EXCEL_PATH = os.getenv("EXCEL_PATH", os.path.join('media', 'sga', 'default_update.xlsx'))
