from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

URL_DJANGO = os.getenv("URL_DJANGO")
EXCEL_FILENAME = os.getenv("EXCEL_FILENAME")
EXCEL_CONTENT_TYPE = os.getenv("EXCEL_CONTENT_TYPE")
AUTH_USERNAME = os.getenv("AUTH_USERNAME")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")

EXCEL_PATH = os.getenv("EXCEL_PATH", os.path.join('media', 'sga', 'default_update.xlsx'))
