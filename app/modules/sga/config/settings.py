
import os
from dotenv import load_dotenv

load_dotenv()

# Leer las variables de entorno, con rutas predeterminadas apuntando a la carpeta "media"
EXCEL_PATH = os.getenv("EXCEL_PATH", os.path.join('media', 'default_update.xlsx'))
NOTEPAD_PATH = os.getenv('NOTEPAD_PATH', os.path.join('media', 'bloc_de_notas.txt'))

