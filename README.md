## Estructura del proyecto

```
fastapi_project/
├── app/
│   ├── modules/
│   │   ├── sga/
│   │   │   ├── scripts/
│   │   │   ├── models.py
│   │   │   └── service.py
│   │   └── oplogin/
│   │       ├── browser/
│   │       ├── scripts/    
│   │       └── main.py
│   │
│   ├── api/
│   │   ├── sga.py
│   │   ├── oplogin.py
│   │           
│   │
│   
└── main.py
├── config.py 
├── logs/
├── media/
├── .env                           
├── .gitignore                    
├── requirements.txt            
└── README.md                     
```

## Instalación

1. Clona el Repositorio
```
git clone https://gitlab.com/AutoATCORP/bot.git

```

2. Entrar al directorio del proyecto
```
cd bot

```

3. Crea y activa un entorno virtual:
```
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

```

4. Instala las dependencias:
```
pip install -r requirements.txt

```

## Configuración

El proyecto utiliza un archivo .env para manejar las variables de entorno. Asegúrate de crear un archivo .env en la raíz del proyecto y definir las variables necesarias. Ejemplo de .env:

```
OPLOGIN_USER=EXXXXX
OPLOGIN_PASSWORD=XXXXX


EXCEL_PATH=media/default_update.xlsx
NOTEPAD_PATH=media/bloc_de_notas.txt


MI_URL=http://10.100.10.100:1000/xxx-x-xxx/
EXCEL_FILENAME=x_report.xlsx
EXCEL_CONTENT_TYPE=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
AUTH_USERNAME=
AUTH_PASSWORD=

```

## Ejecución

Para ejecutar el proyecto desde la línea de comandos:

```
python main.py

```