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
├── venv/
├── logs/
├── media/
├── .env 
├── .env.example                          
├── .gitignore                    
├── requirements.txt            
└── README.md                     
```

## Instalación

1. Clona el Repositorio
```
git clone https://gitlab.com/AutoATCORP/bots_rpa.git

```

2. Entrar al directorio del proyecto
```
cd bots_rpa

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

El proyecto utiliza un archivo .env para manejar las variables de entorno. Asegúrate de crear un archivo .env en la raíz del proyecto y definir las variables necesarias. Use .env.example para guiarse.


## Ejecución

Para ejecutar el proyecto desde la línea de comandos:

```
python main.py

```



