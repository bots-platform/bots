## Estructura del proyecto

fastapi_project/
├── app/
│   ├── modules/
│   │   ├── sga-tickets/
│   │   │   ├── config/
│   │   │   ├── logs/
│   │   │   ├── setup/
│   │   │   ├── scripts/
│   │   │   └── main.py
│   │   └── oplogin/
│   │       ├── config/
│   │       ├── logs/
│   │       ├── setup/
│   │       ├── scripts/
│   │       └── main.py
│   │
│   ├── api/
│   │   └── v1/
│   │       └── endpoints/
│   │           ├── sga.py
│   │           └── oplogin.py
│   │
│   └── core/
│       ├── config.py      # Configuración global
│       └── logging.py     # Logging global
│
└── main.py


