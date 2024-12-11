import os
import time

def wait_for_download(download_path, timeout=60, polling_interval=1):
    """
    Espera dinámica para verificar que el archivo se haya descargado.

    Args:
        download_path (str): La ruta de la carpeta de descargas.
        timeout (int): Tiempo máximo de espera en segundos.
        polling_interval (int): Intervalo de comprobación en segundos.

    Returns:
        str: La ruta completa del archivo descargado o None si no se descargó.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        # Listar archivos que terminen en .xls o .xlsx
        files = [f for f in os.listdir(download_path) if f.endswith(".xls") or f.endswith(".xlsx")]

        # Filtrar solo los archivos modificados después del start_time
        recent_files = [os.path.join(download_path, f) for f in files if os.path.getmtime(os.path.join(download_path,f)) > start_time]

        if recent_files:
            # Obtener el archivo más reciente
            downloaded_file = max(recent_files, key=os.path.getmtime)
            return downloaded_file
        
        time.sleep(polling_interval)

  
    return None
