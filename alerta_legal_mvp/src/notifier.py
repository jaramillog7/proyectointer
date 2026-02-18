from notifypy import Notify

# Función para mostrar una notificación en Windows.
# Utiliza la librería 'notifypy' para generar notificaciones simples en el sistema operativo.
# Parámetros:
# - title: El título que se mostrará en la notificación.
# - message: El cuerpo del mensaje que se mostrará en la notificación.
# - duration: Tiempo en segundos que la notificación permanecerá visible. Por defecto es 6 segundos.
def notify_windows(title: str, message: str, duration: int = 6) -> None:
    try:
        # Crear la instancia de la notificación
        n = Notify()
        n.title = title      # Establecer el título de la notificación
        n.message = message  # Establecer el mensaje de la notificación
        n.send()             # Enviar la notificación al sistema
    except Exception as e:
        # En caso de error (por ejemplo, si 'notifypy' no está configurado correctamente),
        # se captura la excepción y se muestra un mensaje de advertencia en la consola.
        print(f"[notify] warning: no se pudo mostrar notificacion. err={e}")
