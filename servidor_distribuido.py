import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# Configuración de red del servidor
HOST = "127.0.0.1"
PORT = 65432
BUFFER_SIZE = 4096

"""
ThreadPoolExecutor(max_workers=4): Crea la piscina de hilos reutilizables. 
En lugar de instanciar un nuevo hilo por cada tarea (lo que saturaría
memoria si llegan cientos de clientes), las tareas se encolan internamente 
y son tomadas por los 4 trabajadores asignados.
"""
# Definición del tamaño del pool de workers (concurrencia controlada)
MAX_WORKERS = 4
executor = ThreadPoolExecutor(
    max_workers=MAX_WORKERS, thread_name_prefix="WorkerThread"
)


def ejecutar_tarea(datos_tarea):
    """
    Lógica ejecutada por un worker del pool.

    Simula el cómputo de la tarea recibida.
    """
    hilo_actual = threading.current_thread().name
    id_tarea = datos_tarea.get("id_tarea", "desconocido")
    operacion = datos_tarea.get("operacion", "echo")
    parametros = datos_tarea.get("parametros", [])

    print(
        f"[{hilo_actual}] Iniciando procesamiento de tarea '{id_tarea}' ({operacion})..."
    )

    # Simulación de carga de trabajo / I/O
    time.sleep(1.5)

    # Resolución según la operación solicitada
    if operacion == "sumar":
        resultado = sum(parametros)
    elif operacion == "multiplicar":
        resultado = 1
        for num in parametros:
            resultado *= num
    elif operacion == "invertir_cadena":
        cadena = parametros[0] if parametros else ""
        resultado = str(cadena)[::-1]
    else:
        resultado = f"Operación '{operacion}' ejecutada con éxito."

    print(f"[{hilo_actual}] Tarea '{id_tarea}' completada.")

    return {
        "id_tarea": id_tarea,
        "estado": "COMPLETADA",
        "resultado": resultado,
        "procesado_por": hilo_actual,
    }





def manejar_cliente(socket_cliente, direccion_cliente):
    """
    Gestiona el ciclo de vida de la conexión con un cliente TCP.
    """
    print(f"[CONEXIÓN] Cliente conectado desde {direccion_cliente}")

    try:
        # Recepción de datos enviados por el socket del cliente
        datos_recibidos = socket_cliente.recv(BUFFER_SIZE)
        if not datos_recibidos:
            return

        # Decodificación del mensaje JSON
        mensaje = json.loads(datos_recibidos.decode("utf-8"))
        print(f"[RECEPCIÓN] Tarea recibida de {direccion_cliente}: {mensaje}")

        # Despacho de la tarea hacia el pool de workers (asíncrono en el pool)
        futuro = executor.submit(ejecutar_tarea, mensaje)

        # Espera el resultado del worker y lo serializa de regreso al cliente
        respuesta = futuro.result()
        socket_cliente.sendall(json.dumps(respuesta).encode("utf-8"))

    except json.JSONDecodeError:
        error_msg = {"estado": "ERROR", "detalle": "JSON inválido"}
        socket_cliente.sendall(json.dumps(error_msg).encode("utf-8"))
    except Exception as e:
        error_msg = {"estado": "ERROR", "detalle": str(e)}
        socket_cliente.sendall(json.dumps(error_msg).encode("utf-8"))
    finally:
        # Cierre explícito del socket del cliente para liberar recursos en el kernel
        socket_cliente.close()
        print(f"[DESCONEXIÓN] Conexión cerrada con {direccion_cliente}")


def iniciar_servidor():
    """
    socket(AF_INET, SOCK_STREAM): Configura un socket TCP en IPv4. 
    TCP garantiza que la serialización JSON llegue íntegra y ordenada.
    """
    # Inicia el socket pasivo del servidor para escuchar conexiones entrantes.
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   
    """
    SO_REUSEADDR: permite reutilizar el puerto sin esperar el timeout TIME_WAIT del SO
    Evita el error habitual de Address already in use cuando el puerto queda en estado 
    TIME_WAIT al reiniciar el servidor en pruebas rápidas.
    """
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        servidor_socket.bind((HOST, PORT))
        servidor_socket.listen()
        print("=" * 60)
        print(
            f"  SERVIDOR DISTRIBUIDO INICIADO EN {HOST}:{PORT}"
        )
        print(f"  Pool de hilos activo: {MAX_WORKERS} workers listos")
        print("=" * 60)

        while True:
            # Espera bloqueante de una nueva conexión entrante
            socket_cliente, direccion_cliente = servidor_socket.accept()

            # Despachamos la atención de red a un hilo liviano para no frenar el accept()
            hilo_conexion = threading.Thread(
                target=manejar_cliente,
                args=(socket_cliente, direccion_cliente),
                daemon=True,
            )
            hilo_conexion.start()

    except KeyboardInterrupt:
        print("\n[APAGADO] Deteniendo servidor...")
    finally:
        servidor_socket.close()
        executor.shutdown(wait=False)
        print("[APAGADO] Servidor cerrado correctamente.")


if __name__ == "__main__":
    iniciar_servidor()