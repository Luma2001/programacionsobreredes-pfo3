import json
import socket
import sys
import threading
import time

# Parámetros de conexión del servidor socket
HOST = "127.0.0.1"
PORT = 65432
BUFFER_SIZE = 4096


def enviar_tarea(id_tarea, operacion, parametros):
    """
    Establece conexión TCP por socket, transmite la tarea y recibe el resultado.
    """
    payload = {
        "id_tarea": id_tarea,
        "operacion": operacion,
        "parametros": parametros,
    }

    try:
        # Creación del socket TCP (IPv4 + TCP stream)
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((HOST, PORT))

        # Serialización y envío del JSON
        mensaje_bytes = json.dumps(payload).encode("utf-8")
        cliente_socket.sendall(mensaje_bytes)

        # Recepción de la respuesta procesada por el worker
        respuesta_bytes = cliente_socket.recv(BUFFER_SIZE)
        if respuesta_bytes:
            respuesta = json.loads(respuesta_bytes.decode("utf-8"))
            print(f"\n[RESPUESTA RECIBIDA] Tarea: {respuesta.get('id_tarea')}")
            print(f"  * Estado: {respuesta.get('estado')}")
            print(f"  * Resultado: {respuesta.get('resultado')}")
            print(f"  * Procesado por: {respuesta.get('procesado_por')}")
            return respuesta

    except ConnectionRefusedError:
        print(
            f"❌ Error: No se pudo conectar al servidor en {HOST}:{PORT}. Asegúrate de que 'servidor_distribuido.py' esté corriendo."
        )
    except Exception as e:
        print(f"❌ Error durante la comunicación: {e}")
    finally:
        cliente_socket.close()


def prueba_concurrente():
    """
    Lanzamos 6 tareas simultáneas desde clientes concurrentes para ver el pool en acción.
    """
    print("\n--- INICIANDO PRUEBA DE CARGA CONCURRENTE (6 TAREAS) ---")
    print(
        "Para ver cómo los 4 workers procesan en paralelo ver consola del servidor\n"
    )

    tareas = [
        ("T-01", "sumar", [10, 20, 30, 40]),
        ("T-02", "multiplicar", [2, 3, 4, 5]),
        ("T-03", "invertir_cadena", ["Sistemas Distribuidos"]),
        ("T-04", "sumar", [100, 250, 750]),
        ("T-05", "invertir_cadena", ["Sockets TCP y Python"]),
        ("T-06", "multiplicar", [7, 8, 9]),
    ]

    hilos_clientes = []
    tiempo_inicio = time.time()

    for id_t, op, params in tareas:
        hilo = threading.Thread(
            target=enviar_tarea, args=(id_t, op, params)
        )
        hilos_clientes.append(hilo)
        hilo.start()

    for hilo in hilos_clientes:
        hilo.join()

    duracion_total = round(time.time() - tiempo_inicio, 2)
    print(
        f"\n✔ Todas las tareas concurrentes finalizaron en {duracion_total} segundos."
    )


def menu_interactivo():
    """
    Interfaz de consola interactiva.
    """
    while True:
        print("\n==========================================")
        print("   CLIENTE DISTRIBUIDO - SOCKETS TCP      ")
        print("==========================================")
        print("1. Enviar tarea de suma (ej: [15, 25, 60])")
        print("2. Enviar tarea de multiplicación (ej: [4, 5, 2])")
        print("3. Enviar tarea de invertir texto")
        print("4. Ejecutar prueba de carga concurrente (múltiples tareas)")
        print("5. Salir")

        opcion = input("Selecciona una opción (1-5): ").strip()

        if opcion == "1":
            id_t = f"manual_{int(time.time())}"
            entrada = input(
                "Ingresa números separados por coma (ej: 10,20,30): "
            )
            try:
                nums = [float(x.strip()) for x in entrada.split(",") if x.strip()]
                enviar_tarea(id_t, "sumar", nums)
            except ValueError:
                print("❌ Formato numérico incorrecto.")

        elif opcion == "2":
            id_t = f"manual_{int(time.time())}"
            entrada = input(
                "Ingresa números a multiplicar separados por coma: "
            )
            try:
                nums = [float(x.strip()) for x in entrada.split(",") if x.strip()]
                enviar_tarea(id_t, "multiplicar", nums)
            except ValueError:
                print("❌ Formato numérico incorrecto.")

        elif opcion == "3":
            id_t = f"manual_{int(time.time())}"
            texto = input("Ingresa la cadena de texto: ")
            enviar_tarea(id_t, "invertir_cadena", [texto])

        elif opcion == "4":
            prueba_concurrente()

        elif opcion == "5":
            print("Cerrando cliente...")
            sys.exit(0)
        else:
            print("Opción no válida. Ingresa un número del 1 al 5.")


if __name__ == "__main__":
    menu_interactivo()