import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from prettytable import PrettyTable

# Función para conectar a Cassandra
def conectar_cassandra():
    auth_provider = PlainTextAuthProvider(username='jorgeimlz', password='4r13l170M')
    cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
    session = cluster.connect('mercadolibre_test')
    return session

# Función para consultar registros
def consultar_registros(session):
    criterio = input("Ingrese la identificación del cliente a buscar: ")
    try:
        rows = session.execute(f"SELECT * FROM clientes WHERE cliente_identificacion = '{criterio}' ALLOW FILTERING")
        table = PrettyTable()
        table.field_names = ["cliente_id", "cliente_identificacion", "nombre", "telefono", "email", "direcciones"]
        
        for row in rows:
            table.add_row([row.cliente_id, row.cliente_identificacion, row.nombre, row.telefono, row.email, row.direcciones])
        
        print(table)
    except Exception as e:
        print("Error al consultar registros:", e)

# Función para insertar registros
def insertar_registros(session):
    cliente_identificacion = input("Ingrese la identificación del cliente: ")
    nombre = input("Ingrese el nombre del cliente: ")
    telefono = input("Ingrese el teléfono del cliente: ")
    email = input("Ingrese el email del cliente: ")
    direcciones = input("Ingrese la dirección del cliente: ").split(',')  # Puede ingresar múltiples direcciones separadas por coma

    try:
        session.execute(
            """
            INSERT INTO clientes (cliente_id, cliente_identificacion, nombre, telefono, email, direcciones)
            VALUES (uuid(), %s, %s, %s, %s, %s)
            """,
            (cliente_identificacion, nombre, telefono, email, direcciones)
        )
        print("Registro insertado con éxito.")
    except Exception as e:
        print("Error al insertar registro:", e)

# Función para actualizar registros
def actualizar_registros(session):
    cliente_id = input("Ingrese el ID del cliente a actualizar: ")
    nuevo_nombre = input("Ingrese el nuevo nombre del cliente (deje en blanco para no cambiar): ")
    nuevo_telefono = input("Ingrese el nuevo teléfono del cliente (deje en blanco para no cambiar): ")
    nuevo_email = input("Ingrese el nuevo email del cliente (deje en blanco para no cambiar): ")
    
    try:
        if nuevo_nombre:
            session.execute(
                """
                UPDATE clientes SET nombre = %s WHERE cliente_id = %s
                """,
                (nuevo_nombre, uuid.UUID(cliente_id))
            )
        if nuevo_telefono:
            session.execute(
                """
                UPDATE clientes SET telefono = %s WHERE cliente_id = %s
                """,
                (nuevo_telefono, uuid.UUID(cliente_id))
            )
        if nuevo_email:
            session.execute(
                """
                UPDATE clientes SET email = %s WHERE cliente_id = %s
                """,
                (nuevo_email, uuid.UUID(cliente_id))
            )
        print("Registro actualizado con éxito.")
    except Exception as e:
        print("Error al actualizar registro:", e)

# Función para eliminar registros
def eliminar_registros(session):
    cliente_id = input("Ingrese el ID del cliente a eliminar: ")
    try:
        session.execute(
            """
            DELETE FROM clientes WHERE cliente_id = %s
            """,
            (uuid.UUID(cliente_id),)
        )
        print("Registro eliminado con éxito.")
    except Exception as e:
        print("Error al eliminar registro:", e)

# Programa principal
def main():
    session = conectar_cassandra()
    print("Conexión establecida con Cassandra.")
    while True:
        print("1. Insertar registro")
        print("2. Consultar registros")
        print("3. Actualizar registro")
        print("4. Eliminar registro")
        print("5. Salir")

        opcion = input("Seleccione una opción 1-5: ")
        if opcion == '1':
            insertar_registros(session)
        elif opcion == '2':
            consultar_registros(session)
        elif opcion == '3':
            actualizar_registros(session)
        elif opcion == '4':
            eliminar_registros(session)
        elif opcion == '5':
            print("Saliendo del programa.")
            break
        else:
            print("Opción no válida.")

    session.cluster.shutdown()
    session.shutdown()

if __name__ == "__main__":
    main()
