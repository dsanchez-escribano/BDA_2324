import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes
import sys

from datetime import datetime
from numpy.compat import long


## ------------------------------------------------------------
def connect_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="diegodb",
            user="diego",
            password="clave"
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"No se pudo conectar: {e}. Cerrando...")
        sys.exit(1)



## ------------------------------------------------------------
def disconnect_db(conn):
    conn.commit()
    conn.close()



## ------------------------------------------------------------
def es_dni_valido(dni):
    """
    Verifica si una cadena representa un DNI válido en España.
    El DNI debe tener el formato numérico de 8 dígitos seguido de una letra de control.
    """
    if len(dni) != 9:
        return False  # El DNI debe tener exactamente 9 caracteres (8 dígitos + 1 letra)

    digitos = dni[:8]
    letra = dni[8].upper()  # La letra de control debe ser mayúscula

    # Verificar que los primeros 8 caracteres sean dígitos
    if not digitos.isdigit():
        return False  # Los primeros 8 caracteres deben ser dígitos numéricos

    # Calcular la letra de control esperada
    letras_validas = 'TRWAGMYFPDXBNJZSQVHLCKE'
    indice = int(digitos) % 23
    letra_esperada = letras_validas[indice]

    # Comparar la letra de control
    return letra == letra_esperada
def insert_estudio(conn):
    """
    Pide los datos de un usuario y lo inserta en la tabla 'USUARIO'
    :param conn: la conexión abierta a la base de datos
    :return: nada
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    while True:
        try:
            id_estudio = long(input("Introduce el id del estudio: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el identificador.")

    while True:
        nombre = input("Introduce el nombre del estudio: ")
        if len(nombre) <= 20:
            break
        else:
            print("Error: EL nombre debe tener como máximo 20 caracteres.")

    while True:
        pais = input("Introduce el pais del estudio: ")
        if len(pais) <= 20:
            break
        else:
            print("Error: EL pais debe tener como máximo 20 caracteres.")

    sql = "INSERT INTO ESTUDIO (id_Estudio, nombreE, paisOrigen) " \
          "VALUES (%(p)s, %(n)s, %(a)s);"
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'p': id_estudio, 'n': nombre, 'a': pais})
            conn.commit()
            print("Estudio añadido.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                print("La tabla ESTUDIO no existe. No se puede añadir el estudio.")
            elif e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print(f"El id {id_estudio} ya existe, no se añade el estudio.")


            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if 'nombre' in e.pgerror:
                    print("El nombre del estudio es necesario.")
                elif 'pais' in e.pgerror:
                    print("El pais del estudio es necesario.")
                elif e.pgcode == '42501':
                    print("Error de permisos: No tienes permiso para acceder a la tabla 'estudio'."
                          "-- Conceder permisos de INSERT a un usuario sobre la tabla 'estudio': GRANT INSERT ON TABLE estudio TO username;")

            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def insert_usuario(conn):
    """
    Pide los datos de un usuario y lo inserta en la tabla 'USUARIO'
    :param conn: la conexión abierta a la base de datos
    :return: nada
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    while True:
        sDNI = input("DNI del usuario: ")
        if es_dni_valido(sDNI):
            DNI = sDNI
            break
        else:
            print("Error: El DNI debe tener una longitud de 9 caracteres o has introducido un DNI erroneo. Y es obligatorio")


    while True:
        nombre = input("Nombre: ")
        if len(nombre) <= 15:
            break
        else:
            print("Error: El nombre debe tener como máximo 15 caracteres.")

    while True:
        apellido = input("Apellido: ")
        if len(apellido) <= 25:
            break
        else:
            print("Error: Los apellidos deben tener como máximo 25 caracteres.")

    while True:
        try:
            telefono = int(input("Teléfono: "))
            if len(str(telefono)) == 9:
                break
            else:
                print("Error: El número de teléfono debe tener 9 cifras.")
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el teléfono.")

    sql = "INSERT INTO USUARIO (DNI, nombre, apellido, telefono) " \
          "VALUES (%(p)s, %(n)s, %(a)s, %(t)s);"
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'p': DNI, 'n': nombre, 'a': apellido, 't': telefono})
            conn.commit()
            print("Usuario añadido.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                print("La tabla USUARIO no existe. No se puede añadir el usuario.")
            elif e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print(f"El DNI {DNI} ya existe, no se añade el usuario.")
            elif e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'usuario'."
                      "-- Conceder permisos de INSERT a un usuario sobre la tabla 'usuario': GRANT INSERT ON TABLE usuario TO username;")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if 'nombre' in e.pgerror:
                    print("El nombre del usuario es necesario.")
                elif 'apellido' in e.pgerror:
                    print("El primer apellido del usuario es necesario.")
                else:
                    print("El teléfono es necesario.")
            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

def insert_pelicula(conn):
    """
    Pide los datos de una película y lo inserta en la tabla 'PELÍCULA'
    :param conn: la conexión abierta a la base de datos
    :return: nada
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    while True:
        sDNI = input("DNI del usuario: ")
        if es_dni_valido(sDNI):
            id_Us = sDNI
            break
        else:
            print(
                "Error: El DNI debe tener una longitud de 9 caracteres o has introducido un DNI erroneo. Y es obligatorio")

    while True:
        try:
            id_Est = long(input("Introduce el id del estudio: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el identificador.")


    while True:
        try:
            id_pelicula = long(input("Introduce el id de la película: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el identificador.")

    while True:
        titulo = input("Introduce el titulo de la película: ")
        if len(titulo) <= 20:
            break
        else:
            print("Error: EL titulo debe tener como máximo 20 caracteres.")

    while True:
        try:
            precio = float(input("Precio: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el precio.")

    while True:
        try:
            duracion_minutos = int(input("Introduce la duración de la película en minutos: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para la duración.")

    while True:
        ano = input("Año (formato xx-xx-xxxx): ")
        try:
            datetime.strptime(ano, '%d-%m-%Y')
            break
        except ValueError:
            print("Error: El año debe tener el formato dd-mm-yyyy.")

    while True:
        genero = input("Introduce el género de la película: ")
        if len(genero) <= 20:
            break
        else:
            print("Error: EL género debe tener como máximo 20 caracteres.")


    sql = "INSERT INTO PELICULA (id_Us, id_Est, id_Pelicula, precio, titulo, duracion_Minutos, año, genero) " \
          "VALUES (%(u)s, %(e)s, %(p)s, %(pr)s, %(t)s, %(d)s, %(a)s, %(g)s);"
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'u': id_Us, 'e': id_Est, 'p': id_pelicula, 'pr': precio, 't': titulo, 'd': duracion_minutos,
                                 'a': ano, 'g': genero})
            conn.commit()
            print("Película añadida.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                print("La tabla PELICULA no existe. No se puede añadir la película.")
            elif e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print(f"El id de película {id_pelicula} ya existe, no se añade la película")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                print(f"Error de clave externa: El DNI o el estudio, no existen")
            elif e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                      "-- Conceder permisos de INSERT a un usuario sobre la tabla 'pelicula': GRANT INSERT ON TABLE usuario TO username;")

            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if 'id_Us' in e.pgerror:
                    print("El DNI del usuario es necesario.")
                elif 'id_Est' in e.pgerror:
                    print("El ID del estudio es necesario.")
                else:
                    print("El precio es necesario.")

            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def show_pelicula(conn, control_tx=True):
    """
    Solicita al usuario el id de una pelicula y muestra los detalles de la  mismo,
    incluyendo la valoración si está presente.
    :param conn: la conexión abierta a la base de datos
    :param control_tx: indica si se debe realizar commit/rollback o no
    :return: el código del coche mostrado, o None si no existe
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    while True:
        try:
            id_pelicula = long(input("Introduce el id de la pelicula: "))
            break
        except ValueError:
            print("Error: Por favor, introduce un valor numérico para el identificador.")

    sql = """
        SELECT c.id_Us, c.id_Est, c.precio, c.titulo, c.duracion_Minutos, c.año, c.genero,
               COALESCE(c.valoracion::text, 'Sin valoración') AS valoracion
        FROM PELICULA c
        WHERE c.id_pelicula = %(c)s
    """

    retval = None
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        try:
            cursor.execute(sql, {'c': id_pelicula})
            row = cursor.fetchone()
            if row is None:
                print(f"La pelicula con id {id_pelicula} no existe.")
            else:
                usuario = row.get('id_us')
                estudio = row.get('id_est')
                precio = row['precio']
                titulo = row['titulo']
                minutos = row['duracion_minutos']
                ano = row['año']
                genero = row['genero']
                valoracion = row['valoracion']

                print(f"Id de la película: {id_pelicula}")
                print(f"Usuario: {usuario}")
                print(f"Estudio: {estudio}")
                print(f"Precio: {precio}")
                print(f"Titulo: {titulo}")
                print(f"Duracion en minutos: {minutos}")
                print(f"Año: {ano}")
                print(f"Género: {genero}")
                print(f"Valoración de la película: {valoracion}")

                retval = id_pelicula

            if control_tx:
                conn.commit()
        except psycopg2.Error as e:
            if e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                  "-- Conceder permisos de SELECT a un usuario sobre la tabla 'pelicula': GRANT SELECT ON TABLE usuario TO username;")
            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            if control_tx:
                conn.rollback()
    return retval


## ------------------------------------------------------------
def update_pelicula(conn):
    """
    Chama a show_película para pedir un código e mostrar os detalles dunha pelicula,
    pide novos datos e actualiza a película
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    cod = show_pelicula(conn, control_tx=False)
    if cod is None:
        conn.rollback()
        return

    stitulo = input("Título: ")
    titulo = None if stitulo == "" else stitulo

    
    while True:
        sano = input("Año: ")
        if sano == "":
            ano = None
            break
        try:
            ano = datetime.strptime(sano, '%d-%m-%Y')
            break
        except ValueError:
            print("Error: El año debe tener el formato dd-mm-yyyy.")

    
    while True:
        sprecio = input("Precio: ")
        if sprecio == "":
            precio = None
            break
        try:
            precio = float(sprecio)
            break
        except ValueError:
            print("Error: El precio debe ser un número válido.")

    sql = """
            UPDATE PELICULA
            SET titulo = %(m)s,
                año = %(a)s,
                precio = %(p)s
            WHERE id_Pelicula = %(c)s
        """
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'c': cod, 'm': titulo, 'a': ano, 'p': precio})
            input("Pulsa ENTER")
            conn.commit()
            print("Película modificada.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                print("El precio debe ser positivo, no se modifica el coche")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                print("El precio máximo son 999999.99")
            elif e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                      "-- Conceder permisos de SELECT a un usuario sobre la tabla 'pelicula': GRANT UPDATE ON TABLE usuario TO username;")

            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if 'modelo' in e.pgerror:
                    print("El título de la película es necesario.")
                else:
                    print("El año de la película es necesario.")
            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------

def show_peliculas_usuario(conn, control_tx=True):
    """
    Muestra todos las pelíuclas de un usuario dado su DNI.
    :param conn: La conexión abierta a la base de datos.
    :param control_tx: Indica si se debe realizar commit/rollback o no.
    :return: Nada.
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    while True:
        sDNI = input("DNI del usuario: ")
        if es_dni_valido(sDNI):
            dni = sDNI
            break
        else:
            print(
                "Error: El DNI debe tener una longitud de 9 caracteres o has introducido un DNI erroneo. Y es obligatorio")

    sql = """
        SELECT c.id_Pelicula, c.titulo, c.precio
        FROM PELICULA c
        INNER JOIN usuario p ON c.id_Us = p.DNI
        WHERE p.DNI = %(dni)s
    """

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        try:
            cursor.execute(sql, {'dni': dni})
            rows = cursor.fetchall()
            if not rows:
                print(f"El usuario con dni {dni} no tiene películas registradas .")
            else:
                print("Peliculas del usuario:")
                for row in rows:
                    id_pelicula = row['id_pelicula']
                    titulo = row['titulo']
                    precio = row['precio']
                    print(f"Id_pelicula: {id_pelicula}, titulo: {titulo}, Precio: {precio}")
            if control_tx:
                conn.commit()
        except psycopg2.Error as e:
            print(f"Error {e.pgcode}: {e.pgerror}")
            if control_tx:
                conn.rollback()
## ------------------------------------------------------------
def delete_pelicula(conn):
    """
    Pide por teclado el id de una pelicula y la borra de la base de datos.
    :param conn: La conexión abierta a la base de datos.
    :return: Nada.
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    matricula = input("Introduce el id de la película a borrar: ")

    sql = "DELETE FROM PELICULA WHERE id_Pelicula = %s"

    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, (matricula, ))
            conn.commit()
            if cursor.rowcount == 0:
                print("La película no existe.")
            else:
                print("Película eliminada.")
        except psycopg2.Error as e:
            if e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                  "-- Conceder permisos de SELECT a un usuario sobre la tabla 'pelicula': GRANT DELETE ON TABLE usuario TO username;")
            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()
## ------------------------------------------------------------
def decrease_price(conn):
    """
    Llama a show_row para pedir un id y mostrar los detalles de una película,
    pide el decremento del precio y actualiza la película.
    :param conn: La conexión abierta a la base de datos.
    :return: Nada.
    """
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

    id_pelicula = input("Introduce el id de la película: ")
    if not id_pelicula:
        print("id no válido.")
        return
    while True:
        sdecr = input("Decremento de precio (porcentaje): ")
        try:
            decr = None if sdecr == "" else float(sdecr)
            if decr is not None and decr > 100:
                print("El decremento no puede ser mayor que 100%. Por favor, introduce un valor válido.")
            else:
                break
        except ValueError:
            print("El valor introducido no es válido, por favor introduce un número decimal.")

    sql = """
        UPDATE PELICULA
        SET precio = precio - precio * %(d)s / 100
        WHERE id_Pelicula = %(m)s
    """

    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'m': id_pelicula, 'd': decr})
            if cursor.rowcount == 0:
                print("El id no existe.")
            else:
                input("Pulsa ENTER para continuar")
                conn.commit()
                print("Precio modificado.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                print("El precio debe ser positivo, no se modifica la película.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                print("El precio máximo es 999.99.")
            elif e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                print("No se puede modificar el precio porque otro usuario lo modificó.")
            elif e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                 "-- Conceder permisos de SELECT a un usuario sobre la tabla 'pelicula': GRANT UPDATE ON TABLE usuario TO username;")

            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def valorar_pelicula(conn):
    """
    Solicita al usuario el id de una película, luego se pide una valoración y se actualiza la película
    :param conn: la conexión abierta a la base de datos
    :return: Nada
    """
    select_query = """
        SELECT id_Pelicula, titulo
        FROM PELICULA
        WHERE id_Pelicula = %(id_pelicula)s;
    """
    update_query = """
        UPDATE PELICULA
        SET valoracion = %(valoracion)s
        WHERE id_Pelicula = %(id_pelicula)s;
    """

    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    id_pelicula = input("Introduce el id de la pelicula: ")

    with conn.cursor() as cur:
        try:
            cur.execute(select_query, {'id_pelicula': id_pelicula})
            result = cur.fetchone()

            if result:
                titulo = result[1]
                valoracion = None

                while valoracion is None:
                    valor = input(f"Introduce la valoración de la pelicula (1-5) para la pelicula: {titulo}: ")
                    
                    try:
                        valoracion = int(valor)
                        
                        if valoracion < 1 or valoracion > 5:
                            print("La valoración debe estar entre 1 y 5.")
                            valoracion = None
                    except ValueError:
                        print("Debes ingresar un número válido.")
                        valoracion = None

                cur.execute(update_query, {'valoracion': valoracion, 'id_pelicula': id_pelicula})
                conn.commit()
                print("Valoración de la pelicula actualizada exitosamente.")

            else:
                print(f"No se encontró una película con id:{id_pelicula}.")

        except (psycopg2.Error, Exception) as e:
            if e.pgcode == '42501':
                print("Error de permisos: No tienes permiso para acceder a la tabla 'pelicula'."
                 "-- Conceder permisos de SELECT a un usuario sobre la tabla 'pelicula': GRANT UPDATE ON TABLE usuario TO username;")
            else:
                print(f"Error {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def menu(conn):
    """
    Imprime un menú de opcións, solicita a opción e executa a función asociada.
    'q' para saír.
    """
    MENU_TEXT = """
      -- MENÚ --
1- Añadir Película
2- Añadir Usuario
3- Mostrar Película
4- Actualizar Película
5- Mostrar las películas de un usuario
6- Eliminar Película
7- Disminuir Precio
8- Valorar una película
9- Insertar un estudio
q - Saír   
"""
    while True:
        print(MENU_TEXT)
        tecla = input('Opción> ')
        if tecla == 'q':
            break
        elif tecla == '1':
            insert_pelicula(conn)
        elif tecla == '2':
            insert_usuario(conn)
        elif tecla == '3':
            show_pelicula(conn)
        elif tecla == '4':
            update_pelicula(conn)
        elif tecla == '5':
            show_peliculas_usuario(conn)
        elif tecla == '6':
            delete_pelicula(conn)
        elif tecla == '7':
            decrease_price(conn)
        elif tecla == '8':
            valorar_pelicula(conn)
        elif tecla == '9':
            insert_estudio(conn)


## ------------------------------------------------------------
def main():
    """
    Función principal. Conecta á bd e executa o menú.
    Cando sae do menú, desconecta da bd e remata o programa
    """
    print('Conectando a PosgreSQL...')
    conn = connect_db()
    print('Conectado.')
    menu(conn)
    disconnect_db(conn)

## ------------------------------------------------------------

if __name__ == '__main__':
    main()
