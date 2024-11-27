import happybase
import pandas as pd

try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'music_data'
    families = {
        'basic': dict(),       # Información básica de la canción
        'streaming': dict(),   # Información de plataformas de streaming
        'metrics': dict()      # Métricas de audio
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"Tabla '{table_name}' creada exitosamente")

    # 3. Cargar datos del CSV
    music_data = pd.read_csv('spotify.csv', encoding='ISO-8859-1')

    print("Nombres de las columnas:")
    print(music_data.columns)

    # Iterar sobre el DataFrame usando el índice
    for index, row in music_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'track_{index}'.encode()

        # Organizar los datos en familias de columnas
        data = {
            b'basic:track_name': str(row['track_name']).encode(),
            b'basic:artist_name': str(row['artist(s)_name']).encode(),
            b'basic:released_year': str(row['released_year']).encode(),
            b'basic:released_month': str(row['released_month']).encode(),
            b'basic:released_day': str(row['released_day']).encode(),

            b'streaming:in_spotify_playlists': str(row['in_spotify_playlists']).encode(),
            b'streaming:streams': str(row['streams']).encode(),
            b'streaming:in_apple_playlists': str(row['in_apple_playlists']).encode(),
            b'streaming:in_deezer_playlists': str(row['in_deezer_playlists']).encode(),
            b'streaming:in_shazam_charts': str(row['in_shazam_charts']).encode(),

            b'metrics:bpm': str(row['bpm']).encode(),
            b'metrics:energy_percent': str(row['energy_%']).encode(),
            b'metrics:danceability_percent': str(row['danceability_%']).encode(),
            b'metrics:acousticness_percent': str(row['acousticness_%']).encode(),
            b'metrics:instrumentalness_percent': str(row['instrumentalness_%']).encode(),
        }

        # Insertar los datos en la tabla
        table.put(row_key, data)

    print("Datos cargados exitosamente")

    # Ejemplo de consulta
    print("\n=== Primeras canciones en la base de datos ===")
    count = 0
    for key, data in table.scan():
        if count < 3:  # Mostrar solo las 3 primeras canciones
            print(f"\nCanción ID: {key.decode()}")
            print(f"Nombre: {data[b'basic:track_name'].decode()}")
            print(f"Artista: {data[b'basic:artist_name'].decode()}")
            print(f"Streams: {data[b'streaming:streams'].decode()}")
            count += 1

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
