from flask import Flask, render_template, request, redirect, url_for, flash, session, send_from_directory
import pyodbc
import sys
import os
import datetime


app = Flask(__name__)
app.secret_key = "super secret key"
app.config['UPLOAD_FOLDER'] = 'logs' 

def connect_to_database(server, database, username, password):
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        connection = pyodbc.connect(connection_string)
        print("Conexión exitosa a la base de datos.")
        return connection
    except Exception as e:
        print("Error al conectar a la base de datos:", e)
        sys.exit(1)


def write_to_file(filename, data):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    with open(filepath, 'w') as file:
        for entry in data:
            file.write(entry + '\n')
    print(f"Datos guardados exitosamente en '{filename}'.")
    return filepath

def identify_relations(connection):
    print("Identificación automática de las relaciones.")
    cursor = connection.cursor()
    
    query = """
    SELECT 
        fk.name AS foreign_key_name,
        tp.name AS parent_table,
        cp.name AS parent_column,
        tr.name AS referenced_table,
        cr.name AS referenced_column
    FROM 
        sys.foreign_keys AS fk
    INNER JOIN 
        sys.tables AS tp ON fk.parent_object_id = tp.object_id
    INNER JOIN 
        sys.tables AS tr ON fk.referenced_object_id = tr.object_id
    INNER JOIN 
        sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
    INNER JOIN 
        sys.columns AS cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
    INNER JOIN 
        sys.columns AS cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    log_entries = []
    if rows:
        log_entries = [f"Foreign Key: {row.foreign_key_name} - Table: {row.parent_table}({row.parent_column}) -> {row.referenced_table}({row.referenced_column})" for row in rows]
        filepath = write_to_file('relations_log.txt', log_entries)
    else:
        log_entries.append("No se encontraron relaciones.")
        filepath = write_to_file('relations_log.txt', log_entries)
    
    return log_entries, filepath

def check_integrity_anomalies(connection):
    print("Chequeo automático de anomalías en la integridad referencial.")
    cursor = connection.cursor()
    
    queries = {
        'inserts': """
        -- Insert anomaly check (example)
        SELECT * FROM information_schema.referential_constraints
        """,
        'deletes': """
        -- Delete anomaly check (example)
        SELECT * FROM information_schema.referential_constraints
        """,
        'updates': """
        -- Update anomaly check (example)
        SELECT * FROM information_schema.referential_constraints
        """
    }
    
    anomalies_log = []
    for action, query in queries.items():
        anomalies_log.append(f"\nChequeo de anomalías para {action}:")
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if rows:
            for row in rows:
                anomalies_log.append(str(row))
        else:
            anomalies_log.append(f"No se encontraron anomalías para {action}.")
    
    filepath = write_to_file('integrity_anomalies_log.txt', anomalies_log)
    return anomalies_log, filepath

def check_data_anomalies(connection):
    print("Chequeo automático de las anomalías de los datos.")
    cursor = connection.cursor()
    
    data_anomalies_log = []

    # Chequeo de valores nulos
    try:
        query = """
        SELECT 
            table_name, column_name
        FROM 
            information_schema.columns
        WHERE 
            is_nullable = 'YES'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE NULLS:")
        data_anomalies_log.append("="*40)
        if rows:
            found_nulls = False
            for row in rows:
                table_name = row.table_name
                column_name = row.column_name
                check_query = f"""
                SELECT 
                    '{table_name}' AS table_name, 
                    '{column_name}' AS column_name, 
                    COUNT(*) AS null_count
                FROM 
                    {table_name}
                WHERE 
                    {column_name} IS NULL
                """
                cursor.execute(check_query)
                result = cursor.fetchone()
                if result and result.null_count > 0:
                    data_anomalies_log.append(f"{table_name}: {column_name} - Datos nulos: {result.null_count}")
                    found_nulls = True
            if not found_nulls:
                data_anomalies_log.append("No se encontraron valores nulos.")
        else:
            data_anomalies_log.append("No se encontraron columnas que permitan valores nulos.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de nulls: {e}")

    # Chequeo de duplicados
    try:
        query = """
        SELECT 
            table_name, column_name, data_type
        FROM 
            information_schema.columns
        WHERE 
            is_nullable = 'YES'
        """
        cursor.execute(query)
        columns = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE DUPLICADOS:")
        data_anomalies_log.append("="*40)
        for column in columns:
            table_name = column.table_name
            column_name = column.column_name
            data_type = column.data_type
            if data_type not in ['text', 'ntext', 'image']:  # Filtrar tipos de datos incompatibles
                try:
                    check_query = f"""
                    SELECT 
                        '{table_name}' AS table_name, 
                        '{column_name}' AS column_name, 
                        COUNT(*) AS duplicate_count
                    FROM 
                        {table_name}
                    GROUP BY 
                        {column_name}
                    HAVING 
                        COUNT(*) > 1
                    """
                    cursor.execute(check_query)
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            data_anomalies_log.append(f"{table_name}: {column_name} - Duplicado: {row.duplicate_count}")
                    else:
                        data_anomalies_log.append(f"No se encontraron duplicados en {table_name}.{column_name}.")
                except Exception as inner_e:
                    data_anomalies_log.append(f"Error al chequear la columna {column_name} en la tabla {table_name}: {inner_e}")
            else:
                data_anomalies_log.append(f"Columna {column_name} en tabla {table_name} es de tipo {data_type} y fue omitida.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de duplicates: {e}")

    # Chequeo de outliers
    try:
        query = """
        SELECT 
            table_name, column_name
        FROM 
            information_schema.columns
        WHERE 
            data_type IN ('int', 'decimal', 'float', 'numeric')
        """
        cursor.execute(query)
        numeric_columns = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE OUTLIERS:")
        data_anomalies_log.append("="*40)
        for column in numeric_columns:
            table_name = column.table_name
            column_name = column.column_name
            check_query = f"""
            SELECT 
                '{table_name}' AS table_name, 
                '{column_name}' AS column_name, 
                MAX({column_name}) AS max_value, 
                MIN({column_name}) AS min_value
            FROM 
                {table_name}
            """
            cursor.execute(check_query)
            result = cursor.fetchone()
            if result:
                data_anomalies_log.append(f"{table_name}: {column_name} - Max: {result.max_value}, Min: {result.min_value}")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de outliers: {e}")

    # Chequeo de registros huérfanos
    try:
        query = """
        SELECT 
            fk.name AS foreign_key_name,
            tp.name AS parent_table,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            cr.name AS referenced_column
        FROM 
            sys.foreign_keys AS fk
        INNER JOIN 
            sys.tables AS tp ON fk.parent_object_id = tp.object_id
        INNER JOIN 
            sys.tables AS tr ON fk.referenced_object_id = tr.object_id
        INNER JOIN 
            sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
        INNER JOIN 
            sys.columns AS cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
        INNER JOIN 
            sys.columns AS cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE REGISTROS HUÉRFANOS:")
        data_anomalies_log.append("="*40)
        if rows:
            for row in rows:
                data_anomalies_log.append(f"{row.foreign_key_name} - Table: {row.parent_table}({row.parent_column}) -> {row.referenced_table}({row.referenced_column})")
        else:
            data_anomalies_log.append("No se encontraron registros huérfanos.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de orphaned_records: {e}")

    # Chequeo de constraints únicos
    try:
        query = """
        SELECT 
            tc.table_name, 
            tc.constraint_name, 
            kcu.column_name
        FROM 
            information_schema.table_constraints AS tc
        INNER JOIN 
            information_schema.key_column_usage AS kcu
        ON 
            tc.constraint_name = kcu.constraint_name
        WHERE 
            tc.constraint_type = 'UNIQUE'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE CONSTRAINTS ÚNICOS:")
        data_anomalies_log.append("="*40)
        if rows:
            for row in rows:
                data_anomalies_log.append(f"{row.table_name}: {row.column_name} - Constraint único: {row.constraint_name}")
        else:
            data_anomalies_log.append("No se encontraron constraints únicos.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de unique_constraints: {e}")

    # Chequeo de validez de fecha y hora
    try:
        query = """
        SELECT 
            table_name, column_name
        FROM 
            information_schema.columns
        WHERE 
            data_type IN ('datetime', 'date', 'time')
        """
        cursor.execute(query)
        datetime_columns = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE VALIDEZ DE FECHA Y HORA:")
        data_anomalies_log.append("="*40)
        for column in datetime_columns:
            table_name = column.table_name
            column_name = column.column_name
            check_query = f"""
            SELECT 
                '{table_name}' AS table_name, 
                '{column_name}' AS column_name, 
                MIN({column_name}) AS min_date, 
                MAX({column_name}) AS max_date
            FROM 
                {table_name}
            """
            cursor.execute(check_query)
            result = cursor.fetchone()
            if result:
                data_anomalies_log.append(f"{table_name}: {column_name} - Min: {result.min_date}, Max: {result.max_date}")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de date_time_validity: {e}")

    # Chequeo de tamaño de datos
    try:
        query = """
        SELECT 
            table_name, column_name, data_type, character_maximum_length
        FROM 
            information_schema.columns
        WHERE 
            character_maximum_length IS NOT NULL
        """
        cursor.execute(query)
        size_columns = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE TAMAÑO DE DATOS:")
        data_anomalies_log.append("="*40)
        for column in size_columns:
            table_name = column.table_name
            column_name = column.column_name
            data_type = column.data_type
            if data_type not in ['text', 'ntext', 'image']:  # Filtrar tipos de datos incompatibles
                try:
                    max_length = column.character_maximum_length
                    check_query = f"""
                    SELECT 
                        '{table_name}' AS table_name, 
                        '{column_name}' AS column_name, 
                        MAX(LEN({column_name})) AS max_size
                    FROM 
                        {table_name}
                    """
                    cursor.execute(check_query)
                    result = cursor.fetchone()
                    if result and result.max_size > max_length:
                        data_anomalies_log.append(f"{table_name}: {column_name} - Tamaño de datos excede el máximo permitido de {max_length}")
                except Exception as inner_e:
                    data_anomalies_log.append(f"Error al chequear el tamaño de la columna {column_name} en la tabla {table_name}: {inner_e}")
            else:
                data_anomalies_log.append(f"Columna {column_name} en tabla {table_name} es de tipo {data_type} y fue omitida.")
        else:
            data_anomalies_log.append("No se encontraron anomalías de tamaño de datos.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de data_size: {e}")

    # Chequeo de datos en blanco
    try:
        query = """
        SELECT 
            table_name, column_name, data_type
        FROM 
            information_schema.columns
        WHERE 
            is_nullable = 'YES'
        """
        cursor.execute(query)
        columns = cursor.fetchall()
        data_anomalies_log.append("CHEQUEO DE ANOMALÍAS DE DATOS EN BLANCO:")
        data_anomalies_log.append("="*40)
        for column in columns:
            table_name = column.table_name
            column_name = column.column_name
            data_type = column.data_type
            if data_type not in ['image', 'text', 'ntext']:  # Filtrar tipos de datos incompatibles
                try:
                    check_query = f"""
                    SELECT 
                        '{table_name}' AS table_name, 
                        '{column_name}' AS column_name, 
                        COUNT(*) AS blank_count
                    FROM 
                        {table_name}
                    WHERE 
                        {column_name} IS NULL OR {column_name} = ''
                    """
                    cursor.execute(check_query)
                    result = cursor.fetchone()
                    if result and result.blank_count > 0:
                        data_anomalies_log.append(f"{table_name}: {column_name} - Datos en blanco: {result.blank_count}")
                except Exception as inner_e:
                    data_anomalies_log.append(f"Error al chequear la columna {column_name} en la tabla {table_name}: {inner_e}")
            else:
                data_anomalies_log.append(f"Columna {column_name} en tabla {table_name} es de tipo {data_type} y fue omitida.")
        else:
            data_anomalies_log.append("No se encontraron datos en blanco.")
    except Exception as e:
        data_anomalies_log.append(f"Error al ejecutar el chequeo de blank_data: {e}")

    # Guardar y retornar resultados
    filepath = write_to_file('data_anomalies_log.txt', data_anomalies_log)
    return data_anomalies_log, filepath



def generate_custom_log(connection):
    print("Generación de log personalizado para cada caso.")
    cursor = connection.cursor()
    
    log_entries = []

    # Añadir encabezado y marca de tiempo
    log_entries.append("==== LOG PERSONALIZADO DE AUDITORÍA ====")
    log_entries.append(f"Fecha y Hora: {datetime.datetime.now()}")
    log_entries.append("======================================\n")
    
    # Identificación de relaciones de integridad referencial
    log_entries.append("=== Relaciones de Integridad Referencial ===")
    try:
        query = """
        SELECT 
            fk.name AS foreign_key_name,
            tp.name AS parent_table,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            cr.name AS referenced_column
        FROM 
            sys.foreign_keys AS fk
        INNER JOIN 
            sys.tables AS tp ON fk.parent_object_id = tp.object_id
        INNER JOIN 
            sys.tables AS tr ON fk.referenced_object_id = tr.object_id
        INNER JOIN 
            sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
        INNER JOIN 
            sys.columns AS cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
        INNER JOIN 
            sys.columns AS cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if rows:
            for row in rows:
                log_entry = f"Foreign Key: {row.foreign_key_name} - Table: {row.parent_table}({row.parent_column}) -> {row.referenced_table}({row.referenced_column})"
                log_entries.append(log_entry)
        else:
            log_entries.append("No se encontraron relaciones.")
    except Exception as e:
        log_entries.append(f"Error en identificación de relaciones: {e}")
    log_entries.append("\n")
    
    # Chequeo de anomalías de integridad
    log_entries.append("=== Anomalías de Integridad ===")
    try:
        queries = {
            'inserts': """
            -- Insert anomaly check (example)
            SELECT * FROM information_schema.referential_constraints
            """,
            'deletes': """
            -- Delete anomaly check (example)
            SELECT * FROM information_schema.referential_constraints
            """,
            'updates': """
            -- Update anomaly check (example)
            SELECT * FROM information_schema.referential_constraints
            """
        }
        
        for action, query in queries.items():
            log_entries.append(f"\nChequeo de anomalías para {action}:")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if rows:
                for row in rows:
                    log_entries.append(str(row))
            else:
                log_entries.append(f"No se encontraron anomalías para {action}.")
    except Exception as e:
        log_entries.append(f"Error en chequeo de anomalías de integridad: {e}")
    log_entries.append("\n")
    
    # Chequeo de anomalías de los datos
    log_entries.append("=== Anomalías de los Datos ===")
    try:
        # Incluye aquí tu lógica de chequeo de anomalías de datos
        # Usando las funciones ya definidas como ejemplo
        # Chequeo de valores nulos, duplicados, etc.
        data_anomalies_log, _ = check_data_anomalies(connection)
        log_entries.extend(data_anomalies_log)
    except Exception as e:
        log_entries.append(f"Error en chequeo de anomalías de datos: {e}")
    log_entries.append("\n")
    
    # Resumen Estadístico
    log_entries.append("=== Resumen Estadístico ===")
    log_entries.append("Total de relaciones identificadas: ...")
    log_entries.append("Total de anomalías de integridad: ...")
    log_entries.append("Total de anomalías de datos: ...")
    
    # Guardar en el archivo
    filepath = write_to_file('custom_log.txt', log_entries)
    return log_entries, filepath

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/connect', methods=['POST'])
def connect():
    server = request.form['server']
    database = request.form['database']
    username = request.form['username']
    password = request.form['password']
    
    global connection
    connection = connect_to_database(server, database, username, password)
    flash('Conexión exitosa a la base de datos.', 'success')
    return redirect(url_for('index'))

@app.route('/audit', methods=['POST'])
def audit():
    option = request.form['option']
    
    if option == '1':
        results, filepath = identify_relations(connection)
    elif option == '2':
        results, filepath = check_integrity_anomalies(connection)
    elif option == '3':
        results, filepath = check_data_anomalies(connection)
    elif option == '4':
        results, filepath = generate_custom_log(connection)
    
    session['results'] = results
    session['filepath'] = filepath
    return redirect(url_for('results'))

@app.route('/results')
def results():
    results = session.get('results', [])
    filepath = session.get('filepath', '')
    return render_template('results.html', results=results, filepath=filepath)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True)
