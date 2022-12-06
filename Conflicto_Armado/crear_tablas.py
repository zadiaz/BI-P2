def crear_tablas():
    return """
    
        

        CREATE TABLE IF NOT EXISTS municipios(
            id INT PRIMARY KEY,
            municipio VARCHAR(150),
            departamento VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS recursos(
            id INT PRIMARY KEY,
            tipo VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS fechas(
            id INT PRIMARY KEY,
            anio INT
        );

        CREATE TABLE IF NOT EXISTS actividades_mineras(
            id INT PRIMARY KEY,
            geografia_id INT REFERENCES geografia (id),
            recurso_id INT REFERENCES recurso (id),
            fecha_id INT REFERENCES fecha (id),
            unidad VARCHAR(20),
            cantidad INT
        );

        CREATE TABLE IF NOT EXISTS homicidios(
            id INT PRIMARY KEY,
            geografia_id INT REFERENCES geografia (id),
            fecha_id INT REFERENCES fecha (id),
            cantidad INT,
            desplazamientos INT
        );

    """
