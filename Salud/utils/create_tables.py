def crear_tablas():
    return """

        CREATE TABLE IF NOT EXISTS periodo(
            Periodo_key DATE PRIMARY KEY,
            anio INT,
            trimestre INT
        );

        CREATE TABLE IF NOT EXISTS mineria(
            Mineria_Key INT PRIMARY KEY,
            departamento VARCHAR(150),
            recurso VARCHAR(150),
            nombre VARCHAR(150),
            unidad VARCHAR(150),
            valor DECIMAL,
            tipo VARCHAR(150),
            cantidad DECIMAL
        );

        CREATE TABLE IF NOT EXISTS departamento(
            Departamento_Key INT PRIMARY KEY,
            codigo INT,
            departamento_nombre VARCHAR(150)

        );

        CREATE TABLE IF NOT EXISTS demografia(
            Demografia_Key INT PRIMARY KEY,
            indicador VARCHAR(150),
            indicador VARCHAR(150),
            valorNumerico INT,
            departamento VARCHAR(150),


        );

        CREATE TABLE IF NOT EXISTS indicador(
            Indicador_Key INT PRIMARY KEY,
            concepto VARCHAR(150),
        );

        CREATE TABLE IF NOT EXISTS salud(
            Salud_Key INT PRIMARY KEY,
            codigoEntidad VARCHAR(150),
            entidad VARCHAR(150),
            indicador VARCHAR(150),
            valorNumerico INT,

        );


        CREATE TABLE IF NOT EXISTS fact_order(
            Periodo_key INT PRIMARY KEY,
            Mineria_Key INT REFERENCES mineria (Mineria_Key),
            Departamento_Key INT REFERENCES departamento (departamento_key),
            Demografia_Key INT REFERENCES demografia (demografia_key),
            Indicador_Key DATE REFERENCES indicador (indicador_key),
            Salud_Key DATE REFERENCES salud (salud),
            porcentaje_acceso DECIMAL,
            CantidadInicial INT,
            CantidadFinal INT,
            CantidadPromedio INT,

        ); """
