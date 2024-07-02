
-- Eliminar la tabla 'pelicula' y todas sus dependencias
DROP TABLE IF EXISTS pelicula CASCADE;

-- Eliminar la tabla 'usuario' y todas sus dependencias
DROP TABLE IF EXISTS usuario CASCADE;

-- Eliminar la tabla 'estudio' si existe
DROP TABLE IF EXISTS estudio;

CREATE TABLE IF NOT EXISTS USUARIO (
    DNI VARCHAR(9) PRIMARY KEY,
    telefono VARCHAR(9) NOT NULL UNIQUE,
    nombre VARCHAR(15) NOT NULL,
    apellido VARCHAR(25) NOT NULL
);

CREATE TABLE IF NOT EXISTS ESTUDIO (
    id_Estudio BIGINT PRIMARY KEY,
    nombreE VARCHAR(20) NOT NULL,
    paisOrigen VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS PELICULA (
    id_Us VARCHAR(9) NOT NULL,
    id_Est BIGINT NOT NULL,
    id_Pelicula BIGINT PRIMARY KEY,
    precio DECIMAL(10,2) NOT NULL CHECK (precio >= 0),
    titulo VARCHAR(20) NOT NULL,
    duracion_Minutos INT NOT NULL,
    año TIMESTAMP NOT NULL,
    genero VARCHAR(20) NOT NULL,
    valoracion INT,
    CONSTRAINT id_Us_FK FOREIGN KEY (id_Us) REFERENCES USUARIO(DNI),
    CONSTRAINT id_Est_FK FOREIGN KEY (id_Est) REFERENCES ESTUDIO(id_Estudio)
);

--Dar permisos al usuario que ejecuta el programa en mi caso "diego"
GRANT INSERT,SELECT,UPDATE,DELETE ON TABLE USUARIO TO diego;

GRANT INSERT,SELECT,UPDATE,DELETE ON TABLE ESTUDIO TO diego;

GRANT INSERT,SELECT,UPDATE,DELETE ON TABLE PELICULA TO diego;