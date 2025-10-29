-- Database: dvdrental

-- DROP DATABASE IF EXISTS dvdrental;

CREATE DATABASE dvdrental
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Costa Rica.1252'
    LC_CTYPE = 'Spanish_Costa Rica.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Tablas datamart -------------------------------------------------------
CREATE TABLE dim_lugar (
    id_dim_lugar SERIAL PRIMARY KEY,
    pais TEXT NOT NULL,
    ciudad TEXT NOT NULL,
    lugar TEXT NOT NULL,
    CONSTRAINT uq_dim_lugar UNIQUE (pais, ciudad, lugar)
);

CREATE TABLE dim_sucursal (
    id_dim_sucursal SERIAL PRIMARY KEY,
    id_sucursal INTEGER NOT NULL UNIQUE,
    FOREIGN KEY (id_sucursal) REFERENCES store(store_id)
);

CREATE TABLE dim_pelicula (
    id_dim_pelicula SERIAL PRIMARY KEY,
    categoria TEXT NOT NULL,
    actores TEXT NOT NULL,
    titulo TEXT NOT NULL,
    CONSTRAINT uq_dim_pelicula UNIQUE (categoria, actores, titulo)
);

CREATE TABLE dim_fecha (
    id_dim_fecha SERIAL PRIMARY KEY,
    anio INTEGER CHECK (anio BETWEEN 0 and 10000),
    mes INTEGER CHECK (mes BETWEEN 1 and 12), 
    dia INTEGER CHECK (dia BETWEEN 1 and 31),
    CONSTRAINT uq_dim_fecha UNIQUE (anio, mes, dia)
);

CREATE TABLE fact_rental (
    id_fact_rental SERIAL PRIMARY KEY,
    id_dim_lugar INTEGER,
    id_dim_sucursal INTEGER,
    id_dim_pelicula INTEGER,
    id_dim_fecha INTEGER,
    cont_renta INTEGER CHECK (cont_renta = 1), -- siempre 1,
    monto INTEGER,
    FOREIGN KEY (id_dim_lugar) REFERENCES dim_lugar(id_dim_lugar),
    FOREIGN KEY (id_dim_sucursal) REFERENCES dim_sucursal(id_dim_sucursal),
    FOREIGN KEY (id_dim_pelicula) REFERENCES dim_pelicula(id_dim_pelicula),
    FOREIGN KEY (id_dim_fecha) REFERENCES dim_fecha(id_dim_fecha)
);

-- Procedimientos para alimentar el datamart -------------------------------------

/* 
	Nombre: proc_cargar_dim_lugar
	Descripción: Este procedimiento se encarga de llenar la dimensión "dim_lugar" con los 
	datos de país, ciudad y dirección (lugar) a partir de las tablas de dvdrental
    "address", "city" y "country". Antes de insertar los datos, se eliminan los registros
    existentes para evitar duplicados y se reinicia el contador de la clave primaria.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; realiza una operación de carga en la tabla dim_lugar.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_dim_lugar()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Quitamos los datos antiguos si hay
    TRUNCATE TABLE dim_lugar RESTART IDENTITY CASCADE;

	-- Llenamos dim_lugar
    INSERT INTO dim_lugar (pais, ciudad, lugar)
    SELECT DISTINCT 
        co.country AS pais,
        ci.city AS ciudad,
        a.address AS lugar
    FROM address a
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id;
END;
$$;

/* 
	Nombre: proc_cargar_dim_sucursal
	Descripción: Este procedimiento se encarga de llenar la dimensión "dim_sucursal" 
	con los datos de sucursales a partir de la tabla "store" de la base de datos dvdrental.
	Antes de insertar los datos, se eliminan los registros existentes para evitar duplicados 
	y se reinicia el contador de la clave primaria.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; realiza una operación de carga en la tabla dim_sucursal.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_dim_sucursal()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Quitamos los datos antiguos si hay
    TRUNCATE TABLE dim_sucursal RESTART IDENTITY CASCADE;

	-- Llenamos dim_sucursal
    INSERT INTO dim_sucursal (id_sucursal)
    SELECT store_id FROM store;
END;
$$;

/* 
	Nombre: proc_cargar_dim_pelicula
	Descripción: Este procedimiento se encarga de llenar la dimensión "dim_pelicula" 
	con los datos de categoría, actores y título a partir de las tablas "film", "category", 
	"film_category", "actor" y "film_actor" de la base de datos dvdrental. Antes de insertar 
	los datos, se eliminan los registros existentes para evitar duplicados y se reinicia el 
	contador de la clave primaria. Los actores se agrupan en una cadena separada por comas 
	para cada película.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; realiza una operación de carga en la tabla dim_pelicula.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_dim_pelicula()
LANGUAGE plpgsql
AS $$
BEGIN
	-- -- Quitamos los datos antiguos si hay
    TRUNCATE TABLE dim_pelicula RESTART IDENTITY CASCADE;

	-- Llenamos dim_pelicula
    INSERT INTO dim_pelicula (categoria, actores, titulo)
    SELECT DISTINCT
        c.name AS categoria,
        STRING_AGG(a.first_name || ' ' || a.last_name, ', ' ORDER BY a.first_name) AS actores,
        f.title AS titulo
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    JOIN film_actor fa ON f.film_id = fa.film_id
    JOIN actor a ON fa.actor_id = a.actor_id
    GROUP BY c.name, f.title;
END;
$$;

/* 
	Nombre: proc_cargar_dim_fecha
	Descripción: Este procedimiento se encarga de llenar la dimensión "dim_fecha" 
	con los datos de año, mes y día a partir de la tabla "rental" de la base de datos dvdrental. 
	Antes de insertar los datos, se eliminan los registros existentes para evitar duplicados 
	y se reinicia el contador de la clave primaria. Solo se consideran las fechas de alquiler 
	que no sean nulas.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; realiza una operación de carga en la tabla dim_fecha.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_dim_fecha()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Quitamos los datos antiguos si hay
    TRUNCATE TABLE dim_fecha RESTART IDENTITY CASCADE;

	-- Llenamos dim_fecha
    INSERT INTO dim_fecha (anio, mes, dia)
    SELECT DISTINCT
        EXTRACT(YEAR FROM rental_date)::INTEGER AS anio,
        EXTRACT(MONTH FROM rental_date)::INTEGER AS mes,
        EXTRACT(DAY FROM rental_date)::INTEGER AS dia
    FROM rental
    WHERE rental_date IS NOT NULL;
END;
$$;

/* 
	Nombre: proc_cargar_fact_rental
	Descripción: Este procedimiento se encarga de alimentar la tabla de hechos 
	fact_rental del datamart a partir de los datos de dvdrental.
	Realiza una carga completa (limpia e inserta) para las dimensiones de lugar, 
	sucursal, película y fecha. Además, calcula el monto de cada alquiler según
	los días transcurridos entre la fecha de renta y la fecha de devolución (si aplica).

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; realiza una operación de carga en la tabla fact_rental.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_fact_rental()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Quitamos los datos antiguos si hay
    TRUNCATE TABLE fact_rental RESTART IDENTITY;

	-- Llenamos fact_rental
    INSERT INTO fact_rental (id_dim_lugar, id_dim_sucursal, id_dim_pelicula, id_dim_fecha, cont_renta, monto)
    SELECT 
		dl.id_dim_lugar,
        ds.id_dim_sucursal,
        dp.id_dim_pelicula,
        df.id_dim_fecha,
        1 AS cont_renta, -- Siempre 1 (para contar los alquileres)
        CASE -- Monto del alquiler (días × 500)
            WHEN r.return_date IS NOT NULL THEN 
                500 * GREATEST(1, DATE_PART('day', r.return_date - r.rental_date)::INTEGER)
            ELSE 500  -- Si no ha sido devuelto, asumimos 500 (1 día)
        END AS monto
    FROM rental r
	-- Uniones con el sistema transaccional
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    JOIN store s ON i.store_id = s.store_id
    JOIN customer cu ON r.customer_id = cu.customer_id
	JOIN address a ON cu.address_id = a.address_id 
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
	-- Uniones con las tablas de dimensión 
    JOIN dim_lugar dl ON co.country = dl.pais AND ci.city = dl.ciudad AND a.address = dl.lugar
    JOIN dim_sucursal ds ON s.store_id = ds.id_sucursal
    JOIN dim_pelicula dp ON c.name = dp.categoria AND f.title = dp.titulo
    JOIN dim_fecha df ON 
        EXTRACT(YEAR FROM r.rental_date) = df.anio AND
        EXTRACT(MONTH FROM r.rental_date) = df.mes AND
        EXTRACT(DAY FROM r.rental_date) = df.dia;
END;
$$;

/* 
	Nombre: proc_cargar_datamart_completo
	Descripción: Este procedimiento llama a los otros procedimientos
	encargados de alimentar al datamart.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor.
*/
CREATE OR REPLACE PROCEDURE proc_cargar_datamart_completo()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL proc_cargar_dim_lugar();
    CALL proc_cargar_dim_sucursal();
    CALL proc_cargar_dim_pelicula();
    CALL proc_cargar_dim_fecha();
    CALL proc_cargar_fact_rental();
END;
$$;

CALL proc_cargar_datamart_completo();

-- Consulta de datos (para filtros por Tableau) ---------------------------------------------------------
/* 
	Nombre: vw_fact_pelicula_actor
	Descripción: Esta vista genera una tabla de hechos por película y actor a partir de
	la tabla de hechos "fact_rental" y la dimensión "dim_pelicula" del datamart.
	Cada fila representa un alquiler individual de una película, asociado a cada actor
	que participó en ella. Para los actores, se descompone la cadena de nombres concatenados
	en la dimensión "dim_pelicula" utilizando la función unnest, creando una fila por cada actor.
	La vista incluye información de lugar, sucursal, fecha, cantidad de rentas (cont_renta)
	y monto del alquiler.

	Parámetros:
	(no recibe parámetros)

	Retorna:
	void -> No retorna ningún valor; es una vista que muestra los datos de por película y actor.
*/
CREATE OR REPLACE VIEW vw_fact_pelicula_actor AS
SELECT
    fr.id_fact_rental,
    fr.id_dim_lugar,
    fr.id_dim_sucursal,
    fr.id_dim_pelicula,
    fr.id_dim_fecha,
    TRIM(actor) AS actor,
    fr.cont_renta,
    fr.monto
FROM fact_rental fr
JOIN dim_pelicula dp
  ON dp.id_dim_pelicula = fr.id_dim_pelicula
CROSS JOIN LATERAL unnest(string_to_array(dp.actores, ',')) AS actor;
