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

CREATE ROLE video NOLOGIN;
ALTER TABLE public.actor OWNER TO video;
ALTER TABLE public.address OWNER TO video;
ALTER TABLE public.category OWNER TO video;
ALTER TABLE public.city OWNER TO video;
ALTER TABLE public.country OWNER TO video;
ALTER TABLE public.customer OWNER TO video;
ALTER TABLE public.film OWNER TO video;
ALTER TABLE public.film_actor OWNER TO video;
ALTER TABLE public.film_category OWNER TO video;
ALTER TABLE public.inventory OWNER TO video;
ALTER TABLE public.language OWNER TO video;
ALTER TABLE public.payment OWNER TO video;
ALTER TABLE public.rental OWNER TO video;
ALTER TABLE public.staff OWNER TO video;
ALTER TABLE public.store OWNER TO video;

CREATE ROLE EMP;
CREATE ROLE ADMIN;

CREATE USER empleado1 WITH PASSWORD 'pass_emp';
CREATE USER administrador1 WITH PASSWORD 'pass_admin';

GRANT EMP TO empleado1;
GRANT ADMIN TO administrador1;

GRANT EMP TO ADMIN;

REVOKE ALL ON SCHEMA public FROM PUBLIC; 						-- Revoca los permisos sobre el schema public del rol public
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; 	-- Revoca los permisos de execute a todos en schema public
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM EMP; 		-- Le quita el permiso de execute a EMP


GRANT USAGE ON SCHEMA public TO EMP;		-- Permisos para que los roles emp y admin
GRANT USAGE ON SCHEMA public TO ADMIN;		-- puedan usar las funciones

REVOKE ALL ON TABLE public.customer FROM PUBLIC; 
GRANT ALL ON TABLE public.customer TO video;

SELECT * FROM customer;

/* 
	Nombre: insertar_nuevo_cliente
	Descripción: Inserta un nuevo cliente en la BD.

	Parámetros:
		p_store_id INTEGER			-> Id de la tienda.
    	p_first_name VARCHAR(30)	-> Primer nombre del cliente.
    	p_last_name VARCHAR(30)		-> Apellido del cliente.
    	p_email VARCHAR(50)			-> Email del cliente.
    	p_address VARCHAR(50)		-> Dirección del cliente.
		p_city VARCHAR(50)			-> Ciudad del cliente.
		p_district VARCHAR(50)		-> Distrito del cliente.
    	p_phone VARCHAR(20)			-> Teléfono del cliente.
		p_address2 VARCHAR(50)		-> Dirección secundaria del cliente (opcional).
		p_postal_code VARCHAR(5)	-> Codigo postal del cliente (opcional).

	Retorna:
  	Integer -> Corresponde al id del cliente creado
*/
CREATE OR REPLACE FUNCTION insertar_nuevo_cliente(
    p_store_id INTEGER,			-- Cambié esto por integer para usar el id <-- Nota***
    p_first_name VARCHAR(30),
    p_last_name VARCHAR(30),
    p_email VARCHAR(50),
    p_address VARCHAR(50),
	p_city VARCHAR(50),
	p_district VARCHAR(50),
    p_phone VARCHAR(20),
	p_address2 VARCHAR(50) DEFAULT NULL,
	p_postal_code VARCHAR(5) DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_address_id integer;
    v_customer_id integer;
	v_city_id integer; -- Esta variable no estaba definida   <-- Nota***
BEGIN
	-- Se asume que las ciudades posibles ya están en la BD
	SELECT city_id INTO v_city_id FROM city
	WHERE UPPER(city) = UPPER(p_city);

	-- Se busca la dirección del nuevo cliente, si no está se inserta
    SELECT address_id INTO v_address_id FROM address
     WHERE UPPER(address) = UPPER(p_address) AND city_id = v_city_id; -- Qué pasa con el teléfono? TT
    IF v_address_id IS NULL THEN
        INSERT INTO address(address, address2, district, city_id, postal_code, phone, last_update)
        VALUES (INITCAP(p_address),INITCAP(p_address2), INITCAP(p_district), v_city_id, p_postal_code, p_phone, now())
        RETURNING address_id INTO v_address_id;
    END IF;

	-- Se inserta el nuevo cliente
    INSERT INTO customer(store_id, first_name, last_name, email, address_id, activebool, create_date, last_update, active)
    VALUES (p_store_id, INITCAP(p_first_name), INITCAP(p_last_name), LOWER(p_email), v_address_id, TRUE, now(), now(), 1)
    RETURNING customer_id INTO v_customer_id;

    RETURN v_customer_id;
END;
$$;

/* 
	Nombre: registrar_alquiler
	Descripción: Registra un nuevo alquiler y genera su pago asociado.

	Parámetros:
		p_customer_id (INT)		-> Identificador del cliente.
		p_inventory_id (INT)	-> Identificador del item en inventario (película).
		p_staff_id (INT)		-> Identificador del empleado.
		p_monto (NUMERIC(10,2))	-> Monto de pago.

	Retorna:
  	TEXT -> Mensaje descriptivo con el resultado de la operación.
				- Si la pelicula no está disponible para el alquiler
				- Si el alquiler se realizó con éxito
*/
CREATE OR REPLACE FUNCTION registrar_alquiler(
    p_customer_id INT,
    p_inventory_id INT,
    p_staff_id INT,
    p_monto NUMERIC(10,2)
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_available BOOLEAN;
    v_rental_id INT;
BEGIN
	-- Verificar si la copia está disponible 
	SELECT NOT EXISTS (
        SELECT 1
        FROM rental r
        WHERE r.inventory_id = p_inventory_id
          AND r.return_date IS NULL
    )
    INTO v_available;

    IF NOT v_available THEN
        RETURN 'La película no está disponible actualmente.';
    END IF;

    -- Insertar el alquiler
    INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, last_update)
    VALUES (NOW(), p_inventory_id, p_customer_id, p_staff_id, NOW())
    RETURNING rental_id INTO v_rental_id;

    -- Insertar el pago asociado
    INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
    VALUES (p_customer_id, p_staff_id, v_rental_id, p_monto, NOW());

    RETURN format('Alquiler registrado correctamente. ID de alquiler: %s', v_rental_id);
END;
$$;

/* 
	Nombre: registrar_devolucion
	Descripción: Registra la devolución de un alquiler existente.

	Parámetros:
	p_rental_id (INT) -> Identificador del alquiler a devolver.

	Retorna:
	TEXT -> Mensaje indicando el resultado de la operación:
			- Si no existe el alquiler.
			- Si ya fue devuelto.
			- Si se registró correctamente la devolución.
*/
CREATE OR REPLACE FUNCTION registrar_devolucion(
	p_rental_id INT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
	v_rental RECORD;
BEGIN
    -- Verificar que el alquiler exista y no haya sido devuelto
    SELECT *
    INTO v_rental
    FROM rental
    WHERE rental_id = p_rental_id;

    IF NOT FOUND THEN
        RETURN 'No existe un alquiler con ese ID.';
    END IF;

    IF v_rental.return_date IS NOT NULL THEN
        RETURN 'Este alquiler ya fue devuelto.';
    END IF;

    -- Registrar la fecha de devolución
    UPDATE rental
    SET return_date = NOW(),
        last_update = NOW()
    WHERE rental_id = p_rental_id;

    RETURN format('Devolución registrada correctamente para rental_id = %s', p_rental_id);
END;
$$;

/* 
	Nombre: buscar_pelicula
	Descripción: Busca una pelicula a partir del titulo.

	Parámetros:
	p_titulo (VARCHAR2(30)) -> Título o fragmento del título a buscar.

	Retorna:
	TABLE (
      film_id INT,
      title VARCHAR,
      description TEXT,
      release_year YEAR,
      category VARCHAR
	)
	-> Cada fila contiene los datos de una película que cumple con el criterio 
		de búsqueda.
*/
CREATE OR REPLACE FUNCTION buscar_pelicula(
    p_titulo VARCHAR(30)
)
RETURNS TABLE(
    film_id INT,
    title VARCHAR,
    description TEXT,
    release_year YEAR,
    category VARCHAR
) 
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
	-- Se buscan y devuelven las peliculas que contengan p_titulo
    RETURN QUERY 
    SELECT 
		f.film_id,
        f.title,
        f.description,
        f.release_year,
        c.name AS category
    FROM film f
    LEFT JOIN film_category fc ON f.film_id = fc.film_id
    LEFT JOIN category c ON fc.category_id = c.category_id
    WHERE (p_titulo IS NULL OR f.title ILIKE '%' || p_titulo || '%')
    GROUP BY f.film_id, f.title, f.description, f.release_year, c.name
	ORDER BY f.title;
END;
$$;

-- Parte de permisos para las funciones ------------------------------------------------------------------------------------

-- Cambiar las funciones para que el dueño sea video (si se hizo sin set video)
ALTER FUNCTION buscar_pelicula(VARCHAR(30)) OWNER TO video;
ALTER FUNCTION registrar_devolucion(INT) OWNER TO video;
ALTER FUNCTION registrar_alquiler(INT, INT, INT, NUMERIC(10,2)) OWNER TO video;
ALTER FUNCTION insertar_nuevo_cliente(INTEGER, VARCHAR(30), VARCHAR(30), VARCHAR(50), VARCHAR(50), VARCHAR(50), 
										VARCHAR(50), VARCHAR(20), VARCHAR(50), VARCHAR(5)) OWNER TO video;

-- Dar permisos a las funciones especificas para empleados y administradores
GRANT EXECUTE ON FUNCTION registrar_alquiler(INT, INT, INT, NUMERIC(10,2)) TO EMP;
GRANT EXECUTE ON FUNCTION registrar_devolucion(INT) TO EMP;
GRANT EXECUTE ON FUNCTION buscar_pelicula(VARCHAR(30)) TO EMP;
GRANT EXECUTE ON FUNCTION insertar_nuevo_cliente(INTEGER, VARCHAR(30), VARCHAR(30), VARCHAR(50), VARCHAR(50), VARCHAR(50), 
										VARCHAR(50), VARCHAR(20), VARCHAR(50), VARCHAR(5)) TO ADMIN;

-- Esto no debería ser encesario pero lo dejo por si acaso 
-- (pruebenlo sin usar esto, si les da error lo usan)		 <-- Nota***
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM emp;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM admin;

-- Permisos necesarios de video
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO video;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO video;
GRANT USAGE ON SCHEMA public TO video;
GRANT CREATE ON SCHEMA public TO video; --> Para crear las funciones seteado como video

-- Sets para probar las funciones
SET ROLE video;
SET ROLE postgres;
SET ROLE empleado1;
SET ROLE administrador1;
RESET ROLE;

-- Pruebas ----------------------------------------------------------------------
SELECT * FROM public.buscar_pelicula('matrix');
SELECT registrar_alquiler(1, 1, 1, 5.00);
-- "Alquiler registrado correctamente. ID de alquiler: 16056" 		<-- Nota de Prueba (1605#)
SELECT registrar_devolucion(16056);

SELECT insertar_nuevo_cliente(
    1,                		-- store_id
    'Ema',          		-- first_name
    'Villacalencia',       	-- last_name
    'ema@email.com',		-- email
    '47 MySakila Drive',  	-- address (sí existe en la BD)
    'Lethbridge',           -- city
	'Alberta',				-- district
	'00000000'				-- phone
);
-- Nuevo cliente 


/* 
	Para hacer pruebas de insert cliente
	Parámetros:
		p_store_id INTEGER			-> Id de la tienda.
    	p_first_name VARCHAR(30)	-> Primer nombre del cliente.
    	p_last_name VARCHAR(30)		-> Apellido del cliente.
    	p_email VARCHAR(50)			-> Email del cliente.
    	p_address VARCHAR(50)		-> Dirección del cliente.
		p_city VARCHAR(50)			-> Ciudad del cliente.
		p_district VARCHAR(50)		-> Distrito del cliente.
    	p_phone VARCHAR(20)			-> Teléfono del cliente.
		p_address2 VARCHAR(50)		-> Dirección secundaria del cliente.
		p_postal_code VARCHAR(5)	-> Codigo postal del cliente.

*/

select * from store;
select * from address where city_id = 300;
select * from city where city_id = 300;
