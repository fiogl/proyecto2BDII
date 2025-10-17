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

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON TABLE public.customer FROM PUBLIC; 
GRANT ALL ON TABLE public.customer TO video;

SELECT * FROM customer;

CREATE OR REPLACE FUNCTION insertar_nuevo_cliente(
    p_store VARCHAR(30),
    p_first_name VARCHAR(30),
    p_last_name VARCHAR(30),
    p_email VARCHAR(50),
    p_address VARCHAR(50)
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_address_id integer;
    v_customer_id integer;
BEGIN
    SELECT address_id INTO v_address_id FROM address
     WHERE UPPER(address) = UPPER(p_address) AND city_id = v_city_id;
    IF v_address_id IS NULL THEN
        INSERT INTO address(address)
        VALUES (INITCAP(p_address))
        RETURNING address_id INTO v_address_id;
    END IF;

    INSERT INTO customer(store_id, first_name, last_name, email, address_id, active, create_date)
    VALUES (p_store_id, INITCAP(p_first_name), INITCAP(p_last_name), LOWER(p_email), v_address_id, TRUE, now())
    RETURNING customer_id INTO v_customer_id;

    RETURN v_customer_id;
END;
$$;

ALTER FUNCTION insertar_nuevo_cliente(VARCHAR(30),VARCHAR(30),VARCHAR(30),VARCHAR(50),VARCHAR(50)) OWNER TO video;
