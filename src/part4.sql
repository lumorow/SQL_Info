DROP DATABASE IF EXISTS for_part_4;
CREATE DATABASE for_part_4;

CREATE TABLE "TableName_1"
(
    Opapa_1 varchar
);

CREATE TABLE "TableName_2"
(
    Opapa_2 varchar
);

CREATE TABLE "TableName_3"
(
    Opapa_3 varchar
);

CREATE TABLE "TableName_4"
(
    Opapa_4 varchar
);

CREATE TABLE "_1_TableName_1_"
(
    Opapa_1 varchar
);

CREATE TABLE "_2_TableName_2_"
(
    Opapa_2 varchar
);

CREATE TABLE "_3_TableName_3_"
(
    Opapa_3 varchar
);

CREATE TABLE "_4_TableName_4_"
(
    Opapa_4 varchar
);

-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных,
-- уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.
CREATE OR REPLACE PROCEDURE destroys_tables_begin_with_the_phrase(table_name_begin varchar)
AS
$$
DECLARE
    cp_cmd    varchar;
    table_rec record;
BEGIN
    FOR table_rec IN (SELECT table_name AS tname
                        FROM information_schema.tables
                       WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                         AND table_schema IN ('public', 'myschema')
                         AND table_name SIMILAR TO 'TableName%')
        LOOP
            EXECUTE 'DROP TABLE "' || table_rec.tname || '" cascade';
        END LOOP;
END;
$$ LANGUAGE 'plpgsql';


BEGIN;
CALL destroys_tables_begin_with_the_phrase('TableName');
COMMIT;

-- 2) Создать хранимую процедуру с выходным параметром,
-- которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций.
CREATE OR REPLACE FUNCTION Multiply (A int, B int)
RETURNS INT
AS
$$
BEGIN
RETURN A * B;
END;
$$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION Div (A int, B int)
RETURNS INT
AS
$$
BEGIN
RETURN A / B;
END;
$$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION Sum (A int, B int)
RETURNS INT
AS
$$
BEGIN
RETURN A + B;
END;
$$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION Sub (A int, B int)
RETURNS INT
AS
$$
BEGIN
RETURN A - B;
END;
$$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION Subwithout ()
RETURNS INT
AS
$$
BEGIN
RETURN 15 - 4;
END;
$$
    LANGUAGE 'plpgsql';


SELECT Multiply(3,4);
SELECT Div(8,2);
SELECT Sum(4,3);
SELECT Sub(4,3);
SELECT Subwithout();

CREATE OR REPLACE FUNCTION get_scalar_functions() RETURNS TABLE(scalar_functions text, parameters text)
AS
    $$
    SELECT ROUTINE_NAME::varchar scalar_functions, STRING_AGG(information_schema.parameters.parameter_name,',' )::varchar parameters
    FROM INFORMATION_SCHEMA.ROUTINES
    LEFT JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
    WHERE routines.specific_schema IN ('public', 'myschema') AND parameters.data_type IS NOT NULL
    GROUP BY ROUTINE_NAME;
    $$
LANGUAGE SQL;


CREATE OR REPLACE PROCEDURE scalar_functions (IN ref refcursor, out _count_ int)
AS
    $$
BEGIN
    OPEN ref FOR
    SELECT *
    FROM get_scalar_functions();
    _count_ = count(*) FROM (SELECT * FROM get_scalar_functions()) AS c;
    RETURN;
END;
$$
    LANGUAGE 'plpgsql';

BEGIN;
CALL scalar_functions('cursor_name', NULL);
FETCH ALL IN "cursor_name";
COMMIT;

-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных.
-- Выходной параметр возвращает количество уничтоженных триггеров.


CREATE TABLE "TableName_1"
(
    Opapa_1 varchar
);

CREATE TABLE audit
(
    created    TIMESTAMP WITH TIME ZONE NOT NULL,
    type_event CHAR(1)                  NOT NULL,
    info       VARCHAR
);


CREATE OR REPLACE FUNCTION fnc_trg_TableName_1_audit()
    RETURNS trigger AS
$$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO audit(created, type_event, info)
        VALUES (NOW(), 'I', new.Opapa_1);
        RETURN new;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO audit(created, type_event, info)
        VALUES (NOW(), 'U', new.Opapa_1);
        RETURN new;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO audit(created, type_event, info)
        VALUES (NOW(), 'D', old.Opapa_1);
        RETURN old;
    END IF;
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_audit1
    AFTER INSERT OR UPDATE OR DELETE
    ON "TableName_1"
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_TableName_1_audit();

CREATE OR REPLACE PROCEDURE delete_triggers()
    LANGUAGE plpgsql AS
$$
DECLARE
    v_sql_srt text;
    value     record;
BEGIN
    FOR value IN (SELECT trigger_name, event_object_table FROM information_schema.triggers)
        LOOP
            v_sql_srt := 'DROP TRIGGER IF EXISTS ' || value.trigger_name || ' ON "' || value.event_object_table || '";';
            EXECUTE v_sql_srt;
        END LOOP;
END;
$$;

CALL delete_triggers();


-- 4) Создать хранимую процедуру с входным параметром,
-- которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций),
-- в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.
DROP FUNCTION IF EXISTS outputs_names_and_descriptions_of_object_types(string_in varchar);
CREATE OR REPLACE FUNCTION outputs_names_and_descriptions_of_object_types(string_in varchar) RETURNS TABLE
    (
        name varchar,
        type_object  varchar
    )
AS
$$
BEGIN
   RETURN QUERY  SELECT ROUTINE_NAME::varchar, routine_type::varchar
      FROM INFORMATION_SCHEMA.ROUTINES
     WHERE ROUTINE_TYPE = 'PROCEDURE'
        OR ROUTINE_TYPE = 'FUNCTION'
         AND specific_schema NOT IN ('information_schema', 'pg_catalog')
         AND specific_schema IN ('public', 'myschema') AND ROUTINE_DEFINITION ~ string_in;
END;
$$ LANGUAGE 'plpgsql';

SELECT * FROM outputs_names_and_descriptions_of_object_types('BEGIN');

SELECT ROUTINE_NAME::varchar, routine_type::varchar
      FROM INFORMATION_SCHEMA.ROUTINES

