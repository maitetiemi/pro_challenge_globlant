CREATE TABLE IF NOT EXISTS public.log
(
    id SERIAL PRIMARY KEY,
    transaction text COLLATE pg_catalog."default",
    intable text COLLATE pg_catalog."default",
    rows bigint,
    datetime timestamp without time zone
)