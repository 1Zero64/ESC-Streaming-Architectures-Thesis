CREATE TABLE IF NOT EXISTS public.event_store
(
    id bigint NOT NULL DEFAULT nextval('"eventStore_Id_seq"'::regclass),
    sensor_id bigint NOT NULL,
    temperature real NOT NULL,
    humidity real NOT NULL,
    event_stream text COLLATE pg_catalog."default" NOT NULL,
    created_on timestamp without time zone NOT NULL,
    processed_on timestamp without time zone NOT NULL,
    CONSTRAINT "Id" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.materialized_view
(
    id bigint NOT NULL DEFAULT nextval('"materializedView_Id_seq"'::regclass),
    sensor_id bigint NOT NULL,
    temperature real NOT NULL,
    humidity real NOT NULL,
    event_stream text COLLATE pg_catalog."default" NOT NULL,
    danger text COLLATE pg_catalog."default" NOT NULL,
    created_on timestamp without time zone NOT NULL,
    processed_on timestamp without time zone NOT NULL,
    latency real NOT NULL,
    CONSTRAINT "materializedView_pkey" PRIMARY KEY (id)
);

