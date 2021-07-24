CREATE SEQUENCE test_id_seq INCREMENT 1
START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE IF NOT EXISTS test
(
    id integer NOT NULL DEFAULT nextval('test_id_seq'::regclass),
    name character varying(255) NOT NULL,
    age integer NOT NULL,
    ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ctime2 timestamp NOT NULL,
    ctime3 date NOT NULL,
    ctime4 bigint NOT NULL
);

INSERT INTO TEST
VALUES  (1,'orca',29,'2019-03-01 08:29:12','2019-03-01 16:28:26','2019-03-01',1551428928),
	    (2,'zhangwei',28,'2019-03-01 09:21:20','2020-03-01 09:21:20','2020-03-01',0);

CREATE INDEX IDX_CTIME ON TEST (CTIME);

CREATE SEQUENCE test2_id_seq INCREMENT 1
START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE IF NOT EXISTS test2
(
    id integer NOT NULL DEFAULT nextval('test2_id_seq'::regclass),
    name character varying(255) NOT NULL,
    age integer,
    PRIMARY KEY (id)
);

INSERT INTO test2( name, age)
VALUES (
        'test',
        20);