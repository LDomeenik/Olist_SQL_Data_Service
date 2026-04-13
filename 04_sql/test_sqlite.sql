DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
    id    INTEGER   PRIMARY KEY,
    name  TEXT
);

INSERT INTO test_table (name) VALUES ('alpha');
INSERT INTO test_table (name) VALUES ('beta');