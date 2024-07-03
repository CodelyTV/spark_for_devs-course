
CREATE TABLE example_table (
    name VARCHAR(10),
    value INT
);

INSERT INTO example_table (name, value)
VALUES
    ('Alice', 10),
    ('Bob', 20),
    ('Alice', 20),
    ('Charlie', 20),
    ('Charlie', 30);