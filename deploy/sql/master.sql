CREATE TABLE if not exists sql_api (
    name VARCHAR(255) NOT NULL PRIMARY KEY,
    function_name VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE if not exists sql_function (
    name VARCHAR(255) NOT NULL PRIMARY KEY,
    sql_list text NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);