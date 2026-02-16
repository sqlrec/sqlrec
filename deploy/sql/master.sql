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

CREATE TABLE if not exists model (
    name VARCHAR(255) NOT NULL PRIMARY KEY,
    ddl text NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE if not exists checkpoint (
    model_name VARCHAR(255) NOT NULL,
    checkpoint_name VARCHAR(255) NOT NULL,
    ddl text NOT NULL,
    yaml text NOT NULL,
    checkpoint_type VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (model_name, checkpoint_name)
);