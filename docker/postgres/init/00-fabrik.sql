DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fabrik') THEN
        CREATE ROLE fabrik LOGIN PASSWORD 'fabrik';
    END IF;
END
$$;

SELECT 'CREATE DATABASE fabrik OWNER fabrik'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'fabrik')\gexec

GRANT ALL PRIVILEGES ON DATABASE fabrik TO fabrik;
