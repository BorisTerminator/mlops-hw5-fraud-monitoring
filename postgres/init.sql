-- Local educational account: Grafana is only allowed to read scores.
CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_reader';
GRANT CONNECT ON DATABASE fraud_db TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE fraud_user IN SCHEMA public
    GRANT SELECT ON TABLES TO grafana_reader;
