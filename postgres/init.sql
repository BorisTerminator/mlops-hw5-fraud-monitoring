-- Local educational account: Grafana is only allowed to read scores.
CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_reader';
GRANT USAGE ON SCHEMA public TO grafana_reader;
DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO grafana_reader', current_database());
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader', current_user);
END $$;
