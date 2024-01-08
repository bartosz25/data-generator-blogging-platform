CREATE SCHEMA generator_schema;

-- enable PostGis
CREATE EXTENSION postgis;

CREATE TABLE generator_schema.visits (
  visit_id VARCHAR(40) NOT NULL,
  event_time TIMESTAMP NOT NULL,
  user_id TEXT NOT NULL,
  page VARCHAR(20) NOT NULL,
  PRIMARY KEY (visit_id, event_time)
);

ALTER TABLE generator_schema.visits REPLICA IDENTITY FULL;


