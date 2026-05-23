CREATE TABLE IF NOT EXISTS transacciones_raw (
    id SERIAL PRIMARY KEY,
    id_transaccion VARCHAR(50) NOT NULL,
    id_usuario VARCHAR(50) NOT NULL,
    marca_tiempo TIMESTAMP NOT NULL,
    precio DOUBLE PRECISION,
    cantidad INTEGER,
    categoria VARCHAR(50),
    envenenado BOOLEAN DEFAULT FALSE,
    tipo_ataque VARCHAR(50),
    fecha_ingesta TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metricas_integridad (
    id SERIAL PRIMARY KEY,
    marca_tiempo TIMESTAMP NOT NULL,
    nombre_kpi VARCHAR(100) NOT NULL,
    valor_kpi DOUBLE PRECISION NOT NULL,
    umbral DOUBLE PRECISION,
    es_anomalia BOOLEAN DEFAULT FALSE,
    inicio_ventana TIMESTAMP,
    fin_ventana TIMESTAMP
);

CREATE INDEX idx_tx_tiempo ON transacciones_raw(marca_tiempo);
CREATE INDEX idx_tx_envenenado ON transacciones_raw(envenenado);
CREATE INDEX idx_metric_tiempo ON metricas_integridad(marca_tiempo);
CREATE INDEX idx_metric_anomalia ON metricas_integridad(es_anomalia);