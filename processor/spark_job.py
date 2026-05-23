import time, datetime
import numpy as np
import prometheus_client
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, Row

broker ='localhost:9092'
topic= 'transactions'
postgre_url='jdbc:postgresql://localhost:5432/data_poisoning'
postgre_propiedades ={'user':'tfg','password':'tfg1234',
           'driver': 'org.postgresql.Driver'}

# Esquema del JSON que llega desde Kafka
esquema_tx =T.StructType([
    T.StructField('id_transaccion',T.StringType()),
    T.StructField('id_usuario', T.StringType()),
    T.StructField('marca_tiempo',T.StringType()),
    T.StructField('precio',T.DoubleType()),
    T.StructField('cantidad', T.IntegerType()),
    T.StructField('categoria',T.StringType()),
    T.StructField('envenenado',T.BooleanType()),
    T.StructField('tipo_ataque', T.StringType()),
])

# Métricas de prometheus en el puerto 8001 (el producer usa el 8000)
total_prometheus_registros =prometheus_client.Counter('processor_records_processed_total','Registros procesados')
total_prometheus_anomlias=prometheus_client.Counter('processor_anomalies_detected_total', 'Anomalías', ['kpi_name'])
gauge_prometheus_anomalias =prometheus_client.Gauge('processor_kpi_anomaly_rate','Tasa anomalías')
gauge_prometheus_zscore=prometheus_client.Gauge('processor_kpi_zscore_max', 'Z-score máximo', ['field'])
gauge_prometheus_drift = prometheus_client.Gauge('processor_kpi_drift','Drift',['field'])
gauge_prometheus_volumen=prometheus_client.Gauge('processor_kpi_volume', 'Registros ventana')
latencia_prometheus =prometheus_client.Histogram('processor_window_latency_seconds','Latencia ventana')

# Medias anteriores para calcular drift dentro del lote
medias_prev ={'precio': [], 'cantidad': []}
estado ={'ventanas': 0, 'total_registros': 0}


def procesar_lote(batch_df, batch_id):

    if batch_df.isEmpty(): return

    t0=time.time()
    estado['ventanas'] +=1
    numero_registros =batch_df.count()
    estado['total_registros']+=numero_registros
    tiempo_inicio=datetime.datetime.now(datetime.timezone.utc)

    batch_df.select(
        'id_transaccion','id_usuario',
        F.col('marca_tiempo').cast('timestamp').alias('marca_tiempo'),
        'precio','cantidad','categoria','envenenado','tipo_ataque'
    ).write.jdbc(url=postgre_url, table='transacciones_raw',mode='append', properties=postgre_propiedades)

    estadisticas=batch_df.select(
        F.mean('precio').alias('media_precio_ventana'), F.stddev('precio').alias('desviacion_estandar_precio'),F.max('precio').alias('xp'),
        F.mean('cantidad').alias('media_cantidad'),F.stddev('cantidad').alias('desviacion_estandar_cantidad'), F.max('cantidad').alias('xc'),
        F.count('*').alias('n')
    ).collect()[0]
    # print(f'DEBUG estadisticas: {estadisticas}')

    media_precio_ventana=estadisticas['media_precio_ventana'] or 0
    media_cantidad=float(estadisticas['media_cantidad'] or 0)
    desviacion_estandar_precio =estadisticas['desviacion_estandar_precio'] or 0
    desviacion_estandar_cantidad =float(estadisticas['desviacion_estandar_cantidad'] or 0)

    kpis=[]
    tot_anom=0

    # z-score precio
    if numero_registros > 5 and desviacion_estandar_precio > 0:
        zmax_precio =abs(float(estadisticas['xp']) - media_precio_ventana) / desviacion_estandar_precio
        nanom_precio=batch_df.filter(F.abs((F.col('precio').cast('double')-F.lit(media_precio_ventana)) /
                                           F.lit(desviacion_estandar_precio))>2.5).count()
    else:
        zmax_precio=0.0
        nanom_precio =0
    gauge_prometheus_zscore.labels(field='precio').set(zmax_precio)
    if zmax_precio > 2.5:
        total_prometheus_anomlias.labels(kpi_name='zscore_precio').inc()
    tot_anom +=nanom_precio
    kpis.append({'name':'zscore_max_precio','value':zmax_precio, 'threshold':2.5,'es_anomalo':zmax_precio>2.5})

    # z-score cantidad
    if not (numero_registros <= 5 or desviacion_estandar_cantidad == 0):
        zscore_max_cant =abs(float(estadisticas['xc']) - media_cantidad) / desviacion_estandar_cantidad
        nanom_cant=batch_df.filter(F.abs((F.col('cantidad').cast('double')-F.lit(media_cantidad)) / F.lit(desviacion_estandar_cantidad))>2.5).count()
    else:
        zscore_max_cant = 0.0
        nanom_cant=0
    gauge_prometheus_zscore.labels(field='cantidad').set(zscore_max_cant)
    if zscore_max_cant>2.5:
        total_prometheus_anomlias.labels(kpi_name='zscore_cantidad').inc()
    tot_anom+=nanom_cant
    kpis.append({'name':'zscore_max_cantidad','value':zscore_max_cant,'threshold':2.5,'es_anomalo': zscore_max_cant>2.5})

    tasa=tot_anom/(numero_registros*2)
    gauge_prometheus_anomalias.set(tasa)
    anom =tasa>0.20
    kpis.append({'name':'anomaly_rate','value':tasa,'threshold':0.20, 'es_anomalo':anom})
    if anom:
        total_prometheus_anomlias.labels(kpi_name='anomaly_rate').inc()

    # Drift — compara media actual con las 20 ventanas anteriores
    for campo, media in [('precio',media_precio_ventana), ('cantidad',media_cantidad)]:
        h=medias_prev[campo]
        h.append(media)
        if len(h)>20: h.pop(0)
        if len(h) < 3:
            dv=0.0
        else:
            mh=np.mean(h[:-1])
            sh =np.std(h[:-1])
            dv = abs(media-mh)/sh if sh>0 else 0.0
        gauge_prometheus_drift.labels(field=campo).set(dv)
        anom=dv>3.0
        kpis.append({'name': f'drift_{campo}','value':dv,'threshold':3.0,'es_anomalo':anom})
        if anom: total_prometheus_anomlias.labels(kpi_name=f'drift_{campo}').inc()

    gauge_prometheus_volumen.set(numero_registros)
    kpis.append({'name':'data_volume','value':float(numero_registros),'threshold':0,'es_anomalo':False})
    total_prometheus_registros.inc(numero_registros)
    tiempo_final =datetime.datetime.now(datetime.timezone.utc)

    filas=[]
    for k in kpis:
        filas.append(Row(marca_tiempo=datetime.datetime.now(datetime.timezone.utc),
            nombre_kpi=k['name'], valor_kpi=float(k['value']),
            umbral=float(k['threshold']),es_anomalia=bool(k['es_anomalo']),
            inicio_ventana=tiempo_inicio,fin_ventana=tiempo_final))
    batch_df.sparkSession.createDataFrame(filas).write.jdbc(
        url=postgre_url,table='metricas_integridad', mode='append',properties=postgre_propiedades)

    dur=time.time()-t0
    latencia_prometheus.observe(dur)

    alertas = [k for k in kpis if k['es_anomalo']]
    print(f'=== Ventana #{estado["ventanas"]} ===')
    print(f'  Registros: {numero_registros} | Total: {estado["total_registros"]}')
    for k in kpis:
        tag='ALERTA' if k['es_anomalo'] else 'OK'
        print(f'  {k["name"]}: {k["value"]:.4f} (umbral: {k["threshold"]:.2f}) | {tag}')
    print(f'  Alertas: {len(alertas)} | {dur:.3f}s\n')


prometheus_client.start_http_server(8001)
print('Prometheus en :8001')

spark=SparkSession.builder.appName('DetectorDataPoisoning').master('local[*]')\
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1')\
    .config('spark.sql.streaming.forceDeleteTempCheckpointLocation','true').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print('Spark OK')

stream=spark.readStream.format('kafka').option('kafka.bootstrap.servers',broker) \
    .option('subscribe',topic).option('startingOffsets','latest').option('failOnDataLoss','false').load()

parsed=stream.select(
    F.from_json(F.col('value').cast('string'),esquema_tx).alias('d')).select('d.*')

print(f'Topic: {topic} | Ventana: 90s | Z-score: 2.5 | Drift: 3.0')

query=parsed.writeStream.foreachBatch(procesar_lote).trigger(processingTime='90 seconds') \
    .option('checkpointLocation','C:/tmp/spark_checkpoint').start()

print('Streaming OK\n')
query.awaitTermination()