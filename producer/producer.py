import json, time, random, datetime
import kafka
import prometheus_client

broker ='localhost:9092'

# Metricas de prometheus. Se recogen cada 15 segundos
total_enviados=prometheus_client.Counter('producer_records_sent_total','Registros enviados', ['id_usuario', 'envenenado'])
total_ataques =prometheus_client.Counter('producer_attacks_total','Ataques por tipo', ['tipo_ataque'])
gauge_precio= prometheus_client.Gauge('producer_last_price', 'Último precio', ['id_usuario'])
gauge_cantidad=prometheus_client.Gauge('producer_last_quantity', 'Última cantidad', ['id_usuario'])
latencia =prometheus_client.Summary('producer_send_latency_seconds', 'Latencia envío')

def inicio_pipeline():
    prometheus_client.start_http_server(8000)

    # Conexión a kafka. Está en docker
    productor_mensajes =None
    for intento in range(10):
        try:
            productor_mensajes = kafka.KafkaProducer(bootstrap_servers=broker,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            break
        except kafka.errors.NoBrokersAvailable:
            print('Intento ' + str(intento+1) + ' - Kafka no responde')
            time.sleep(5)
    if not productor_mensajes:
        print('No se pudo conectar a Kafka tras 10 intentos')
        return
    print('Conectado a Kafka')

    ataques=['outlier_injection',
             'gradual_drift',
             'label_flipping',
             'random_noise']
    categorias = ['Electronica',
                  'Ropa',
                  'Alimentacion',
                  'Libros',
                  'Hogar']
    users=[f'User_{i:02d}' for i in range(1, 6)]
    estado ={'registros': 0, 'drift': 0.0, 'descartados': 0}
    

    # Bucle principal. Genera transacciones cada dos segundos y se publican en kafka
    while True:
        try:
            for uid in users:
                precio =round(random.uniform(5.0, 200.0), 2)
                cantidad=random.randint(1, 10)
                categoria=random.choice(categorias)
                envenenado=False
                tipo=None

                # El 15% de las transacciones se envenenan para la simulación
                if random.random() < 0.15:
                    tipo=random.choice(ataques)
                    envenenado =True
                    estado['drift']+= 0.1

                    if tipo=='outlier_injection':
                        if random.random() < 0.5:
                            precio=round(random.uniform(5000.0, 15000.0), 2)
                        else:
                            cantidad=random.randint(500, 1000)
                    elif tipo=='gradual_drift':
                        precio=round(precio + 5.0 * estado['drift'], 2)
                        cantidad+=int(estado['drift'])
                    elif tipo=='random_noise':
                        precio=round(abs(precio + random.gauss(0, 50)), 2)
                        cantidad=max(1, cantidad + int(random.gauss(0, 3)))

                if precio<0:
                    precio=0.01
                if cantidad<1:
                    cantidad=1

                if precio > 5000:
                    estado['descartados']+=1
                    print('WARN: precio sospechoso ' + str(precio) + '€ para ' + uid + '')
                    continue

                if categoria not in categorias:
                    print('Categoría no válida: ' + categoria)
                    continue

                diccionario_transaccion={
                    'id_transaccion':str(random.randint(10000000, 99999999)),
                    'id_usuario':uid,
                    'marca_tiempo':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'precio':precio,
                    'cantidad':cantidad,
                    'categoria':categoria,
                    'envenenado':envenenado,
                    'tipo_ataque':tipo,
                }

                # Se publica en kafka
                with latencia.time():
                    productor_mensajes.send('transactions', value=diccionario_transaccion)
                total_enviados.labels(id_usuario=uid, envenenado=str(envenenado)).inc()
                gauge_precio.labels(id_usuario=uid).set(precio)
                gauge_cantidad.labels(id_usuario=uid).set(cantidad)
                if envenenado:
                    total_ataques.labels(tipo_ataque=tipo).inc()
                estado['registros']+=1
                if envenenado:
                    etiqueta=f'Envenenado ({tipo})'
                else:
                    etiqueta=f'Normal'
                print('[' + str(estado["registros"]) + '] ' + uid + ' | ' + str(precio) + '€ | x' + str(cantidad) + ' | ' + categoria + ' | ' + etiqueta)
            productor_mensajes.flush()
            time.sleep(2)

        except KeyboardInterrupt:
            print('Parado. Enviados: ' + str(estado["registros"]) + ' | Descartados: ' + str(estado["descartados"]))
            productor_mensajes.close()
            break
inicio_pipeline()