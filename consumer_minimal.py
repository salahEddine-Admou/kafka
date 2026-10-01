import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:29092,localhost:39092,localhost:49092",
    "group.id": "verdalis-dashboard-1",   # nom du groupe = identité du lecteur
    "auto.offset.reset": "earliest",    # sans offset commité : depuis le début
    "enable.auto.commit": False,        # on commite nous-mêmes
})
consumer.subscribe(["verdalis.mesures"])

try:
    while True:
        msg = consumer.poll(1.0)          # attend au plus 1 s
        if msg is None:
            continue
        if msg.error():
            print("Erreur :", msg.error())
            continue
        mesure = json.loads(msg.value())
        print(msg.key().decode(), mesure, f"p{msg.partition()}@{msg.offset()}")
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()                    # quitte le groupe proprement
