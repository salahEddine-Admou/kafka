import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:29092,localhost:39092,localhost:49092",
    "acks": "all",                  # attendre les répliques synchronisées
    "enable.idempotence": True,     # pas de doublon en cas de retry
})

def rapport(err, msg):              # appelé quand le broker a répondu
    if err:
        print("Échec :", err)
    else:
        print(f"OK -> partition {msg.partition()}, offset {msg.offset()}")

mesure = {"serre_id": "serre-07", "type": "temperature", "valeur": 24.1}
producer.produce("verdalis.mesures",
                 key="serre-06",
                 value=json.dumps(mesure),
                 callback=rapport)

producer.flush(10)                  # vider le tampon avant de quitter
