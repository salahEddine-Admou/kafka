"""Exercice 3 — service d'alerte Verdalis (corrigé)."""
import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092"
SEUILS = {  # type : (sens, seuil)
    "temperature": ("max", 32.0),
    "humidite": ("min", 40.0),
    "co2": ("max", 1200.0),
}
LOT = 100  # commit tous les 100 messages

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "verdalis-alerting",
    "client.id": f"alerting-{os.getpid()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
})


def affiche_partitions(_consumer, partitions):
    print("Partitions attribuées :", sorted(p.partition for p in partitions))


def depasse(m):
    sens, seuil = SEUILS[m["type"]]
    return m["valeur"] > seuil if sens == "max" else m["valeur"] < seuil


consumer.subscribe(["verdalis.mesures"], on_assign=affiche_partitions)
non_commites = 0

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur :", msg.error())
            continue

        try:
            m = json.loads(msg.value())
            alerte = depasse(m)
        except (ValueError, KeyError) as exc:
            print(f"Message ignoré p{msg.partition()}@{msg.offset()} : {exc}")
            alerte = False

        if alerte:
            evt = {
                "serre_id": m["serre_id"],
                "type": m["type"],
                "valeur": m["valeur"],
                "seuil": SEUILS[m["type"]][1],
                "ts_mesure": m["ts"],
                "origine": f"p{msg.partition()}@{msg.offset()}",
            }
            producer.produce("verdalis.alertes", key=m["serre_id"], value=json.dumps(evt))
            producer.poll(0)
            print(f"ALERTE {m['serre_id']} {m['type']}={m['valeur']}")

        non_commites += 1
        if non_commites >= LOT:
            producer.flush(10)                   # 1. alertes bien écrites
            consumer.commit(asynchronous=False)  # 2. puis on avance le marque-page
            non_commites = 0
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    consumer.close()
