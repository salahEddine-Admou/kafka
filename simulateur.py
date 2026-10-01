"""Exercice 2 — simulateur de capteurs Verdalis (corrigé)."""
import json
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092"
TOPIC = "verdalis.mesures"
SERRES = [f"serre-{i:02d}" for i in range(1, 41)]
# type : (moyenne, écart-type, unité)
CAPTEURS = {
    "temperature": (24.0, 4.0, "°C"),
    "humidite": (65.0, 10.0, "%"),
    "co2": (800.0, 200.0, "ppm"),
}

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "simulateur-verdalis",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,            # regrouper les messages en lots
    "compression.type": "lz4",
})

erreurs = 0


def rapport(err, msg):
    global erreurs
    if err is not None:
        erreurs += 1
        print(f"ÉCHEC {msg.key().decode()} : {err}")


def mesure(serre, type_, moyenne, ecart, unite):
    return {
        "serre_id": serre,
        "capteur_id": f"{serre}-{type_}",
        "type": type_,
        "valeur": round(random.gauss(moyenne, ecart), 1),
        "unite": unite,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


en_cours = True


def arreter(*_):
    global en_cours
    en_cours = False


signal.signal(signal.SIGINT, arreter)

envoyes = 0
while en_cours:
    for serre in SERRES:
        for type_, (moyenne, ecart, unite) in CAPTEURS.items():
            evt = mesure(serre, type_, moyenne, ecart, unite)
            producer.produce(TOPIC, key=serre, value=json.dumps(evt), callback=rapport)
            envoyes += 1
    producer.poll(0)            # déclenche les callbacks en attente
    print(f"{envoyes} mesures envoyées, {erreurs} échecs")
    time.sleep(2)

restants = producer.flush(10)
print(f"Arrêt. Messages non livrés : {restants}")
