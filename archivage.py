"""Exercice 5 — archivage des mesures dans un CSV (corrigé)."""
import csv
import json
import sys

from confluent_kafka import Consumer

GROUPE = sys.argv[1] if len(sys.argv) > 1 else "verdalis-archivage"

consumer = Consumer({
    "bootstrap.servers": "localhost:29092,localhost:39092,localhost:49092",
    "group.id": GROUPE,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,        # acceptable ici : un doublon dans un CSV n'est pas grave
})
consumer.subscribe(["verdalis.mesures"])

with open(f"{GROUPE}.csv", "a", newline="", encoding="utf-8") as f:
    ecrivain = csv.writer(f)
    lignes = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            m = json.loads(msg.value())
            ecrivain.writerow([m["ts"], m["serre_id"], m["type"], m["valeur"],
                               msg.partition(), msg.offset()])
            lignes += 1
            if lignes % 1000 == 0:
                f.flush()
                print(f"{lignes} lignes archivées")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
