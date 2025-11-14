import random
import json
import logging
import time
from datetime import datetime, timedelta
from quixstreams import Application
import psycopg2

# Pour mémoriser certains enregistrements et générer des doublons
generated_records = []

# Configuration logging
logging.basicConfig(level=logging.DEBUG)

# Fonction pour récupérer les user_id depuis la table clients
def get_user_ids_from_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            dbname="BIG_DATA_DB",
            user="postgres",
            password="widad2003"
        )
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM clients")
        results = cur.fetchall()
        user_ids = [row[0] for row in results]
        cur.close()
        conn.close()
        return user_ids
    except Exception as e:
        print("❌ Erreur lors de la récupération des user_id :", e)
        return []

# Fonction pour générer un numéro de téléphone aléatoire
def generate_phone_number():
    return "2126" + "".join([str(random.randint(0, 9)) for _ in range(8)])

# Fonction pour générer un timestamp aléatoire
def generate_timestamp():
    start = datetime(2025, 4, 1)
    end = datetime(2025, 4, 30)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=random_seconds)).isoformat() + "Z"

# Fonction principale pour générer un enregistrement
def generate_record(user_ids):
    record_type = random.choice(["voice", "sms", "data"])
    user_id = random.choice(user_ids)

    base_record = {
        "record_type": record_type,
        "timestamp": generate_timestamp(),
        "cell_id": random.choice(["ALHOCEIMA_23", "IMZOUREN_10", "TANGER_05"]),
        "technology": random.choice(["2G", "3G", "4G", "5G"]),
        "caller_id": "",
        "callee_id": "",
        "duration_sec": "",
        "sender_id": "",
        "receiver_id": "",
        "user_id": user_id,
        "data_volume_mb": "",
        "session_duration_sec": ""
    }

    if record_type == "voice":
        base_record["caller_id"] = user_id
        base_record["callee_id"] = generate_phone_number()
        base_record["duration_sec"] = random.randint(10, 600)
    elif record_type == "sms":
        base_record["sender_id"] = user_id
        base_record["receiver_id"] = generate_phone_number()
    else:  # data
        base_record["data_volume_mb"] = round(random.uniform(1, 500), 2)
        base_record["session_duration_sec"] = random.randint(60, 3600)

    # 1. Doublons (5%)
    if generated_records and random.random() < 0.05:
        return random.choice(generated_records)

    # 2. Champs manquants (10%)
    if random.random() < 0.1:
        fields_required = {
            "voice": ["caller_id", "callee_id", "duration_sec"],
            "sms": ["sender_id", "receiver_id"],
            "data": ["data_volume_mb", "session_duration_sec"]
        }
        to_remove = random.choice(fields_required[record_type])
        base_record[to_remove] = None

    # 3. Données corrompues (10%)
    if random.random() < 0.1:
        corrupt_fields = {
            "voice": ["duration_sec"],
            "sms": ["sender_id", "receiver_id"],
            "data": ["data_volume_mb", "session_duration_sec"]
        }
        field = random.choice(corrupt_fields[record_type])
        base_record[field] = "CORRUPTED_DATA"

    generated_records.append(base_record)
    return base_record

# Enregistrement local dans un fichier JSONL
def save_to_batch_file(record, filename="batch_records.json"):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

# Fonction principale
def main():
    user_ids = get_user_ids_from_db()
    if not user_ids:
        print("⚠️ Aucun user_id trouvé dans la table clients. Arrêt du programme.")
        return

    print("✅ Récupération des user_id clients : OK")

    app = Application(broker_address="localhost:9092", loglevel="DEBUG")

    with app.get_producer() as producer:
        while True:
            record = generate_record(user_ids)
            print("📤 Enregistrement généré :", json.dumps(record, ensure_ascii=False))

            # 1. Sauvegarde locale
            save_to_batch_file(record)

            # 2. Envoi à Kafka
            producer.produce(
                topic="records",
                key="msg",
                value=json.dumps(record),
            )

            logging.info("Produced. Sleeping...")
            time.sleep(1)

if __name__ == "__main__":
    print("🚀 Démarrage du générateur de données CDR/EDR Kafka…")
    main()
