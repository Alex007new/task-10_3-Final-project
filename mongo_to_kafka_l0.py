import os
import json
import re
import hmac
import hashlib
from copy import deepcopy
from bson import ObjectId
from pymongo import MongoClient
from confluent_kafka import Producer

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")
DB_NAME = os.getenv("MONGO_DB", "retail")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")

# ВАЖНО: задай в окружении (или .env) стабильную соль
PII_SALT = os.getenv("PII_SALT", "CHANGE_ME_SUPER_SECRET_SALT").encode("utf-8")

# Можно переключить алгоритм: "hmac_sha256" (рекомендуется) или "md5"
PII_ALGO = os.getenv("PII_ALGO", "hmac_sha256")

TOPICS = {
    "customers": "retail.customers.l0",
    "products": "retail.products.l0",
    "stores": "retail.stores.l0",
    "purchases": "retail.purchases.l0",
}

# Какие поля шифруем (точечные пути как в примерах документов)
PII_PATHS = {
    "customers": [
        ("email",),
        ("phone",),
    ],
    "stores": [
        ("manager", "email"),
        ("manager", "phone"),
    ],
    "purchases": [
        ("customer", "email"),
        ("customer", "phone"),
    ],
    "products": [
        # Пусто: PII нет
    ],
}

PHONE_DIGITS_RE = re.compile(r"\D+")

def normalize_email(v: str) -> str:
    return v.strip().lower()

def normalize_phone(v: str) -> str:
    # 1) только цифры
    digits = PHONE_DIGITS_RE.sub("", v)
    # 2) простая RU-нормализация: 8XXXXXXXXXX -> 7XXXXXXXXXX
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    # 3) если уже 11 и начинается на 7 — ок
    #    иначе оставим как есть (но всё равно строкой)
    return digits

def pii_hash(normalized: str) -> str:
    if PII_ALGO == "md5":
        # md5(salt + value)
        m = hashlib.md5()
        m.update(PII_SALT)
        m.update(normalized.encode("utf-8"))
        return m.hexdigest()
    # hmac_sha256(salt_key, value)
    return hmac.new(PII_SALT, normalized.encode("utf-8"), hashlib.sha256).hexdigest()

def set_in_path(obj: dict, path: tuple, new_value: str) -> None:
    cur = obj
    for key in path[:-1]:
        if not isinstance(cur, dict) or key not in cur:
            return
        cur = cur[key]
    last = path[-1]
    if isinstance(cur, dict) and last in cur and cur[last] is not None:
        cur[last] = new_value

def transform_doc_for_l0(collection_name: str, doc: dict) -> dict:
    d = deepcopy(doc)

    # ObjectId -> str (иначе json не сериализуется)
    if "_id" in d and isinstance(d["_id"], ObjectId):
        d["_id"] = str(d["_id"])

    # Нормализация + хэширование только email/phone по заданным путям
    for path in PII_PATHS.get(collection_name, []):
        # достаём текущее значение
        cur = d
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if not ok or cur is None:
            continue

        raw_val = str(cur)

        if path[-1] == "email":
            norm = normalize_email(raw_val)
            hashed = pii_hash(norm)
            set_in_path(d, path, hashed)

        if path[-1] == "phone":
            norm = normalize_phone(raw_val)
            hashed = pii_hash(norm)
            set_in_path(d, path, hashed)

    return d

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")

def main():
    mongo = MongoClient(MONGO_URI)
    db = mongo[DB_NAME]

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    for coll_name, topic in TOPICS.items():
        coll = db[coll_name]
        count = coll.count_documents({})
        print(f"Publishing '{coll_name}' ({count} docs) -> '{topic}'")

        for doc in coll.find({}):
            out_doc = transform_doc_for_l0(coll_name, doc)

            # JSONEachRow: 1 JSON = 1 message (одна строка JSON)
            payload_json = json.dumps(out_doc, ensure_ascii=False)

            producer.produce(topic, value=payload_json, on_delivery=delivery_report)

        producer.flush()
        print(f"Done: {coll_name}")

    print("All collections published.")

if __name__ == "__main__":
    main()