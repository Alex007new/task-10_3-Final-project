# mongo_loader.py
# Load NDJSON (JSONEachRow) files into MongoDB collections.
#
# Usage:
#   python mongo_loader.py --mongo-uri mongodb://localhost:27018 --db retail --input-dir data/jsonl
#
# Notes:
# - Expects files: products.jsonl, customers.jsonl, stores.jsonl, purchases.jsonl (each line is a JSON object).
# - Uses bulk operations with configurable batch size.
# - Creates helpful indexes (unique ids) unless --no-index is used.

import argparse
import json
import os
from typing import Dict, Iterable, List, Tuple

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError


DEFAULT_FILES = {
    "products": "products.jsonl",
    "customers": "customers.jsonl",
    "stores": "stores.jsonl",
    "purchases": "purchases.jsonl",
}


def iter_jsonl(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {i} in {path}: {e}") from e


def batched(iterable: Iterable[dict], batch_size: int) -> Iterable[List[dict]]:
    batch: List[dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def upsert_many(collection, docs: List[dict], id_field: str, ordered: bool = False) -> Tuple[int, int]:
    """Upsert by id_field (replace full document). Returns (affected_total, modified_count)."""
    from pymongo import ReplaceOne

    ops = []
    for d in docs:
        if id_field not in d:
            raise KeyError(f"Document missing id field '{id_field}' in collection '{collection.name}'")
        ops.append(ReplaceOne({id_field: d[id_field]}, d, upsert=True))

    res = collection.bulk_write(ops, ordered=ordered)
    total = (res.upserted_count or 0) + (res.matched_count or 0)
    modified = res.modified_count or 0
    return total, modified


def ensure_indexes(db, no_index: bool = False) -> None:
    if no_index:
        return

    db["products"].create_index([("id", ASCENDING)], unique=True, name="ux_products_id")
    db["customers"].create_index([("customer_id", ASCENDING)], unique=True, name="ux_customers_customer_id")
    db["stores"].create_index([("store_id", ASCENDING)], unique=True, name="ux_stores_store_id")
    db["purchases"].create_index([("purchase_id", ASCENDING)], unique=True, name="ux_purchases_purchase_id")

    db["purchases"].create_index([("purchase_datetime", ASCENDING)], name="ix_purchases_purchase_datetime")
    db["purchases"].create_index([("store.store_id", ASCENDING)], name="ix_purchases_store_id")
    db["purchases"].create_index([("customer.customer_id", ASCENDING)], name="ix_purchases_customer_id")


def load_collection(db, name: str, jsonl_path: str, batch_size: int, mode: str, ordered: bool) -> Dict[str, int]:
    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(f"File not found: {jsonl_path}")

    coll = db[name]
    id_field_map = {
        "products": "id",
        "customers": "customer_id",
        "stores": "store_id",
        "purchases": "purchase_id",
    }
    id_field = id_field_map[name]

    affected = 0
    modified = 0

    for batch in batched(iter_jsonl(jsonl_path), batch_size=batch_size):
        if mode == "insert":
            try:
                res = coll.insert_many(batch, ordered=ordered)
                affected += len(res.inserted_ids)
            except BulkWriteError as e:
                raise RuntimeError(
                    f"Bulk insert failed for '{name}'. If rerunning, use --mode upsert. "
                    f"First errors: {e.details.get('writeErrors', [])[:3]}"
                ) from e
        elif mode == "upsert":
            total, mod = upsert_many(coll, batch, id_field=id_field, ordered=ordered)
            affected += total
            modified += mod
        else:
            raise ValueError(f"Unknown mode: {mode}")

    return {"affected": affected, "modified": modified}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load JSONL (JSONEachRow) files into MongoDB.")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27018"),
                   help="MongoDB URI (default: mongodb://localhost:27018).")
    p.add_argument("--db", default=os.getenv("MONGO_DB", "retail"),
                   help="Database name (default: retail).")
    p.add_argument("--input-dir", default="data/jsonl",
                   help="Directory with JSONL files (default: data/jsonl).")
    p.add_argument("--batch-size", type=int, default=1000, help="Bulk batch size (default: 1000).")
    p.add_argument("--mode", choices=["insert", "upsert"], default="upsert",
                   help="insert=fail on duplicates, upsert=safe rerun (default: upsert).")
    p.add_argument("--drop", action="store_true", help="Drop collections before loading (destructive).")
    p.add_argument("--no-index", action="store_true", help="Do not create indexes.")
    p.add_argument("--ordered", action="store_true",
                   help="Use ordered bulk ops (slower; stops on first error). Default unordered.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")

    db = client[args.db]

    if args.drop:
        for c in DEFAULT_FILES.keys():
            db[c].drop()

    ensure_indexes(db, no_index=args.no_index)

    results = {}
    for coll_name, filename in DEFAULT_FILES.items():
        path = os.path.join(args.input_dir, filename)
        results[coll_name] = load_collection(
            db=db,
            name=coll_name,
            jsonl_path=path,
            batch_size=args.batch_size,
            mode=args.mode,
            ordered=args.ordered,
        )

    print("✅ Load complete")
    for k, v in results.items():
        print(f" - {k}: affected={v['affected']}, modified={v['modified']}")
    print(f"DB: {args.db} @ {args.mongo_uri}")


if __name__ == "__main__":
    main()
