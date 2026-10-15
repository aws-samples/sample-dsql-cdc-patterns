#!/usr/bin/env python3
"""DSQL CDC to S3 Vector Buckets — Interactive Demo.

Provides an interactive CLI to:
  - Generate random products (INSERT) in DSQL
  - Mutate products (UPDATE/DELETE) in DSQL
  - Semantic search over product vectors via S3 Vector Buckets
  - View pipeline stats (vector count)
  - List products stored in S3 Vectors
  - Clear all products and vectors
  - Health check the pipeline
  - Cleanup all resources
"""

import json
import os
import secrets
import subprocess
import sys
import time

import boto3
import psycopg2

# ---------------------------------------------------------------------------
# Configuration (from env.sh)
# ---------------------------------------------------------------------------
CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "")
REGION = os.environ.get("REGION", "us-east-1")
VECTOR_BUCKET_NAME = os.environ.get("VECTOR_BUCKET_NAME", "dsql-product-vectors")
INDEX_NAME = os.environ.get("INDEX_NAME", "products")
DSQL_ENDPOINT = os.environ.get("DSQL_ENDPOINT", f"https://dsql.{REGION}.api.aws")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"

_rng = secrets.SystemRandom()

# ---------------------------------------------------------------------------
# Product generation data
# ---------------------------------------------------------------------------
PRODUCT_NAMES = [
    "Wireless Bluetooth Headphones", "Organic Cotton T-Shirt",
    "Stainless Steel Water Bottle", "LED Desk Lamp", "Yoga Mat Premium",
    "Portable Charger 20000mAh", "Bamboo Cutting Board",
    "Running Shoes Ultralight", "Cast Iron Skillet",
    "Noise Cancelling Earbuds", "Ceramic Coffee Mug",
    "Fitness Tracker Band", "Cotton Bed Sheet Set",
    "Electric Toothbrush", "Travel Backpack 40L",
    "Insulated Lunch Box", "Vitamin C Serum",
    "Mechanical Keyboard", "Plant-Based Protein Bar",
    "Smart Watch Pro", "Memory Foam Pillow", "Air Purifier HEPA",
    "Resistance Band Set", "Stainless Steel Thermos",
    "Organic Green Tea", "USB-C Hub Adapter", "Wool Blend Sweater",
    "Glass Food Container Set", "SPF 50 Sunscreen",
    "Portable Speaker Mini",
]

DESCRIPTIONS = [
    "Premium quality product designed for everyday use. Built with durable materials.",
    "Eco-friendly and sustainable choice for conscious consumers. Made with care.",
    "High-performance product with advanced features. Perfect for professionals.",
    "Compact and lightweight design for on-the-go convenience. Travel-ready.",
    "Ergonomic design for maximum comfort during extended use. User-tested.",
    "Professional-grade quality at an affordable price point. Best value.",
    "Innovative technology meets sleek modern design. Award-winning.",
    "All-natural ingredients sourced from organic farms. Chemical-free.",
    "Weather-resistant construction for indoor and outdoor use. Built tough.",
    "Smart features with intuitive controls for easy operation. Plug and play.",
]

CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports",
    "Books", "Toys", "Beauty", "Food",
]

BRANDS = [
    "TechPro", "EcoLife", "ActiveFit", "HomeEssentials", "PureNature",
    "UrbanStyle", "SmartGear", "VitalHealth", "CraftMaster", "GreenChoice",
]

TAG_POOL = [
    "wireless", "bluetooth", "organic", "eco-friendly", "premium",
    "lightweight", "durable", "portable", "rechargeable", "waterproof",
    "ergonomic", "compact", "professional", "natural", "smart",
    "sustainable", "high-performance", "travel", "indoor", "outdoor",
]


# ---------------------------------------------------------------------------
# DSQL connection with auto-refreshing auth tokens
# ---------------------------------------------------------------------------
class DsqlConnection:
    """Wraps psycopg2 with automatic DSQL IAM token refresh."""

    TOKEN_REFRESH = 14 * 60  # refresh before 15-min expiry

    def __init__(self, host, region, dsql_endpoint):
        self.host = host
        self.region = region
        self.dsql_endpoint = dsql_endpoint
        self._conn = None
        self._token_time = 0

    def _generate_token(self):
        cmd = [
            "aws", "dsql", "generate-db-connect-admin-auth-token",
            "--hostname", self.host,
            "--region", self.region,
        ]
        if self.dsql_endpoint:
            cmd.extend(["--endpoint-url", self.dsql_endpoint])
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()

    def get(self):
        """Return a live psycopg2 connection, refreshing the token if needed."""
        now = time.time()
        if self._conn and (now - self._token_time) < self.TOKEN_REFRESH:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self._conn
            except Exception:
                pass
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        token = self._generate_token()
        self._conn = psycopg2.connect(
            host=self.host, port=5432, dbname="postgres",
            user="admin", password=token, sslmode="require",
        )
        self._conn.autocommit = True
        self._token_time = now
        return self._conn

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------
def ensure_schema(dsql):
    """Create the products table if it doesn't exist (sql/schema.sql)."""
    conn = dsql.get()
    with open(os.path.join(SCRIPT_DIR, "sql", "schema.sql")) as f:
        lines = [l for l in f.read().splitlines() if not l.strip().startswith("--")]
    with conn.cursor() as cur:
        # DSQL allows one DDL statement per transaction.
        for stmt in "\n".join(lines).split(";"):
            if stmt.strip():
                cur.execute(stmt)
    print("  Schema ready (products table)")


# ---------------------------------------------------------------------------
# Random product helpers
# ---------------------------------------------------------------------------
def random_product():
    name = f"{_rng.choice(PRODUCT_NAMES)} {_rng.randint(100, 9999)}"
    description = _rng.choice(DESCRIPTIONS)
    category = _rng.choice(CATEGORIES)
    brand = _rng.choice(BRANDS)
    price = round(_rng.uniform(5.99, 499.99), 2)
    in_stock = _rng.random() < 0.85
    tags = ",".join(_rng.sample(TAG_POOL, _rng.randint(2, 4)))
    return {
        "name": name, "description": description, "category": category,
        "brand": brand, "price": price, "in_stock": in_stock, "tags": tags,
    }


# ---------------------------------------------------------------------------
# Generate products
# ---------------------------------------------------------------------------
def generate_products(dsql, count=10):
    """Batch-insert random products and print a table of results."""
    conn = dsql.get()
    products = [random_product() for _ in range(count)]

    placeholders = ", ".join(
        ["(%s, %s, %s, %s, %s, %s, %s)"] * len(products)
    )
    params = []
    for p in products:
        params.extend([
            p["name"], p["description"], p["category"],
            p["brand"], p["price"], p["in_stock"], p["tags"],
        ])

    sql = (
        "INSERT INTO public.products (name, description, category, brand, price, in_stock, tags) "
        f"VALUES {placeholders} RETURNING id, name, category, price"
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        results = cur.fetchall()

    print(f"\n  Inserted {len(results)} products:\n")
    print(f"  {'ID':<38} {'Name':<40} {'Category':<16} {'Price':>8}")
    print(f"  {'-'*38} {'-'*40} {'-'*16} {'-'*8}")
    for row in results:
        uid = str(row[0])
        name = row[1][:38]
        cat = row[2]
        price = f"${row[3]:.2f}"
        print(f"  {uid:<38} {name:<40} {cat:<16} {price:>8}")
    print()


# ---------------------------------------------------------------------------
# Mutate products
# ---------------------------------------------------------------------------
def mutate_products(dsql, count=5):
    """Randomly update or delete products and print the operations log."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, price, in_stock FROM public.products ORDER BY random() LIMIT %s",
            (count,),
        )
        products = cur.fetchall()

    if not products:
        print("\n  No products found. Generate some first.\n")
        return

    ops = []
    with conn.cursor() as cur:
        for uid, name, price, in_stock in products:
            action = _rng.choice(["update", "delete"])
            if action == "update":
                new_price = round(_rng.uniform(5.99, 499.99), 2)
                new_in_stock = not in_stock
                cur.execute(
                    "UPDATE public.products SET price = %s, in_stock = %s, updated_at = NOW() WHERE id = %s",
                    (new_price, new_in_stock, uid),
                )
                ops.append(("UPDATE", str(uid), name, f"price: {price}->{new_price}, in_stock: {in_stock}->{new_in_stock}"))
            else:
                cur.execute("DELETE FROM public.products WHERE id = %s", (uid,))
                ops.append(("DELETE", str(uid), name, "removed"))

    print(f"\n  Mutated {len(ops)} products:\n")
    print(f"  {'Op':<8} {'ID':<38} {'Name':<35} {'Detail'}")
    print(f"  {'-'*8} {'-'*38} {'-'*35} {'-'*40}")
    for op, uid, name, detail in ops:
        print(f"  {op:<8} {uid:<38} {name[:33]:<35} {detail}")
    print()


# ---------------------------------------------------------------------------
# Semantic search via S3 Vector Buckets
# ---------------------------------------------------------------------------
def semantic_search(s3v_client, bedrock_client):
    """Run a semantic similarity search over product vectors."""
    query = input("  Search query: ").strip()
    if not query:
        print("  Query cannot be empty.\n")
        return

    # Optional filters
    category = input("  Filter by category (blank=none): ").strip() or None
    brand = input("  Filter by brand (blank=none): ").strip() or None
    top_k_str = input("  Top K results [20]: ").strip() or "20"
    try:
        top_k = min(max(int(top_k_str), 1), 100)
    except ValueError:
        top_k = 20

    # Generate query embedding via Bedrock
    resp = bedrock_client.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"inputText": query, "dimensions": 1024, "normalize": True}),
    )
    query_embedding = json.loads(resp["body"].read())["embedding"]

    # Build metadata filter
    filter_clauses = []
    if category:
        filter_clauses.append({"category": {"$eq": category}})
    if brand:
        filter_clauses.append({"brand": {"$eq": brand}})

    metadata_filter = None
    if len(filter_clauses) > 1:
        metadata_filter = {"$and": filter_clauses}
    elif len(filter_clauses) == 1:
        metadata_filter = filter_clauses[0]

    kwargs = {
        "vectorBucketName": VECTOR_BUCKET_NAME,
        "indexName": INDEX_NAME,
        "queryVector": {"float32": query_embedding},
        "topK": top_k,
        "returnMetadata": True,
        "returnDistance": True,
    }
    if metadata_filter:
        kwargs["filter"] = metadata_filter

    result = s3v_client.query_vectors(**kwargs)
    hits = result.get("vectors", [])

    print(f"\n  Query: \"{query}\"")
    print(f"  Results: {len(hits)}\n")

    if not hits:
        print("  No matching vectors found. Generate products and wait for CDC sync.\n")
        return

    print(f"  {'#':<4} {'Distance':<10} {'Name':<40} {'Category':<16} {'Brand':<14} {'Price':>8}")
    print(f"  {'-'*4} {'-'*10} {'-'*40} {'-'*16} {'-'*14} {'-'*8}")
    for i, v in enumerate(hits, 1):
        meta = v.get("metadata", {})
        dist = f"{v.get('distance', 0):.4f}"
        name = str(meta.get("name", "?"))[:38]
        cat = str(meta.get("category", "?"))
        brand_name = str(meta.get("brand", "?"))
        price_val = meta.get("price", 0)
        try:
            price_str = f"${float(price_val):.2f}"
        except (ValueError, TypeError):
            price_str = "?"
        print(f"  {i:<4} {dist:<10} {name:<40} {cat:<16} {brand_name:<14} {price_str:>8}")
    print()


# ---------------------------------------------------------------------------
# Stats — vector count
# ---------------------------------------------------------------------------
def show_stats(s3v_client):
    """Print pipeline stats: vector bucket, index, and count."""
    count = 0
    next_token = None
    while True:
        kwargs = {
            "vectorBucketName": VECTOR_BUCKET_NAME,
            "indexName": INDEX_NAME,
            "maxResults": 1000,
        }
        if next_token:
            kwargs["nextToken"] = next_token
        result = s3v_client.list_vectors(**kwargs)
        count += len(result.get("vectors", []))
        next_token = result.get("nextToken")
        if not next_token:
            break

    print(f"\n  Pipeline Stats")
    print(f"  {'='*40}")
    print(f"  Vector Bucket:  {VECTOR_BUCKET_NAME}")
    print(f"  Index Name:     {INDEX_NAME}")
    print(f"  Vector Count:   {count}")
    print()


# ---------------------------------------------------------------------------
# List products from S3 Vectors
# ---------------------------------------------------------------------------
def list_products(s3v_client):
    """List products stored in S3 Vector index with metadata."""
    result = s3v_client.list_vectors(
        vectorBucketName=VECTOR_BUCKET_NAME,
        indexName=INDEX_NAME,
        maxResults=100,
    )
    keys = [v["key"] for v in result.get("vectors", [])]
    if not keys:
        print("\n  No products found in vector index. Generate products and wait for CDC sync.\n")
        return

    # Fetch metadata in batches of 100
    products = []
    for i in range(0, len(keys), 100):
        batch = keys[i:i+100]
        get_result = s3v_client.get_vectors(
            vectorBucketName=VECTOR_BUCKET_NAME,
            indexName=INDEX_NAME,
            keys=batch,
            returnMetadata=True,
        )
        for v in get_result.get("vectors", []):
            products.append({"id": v["key"], **(v.get("metadata") or {})})

    print(f"\n  Products in vector index ({len(products)} shown):\n")
    print(f"  {'ID':<38} {'Name':<40} {'Category':<16} {'Brand':<14} {'Price':>8}")
    print(f"  {'-'*38} {'-'*40} {'-'*16} {'-'*14} {'-'*8}")
    for p in products:
        uid = str(p.get("id", "?"))
        name = str(p.get("name", "?"))[:38]
        cat = str(p.get("category", "?"))
        brand_name = str(p.get("brand", "?"))
        price_val = p.get("price", 0)
        try:
            price_str = f"${float(price_val):.2f}"
        except (ValueError, TypeError):
            price_str = "?"
        print(f"  {uid:<38} {name:<40} {cat:<16} {brand_name:<14} {price_str:>8}")
    print()


# ---------------------------------------------------------------------------
# Clear all products and vectors
# ---------------------------------------------------------------------------
def clear_all(dsql, s3v_client):
    """Delete all products from DSQL and all vectors from S3 Vectors."""
    confirm = input('  Type "clear" to confirm: ').strip()
    if confirm != "clear":
        print("  Cancelled.\n")
        return

    # Clear DSQL
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.products")
    print("  Deleted all products from DSQL.")

    # Clear vectors
    total_deleted = 0
    next_token = None
    while True:
        kwargs = {
            "vectorBucketName": VECTOR_BUCKET_NAME,
            "indexName": INDEX_NAME,
            "maxResults": 500,
        }
        if next_token:
            kwargs["nextToken"] = next_token
        result = s3v_client.list_vectors(**kwargs)
        keys = [v["key"] for v in result.get("vectors", [])]
        if keys:
            s3v_client.delete_vectors(
                vectorBucketName=VECTOR_BUCKET_NAME,
                indexName=INDEX_NAME,
                keys=keys,
            )
            total_deleted += len(keys)
        next_token = result.get("nextToken")
        if not next_token:
            break

    print(f"  Deleted {total_deleted} vectors from S3 Vectors.")
    print()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def health_check(dsql, s3v_client):
    """Print pipeline health status."""
    print(f"\n  Health Check")
    print(f"  {'='*40}")
    print(f"  Cluster Host:       {CLUSTER_HOST}")
    print(f"  Cluster ID:         {CLUSTER_ID}")
    print(f"  Region:             {REGION}")
    print(f"  Vector Bucket:      {VECTOR_BUCKET_NAME}")
    print(f"  Index Name:         {INDEX_NAME}")

    try:
        conn = dsql.get()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("  DSQL:               OK")
    except Exception as e:
        print(f"  DSQL:               FAILED ({e})")

    try:
        s3v_client.list_vectors(
            vectorBucketName=VECTOR_BUCKET_NAME,
            indexName=INDEX_NAME,
            maxResults=1,
        )
        print("  S3 Vectors:         OK")
    except Exception as e:
        print(f"  S3 Vectors:         FAILED ({e})")
    print()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def run_cleanup():
    """Confirm and execute destroy.sh."""
    print("\n  This will destroy ALL resources (DSQL cluster, CDC stream, CloudFormation stack).")
    confirm = input('  Type "destroy" to confirm: ').strip()
    if confirm != "destroy":
        print("  Cancelled.\n")
        return
    destroy_script = os.path.join(SCRIPT_DIR, "destroy.sh")
    if not os.path.exists(destroy_script):
        print(f"  ERROR: {destroy_script} not found.\n")
        return
    os.execvp("bash", ["bash", destroy_script])


# ---------------------------------------------------------------------------
# Menu
# ---------------------------------------------------------------------------
def show_menu():
    print()
    print("+" + "=" * 52 + "+")
    print("|   DSQL CDC to S3 Vectors --- Interactive Demo      |")
    print("+" + "=" * 52 + "+")
    print("|  1) Generate random products (INSERT)               |")
    print("|  2) Mutate products (UPDATE/DELETE)                  |")
    print("|  3) Semantic search (query S3 Vectors)              |")
    print("|  4) Pipeline stats (vector count)                   |")
    print("|  5) List products (from S3 Vectors)                 |")
    print("|  6) Clear all products + vectors                    |")
    print("|  7) Health check                                    |")
    print("|  8) Cleanup (destroy everything)                    |")
    print("|  9) Exit                                            |")
    print("+" + "=" * 52 + "+")


def main():
    if not CLUSTER_HOST:
        print("ERROR: CLUSTER_HOST not set. Source env.sh first.")
        sys.exit(1)

    dsql = DsqlConnection(CLUSTER_HOST, REGION, DSQL_ENDPOINT)
    s3v_client = boto3.client("s3vectors", region_name=REGION)
    bedrock_client = boto3.client("bedrock-runtime", region_name=REGION)

    print("\n  Connecting to DSQL...")
    try:
        ensure_schema(dsql)
    except Exception as e:
        print(f"  WARNING: Could not create schema: {e}")
        print("  Continuing anyway --- the table may already exist.\n")

    while True:
        show_menu()
        try:
            choice = input("\n  Choose [1-9]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if choice == "1":
            try:
                n = int(input("  How many products? [10]: ").strip() or "10")
            except ValueError:
                n = 10
            try:
                generate_products(dsql, min(max(n, 1), 100))
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "2":
            try:
                n = int(input("  How many to mutate? [5]: ").strip() or "5")
            except ValueError:
                n = 5
            try:
                mutate_products(dsql, min(max(n, 1), 50))
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "3":
            try:
                semantic_search(s3v_client, bedrock_client)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "4":
            try:
                show_stats(s3v_client)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "5":
            try:
                list_products(s3v_client)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "6":
            try:
                clear_all(dsql, s3v_client)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "7":
            health_check(dsql, s3v_client)

        elif choice == "8":
            run_cleanup()

        elif choice == "9":
            print("\n  Goodbye!")
            break

        else:
            print("\n  Invalid choice.\n")

    dsql.close()


if __name__ == "__main__":
    main()
