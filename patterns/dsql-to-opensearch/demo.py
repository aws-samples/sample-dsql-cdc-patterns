#!/usr/bin/env python3
"""DSQL CDC to OpenSearch Serverless — Interactive Demo.

Provides an interactive CLI to:
  - Generate random products (INSERT) in DSQL
  - Mutate products (UPDATE/DELETE) in DSQL
  - Search products via OpenSearch (through API Gateway)
  - Autocomplete product names
  - View faceted aggregations
  - View index statistics
  - Clear all products from DSQL
  - Reset OpenSearch index
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
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session as BotoSession
from urllib.parse import urlencode
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Configuration (from env.sh)
# ---------------------------------------------------------------------------
CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "")
REGION = os.environ.get("REGION", "us-east-1")
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL", "").rstrip("/")
DSQL_ENDPOINT = os.environ.get("DSQL_ENDPOINT", f"https://dsql.{REGION}.api.aws")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

_rng = secrets.SystemRandom()

# ---------------------------------------------------------------------------
# Product generation data
# ---------------------------------------------------------------------------
PRODUCT_NAMES = [
    "Wireless Bluetooth Headphones", "Organic Cotton T-Shirt",
    "Stainless Steel Water Bottle", "LED Desk Lamp",
    "Yoga Mat Premium", "Portable Charger 20000mAh",
    "Bamboo Cutting Board", "Running Shoes Ultralight",
    "Cast Iron Skillet", "Noise Cancelling Earbuds",
    "Ceramic Coffee Mug", "Fitness Tracker Band",
    "Cotton Bed Sheet Set", "Electric Toothbrush",
    "Travel Backpack 40L", "Insulated Lunch Box",
    "Vitamin C Serum", "Mechanical Keyboard",
    "Plant-Based Protein Bar", "Smart Watch Pro",
    "Memory Foam Pillow", "Air Purifier HEPA",
    "Resistance Band Set", "Stainless Steel Thermos",
    "Organic Green Tea", "USB-C Hub Adapter",
    "Wool Blend Sweater", "Glass Food Container Set",
    "SPF 50 Sunscreen", "Portable Speaker Mini",
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
# API Gateway SigV4 client
# ---------------------------------------------------------------------------
def api_gateway_request(method, path, query_params=None):
    """Make a SigV4-signed request to the API Gateway."""
    url = f"{API_GATEWAY_URL}{path}"
    if query_params:
        qs = urlencode({k: v for k, v in query_params.items() if v})
        if qs:
            url = f"{url}?{qs}"

    sess = BotoSession()
    creds = sess.get_credentials().get_frozen_credentials()
    req = AWSRequest(method=method, url=url, headers={"Content-Type": "application/json"})
    SigV4Auth(creds, "execute-api", REGION).add_auth(req)

    http_req = urllib.request.Request(
        url, headers=dict(req.headers), method=method
    )
    try:
        with urllib.request.urlopen(http_req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f"API Gateway {e.code}: {body}")


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------
def ensure_schema(dsql):
    """Create the products table if it doesn't exist."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.products (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name        VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                category    VARCHAR(100) NOT NULL,
                brand       VARCHAR(100) NOT NULL,
                price       DECIMAL(10,2) NOT NULL,
                in_stock    BOOLEAN DEFAULT true,
                tags        TEXT DEFAULT '',
                created_at  TIMESTAMPTZ DEFAULT now(),
                updated_at  TIMESTAMPTZ DEFAULT now()
            )
        """)
    print("  Schema ready (products table)")


# ---------------------------------------------------------------------------
# Generate products
# ---------------------------------------------------------------------------
def generate_products(dsql, count=10):
    """Batch-insert random products and print a table of results."""
    conn = dsql.get()
    rows = []
    for _ in range(count):
        name = f"{secrets.choice(PRODUCT_NAMES)} {_rng.randint(100, 9999)}"
        desc = secrets.choice(DESCRIPTIONS)
        cat = secrets.choice(CATEGORIES)
        brand = secrets.choice(BRANDS)
        price = round(_rng.uniform(5.99, 499.99), 2)
        in_stock = _rng.random() < 0.85
        tags = ",".join(_rng.sample(TAG_POOL, _rng.randint(2, 4)))
        rows.append((name, desc, cat, brand, price, in_stock, tags))

    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(rows))
    params = [val for row in rows for val in row]
    sql = (
        f"INSERT INTO products (name, description, category, brand, price, in_stock, tags) "
        f"VALUES {placeholders} "
        f"RETURNING id, name, category, price"
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
            "SELECT id, name, price, in_stock FROM products ORDER BY random() LIMIT %s",
            (count,),
        )
        products = cur.fetchall()

    if not products:
        print("\n  No products found. Generate some first.\n")
        return

    ops = []
    with conn.cursor() as cur:
        for uid, name, price, in_stock in products:
            action = secrets.choice(["update", "delete"])
            if action == "update":
                new_price = round(_rng.uniform(5.99, 499.99), 2)
                new_in_stock = not in_stock
                cur.execute(
                    "UPDATE products SET price = %s, in_stock = %s, updated_at = NOW() WHERE id = %s",
                    (new_price, new_in_stock, uid),
                )
                ops.append(("UPDATE", str(uid), name, f"${float(price):.2f} -> ${new_price:.2f}, stock={new_in_stock}"))
            else:
                cur.execute("DELETE FROM products WHERE id = %s", (uid,))
                ops.append(("DELETE", str(uid), name, "removed"))

    print(f"\n  Mutated {len(ops)} products:\n")
    print(f"  {'Op':<8} {'ID':<38} {'Name':<32} {'Detail'}")
    print(f"  {'-'*8} {'-'*38} {'-'*32} {'-'*40}")
    for op, uid, name, detail in ops:
        print(f"  {op:<8} {uid:<38} {name[:30]:<32} {detail}")
    print()


# ---------------------------------------------------------------------------
# Search products (via API Gateway -> OpenSearch)
# ---------------------------------------------------------------------------
def search_products():
    """Full-text search with optional filters via the API Gateway."""
    q = input("  Search query []: ").strip()
    cat = input("  Category filter []: ").strip()
    brand = input("  Brand filter []: ").strip()

    params = {}
    if q:
        params["q"] = q
    if cat:
        params["category"] = cat
    if brand:
        params["brand"] = brand

    result = api_gateway_request("GET", "/search", params)
    hits = result.get("hits", [])
    total = result.get("total", 0)
    took = result.get("took", "?")

    print(f"\n  Found {total} results ({took}ms):\n")
    if not hits:
        print("  No results.\n")
        return

    print(f"  {'Score':<8} {'Name':<40} {'Category':<16} {'Brand':<16} {'Price':>8} {'Stock'}")
    print(f"  {'-'*8} {'-'*40} {'-'*16} {'-'*16} {'-'*8} {'-'*6}")
    for h in hits[:20]:
        score = f"{h.get('_score', 0):.2f}"
        name = str(h.get("name", ""))[:38]
        cat = str(h.get("category", ""))
        brand = str(h.get("brand", ""))
        price = f"${float(h.get('price', 0)):.2f}"
        stock = "Yes" if h.get("in_stock") else "No"
        print(f"  {score:<8} {name:<40} {cat:<16} {brand:<16} {price:>8} {stock}")
    print()


# ---------------------------------------------------------------------------
# Autocomplete
# ---------------------------------------------------------------------------
def autocomplete_products():
    """Prefix-based autocomplete via the API Gateway."""
    q = input("  Type a prefix: ").strip()
    if not q:
        print("  No input.\n")
        return

    result = api_gateway_request("GET", "/autocomplete", {"q": q})
    sugs = result.get("suggestions", [])

    print(f"\n  Autocomplete suggestions ({len(sugs)}):\n")
    if not sugs:
        print("  No suggestions.\n")
        return

    for s in sugs:
        print(f"    {s.get('name', '')}  ({s.get('category', '')})")
    print()


# ---------------------------------------------------------------------------
# Facets
# ---------------------------------------------------------------------------
def show_facets():
    """Show faceted aggregation counts from OpenSearch."""
    result = api_gateway_request("GET", "/facets")

    print("\n  Categories:")
    for b in result.get("categories", []):
        print(f"    {b.get('key', '?'):<24} {b.get('doc_count', 0)}")

    print("\n  Brands:")
    for b in result.get("brands", []):
        print(f"    {b.get('key', '?'):<24} {b.get('doc_count', 0)}")

    print("\n  Price Ranges:")
    for b in result.get("price_ranges", []):
        print(f"    {b.get('key', '?'):<24} {b.get('doc_count', 0)}")

    print("\n  In Stock:")
    for b in result.get("in_stock_count", []):
        label = "Yes" if b.get("key_as_string", str(b.get("key"))) == "true" else "No"
        print(f"    {label:<24} {b.get('doc_count', 0)}")
    print()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def show_stats():
    """Show OpenSearch index statistics."""
    result = api_gateway_request("GET", "/stats")
    doc_count = result.get("doc_count", 0)
    print(f"\n  OpenSearch Index Stats")
    print(f"  {'─' * 30}")
    print(f"  Document count:  {doc_count}")
    print()


# ---------------------------------------------------------------------------
# Clear all products
# ---------------------------------------------------------------------------
def clear_all_products(dsql):
    """Delete all products from DSQL."""
    confirm = input("  Delete ALL products from DSQL? Type 'yes' to confirm: ").strip()
    if confirm != "yes":
        print("  Cancelled.\n")
        return
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM products")
    print("  All products cleared from DSQL.\n")


# ---------------------------------------------------------------------------
# Reset OpenSearch index
# ---------------------------------------------------------------------------
def reset_opensearch():
    """Delete and recreate the OpenSearch index."""
    confirm = input("  Reset OpenSearch index? Type 'yes' to confirm: ").strip()
    if confirm != "yes":
        print("  Cancelled.\n")
        return
    result = api_gateway_request("DELETE", "/index")
    print(f"  {result.get('message', 'Done')}\n")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def health_check(dsql):
    """Print pipeline health status."""
    print(f"\n  Health Check")
    print(f"  {'─' * 40}")
    print(f"  Cluster Host:     {CLUSTER_HOST}")
    print(f"  Cluster ID:       {CLUSTER_ID}")
    print(f"  Region:           {REGION}")
    print(f"  API Gateway URL:  {API_GATEWAY_URL}")

    try:
        conn = dsql.get()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("  DSQL:             OK")
    except Exception as e:
        print(f"  DSQL:             FAILED ({e})")

    try:
        result = api_gateway_request("GET", "/stats")
        doc_count = result.get("doc_count", 0)
        print(f"  OpenSearch:       OK ({doc_count} docs)")
    except Exception as e:
        print(f"  OpenSearch:       FAILED ({e})")
    print()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def run_cleanup():
    """Confirm and execute destroy.sh."""
    print("\n  This will destroy ALL resources.")
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
    print("╔═══════════════════════════════════════════════════╗")
    print("║   DSQL CDC to OpenSearch — Interactive Demo       ║")
    print("╠═══════════════════════════════════════════════════╣")
    print("║   1) Generate random products (INSERT)            ║")
    print("║   2) Mutate products (UPDATE/DELETE)              ║")
    print("║   3) Search products (full-text via OpenSearch)   ║")
    print("║   4) Autocomplete product names                   ║")
    print("║   5) View facets (categories, brands, prices)     ║")
    print("║   6) View index stats                             ║")
    print("║   7) Clear all products (DSQL)                    ║")
    print("║   8) Reset OpenSearch index                       ║")
    print("║   9) Health check                                 ║")
    print("║  10) Cleanup (destroy everything)                 ║")
    print("║  11) Exit                                         ║")
    print("╚═══════════════════════════════════════════════════╝")


def main():
    if not CLUSTER_HOST:
        print("ERROR: CLUSTER_HOST not set. Source env.sh first.")
        sys.exit(1)
    if not API_GATEWAY_URL:
        print("ERROR: API_GATEWAY_URL not set. Source env.sh first.")
        sys.exit(1)

    dsql = DsqlConnection(CLUSTER_HOST, REGION, DSQL_ENDPOINT)

    print("\n  Connecting to DSQL...")
    try:
        ensure_schema(dsql)
    except Exception as e:
        print(f"  WARNING: Could not create schema: {e}")
        print("  Continuing anyway — the table may already exist.\n")

    while True:
        show_menu()
        try:
            choice = input("\n  Choose [1-11]: ").strip()
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
                search_products()
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "4":
            try:
                autocomplete_products()
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "5":
            try:
                show_facets()
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "6":
            try:
                show_stats()
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "7":
            try:
                clear_all_products(dsql)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "8":
            try:
                reset_opensearch()
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "9":
            health_check(dsql)

        elif choice == "10":
            run_cleanup()

        elif choice == "11":
            print("\n  Goodbye!")
            break

        else:
            print("\n  Invalid choice.\n")

    dsql.close()


if __name__ == "__main__":
    main()
