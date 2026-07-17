from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import requests
import snowflake.connector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://dummyjson.com"
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DB")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "BRONZE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
ROLE = os.getenv("SNOWFLAKE_ROLE")

USERS_URL = f"{BASE_URL}/users?limit=0"
PRODUCTS_URL = f"{BASE_URL}/products?limit=0"
CARTS_URL = f"{BASE_URL}/carts?limit=0"

ORDER_DATE_BASE = date(2026, 7, 1)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "ecommerce-snowflake-pipeline/1.0"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_json(session: requests.Session, url: str) -> dict[str, Any]:
    response = session.get(url, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Resposta inesperada em {url}")

    return payload


def transform_customers(payload: dict[str, Any]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []

    for user in payload.get("users", []):
        address = user.get("address") or {}
        full_name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()

        rows.append(
            (
                int(user["id"]),
                full_name,
                user.get("email"),
                address.get("state"),
                address.get("city"),
            )
        )

    return rows


def build_category_ids(products: list[dict[str, Any]]) -> dict[str, int]:
    categories = sorted(
        {
            str(product.get("category", "unknown"))
            for product in products
        }
    )
    return {category: index for index, category in enumerate(categories, start=1)}


def transform_products(payload: dict[str, Any]) -> list[tuple[Any, ...]]:
    products = payload.get("products", [])
    category_ids = build_category_ids(products)

    rows: list[tuple[Any, ...]] = []

    for product in products:
        category = str(product.get("category", "unknown"))

        rows.append(
            (
                int(product["id"]),
                product.get("title"),
                category_ids[category],
                Decimal(str(product.get("price", 0))),
            )
        )

    return rows


def transform_orders(payload: dict[str, Any]) -> list[tuple[Any, ...]]:

    rows: list[tuple[Any, ...]] = []

    for cart_index, cart in enumerate(payload.get("carts", [])):
        order_date = ORDER_DATE_BASE + timedelta(days=cart_index)
        customer_id = int(cart["userId"])
        cart_id = int(cart["id"])

        for item_index, product in enumerate(cart.get("products", []), start=1):
            order_id = cart_id * 1000 + item_index

            rows.append(
                (
                    order_id,
                    customer_id,
                    int(product["id"]),
                    int(product.get("quantity", 0)),
                    order_date,
                )
            )

    return rows


def connect_snowflake():
    connection_args: dict[str, Any] = {
        "account": get_required_env("SNOWFLAKE_ACCOUNT"),
        "user": get_required_env("SNOWFLAKE_USER"),
        "password": get_required_env("SNOWFLAKE_PASSWORD"),
        "database": DATABASE,
        "schema": SCHEMA,
    }

    if WAREHOUSE:
        connection_args["warehouse"] = WAREHOUSE

    if ROLE:
        connection_args["role"] = ROLE

    return snowflake.connector.connect(**connection_args)


def ensure_tables(cursor) -> None:
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}")
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS CUSTOMERS (
            customer_id INT,
            full_name STRING,
            email STRING,
            state STRING,
            city STRING
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS PRODUCTS (
            product_id INT,
            product_name STRING,
            category_id INT,
            price NUMBER(10,2)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ORDERS (
            order_id INT,
            customer_id INT,
            product_id INT,
            quantity INT,
            order_date DATE
        )
        """
    )


def replace_table_data(cursor, table_name: str, insert_sql: str, rows: list[tuple[Any, ...]]) -> None:
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    if rows:
        cursor.executemany(insert_sql, rows)

    print(f"{table_name}: {len(rows)} registros carregados.")


def main() -> int:
    try:
        session = build_http_session()

        users_payload = fetch_json(session, USERS_URL)
        products_payload = fetch_json(session, PRODUCTS_URL)
        carts_payload = fetch_json(session, CARTS_URL)

        customers = transform_customers(users_payload)
        products = transform_products(products_payload)
        orders = transform_orders(carts_payload)

        with connect_snowflake() as connection:
            with connection.cursor() as cursor:
                ensure_tables(cursor)

                replace_table_data(
                    cursor,
                    "CUSTOMERS",
                    """
                    INSERT INTO CUSTOMERS
                        (customer_id, full_name, email, state, city)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    customers,
                )

                replace_table_data(
                    cursor,
                    "PRODUCTS",
                    """
                    INSERT INTO PRODUCTS
                        (product_id, product_name, category_id, price)
                    VALUES (%s, %s, %s, %s)
                    """,
                    products,
                )

                replace_table_data(
                    cursor,
                    "ORDERS",
                    """
                    INSERT INTO ORDERS
                        (order_id, customer_id, product_id, quantity, order_date)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    orders,
                )

            connection.commit()

        print("Ingestão concluída com sucesso.")
        return 0

    except requests.RequestException as exc:
        print(f"Erro ao consultar a API: {exc}", file=sys.stderr)
        return 1
    except snowflake.connector.Error as exc:
        print(f"Erro no Snowflake: {exc}", file=sys.stderr)
        return 1
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        print(f"Erro de configuração ou transformação: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())