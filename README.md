# Пишем свою BI-витрину при помощи DuckDB, Python и Dash

Примерная схема БД:

```mermaid
erDiagram
    USERS {
        int id
        string name
        int user_type_id
    }
    USER_TYPES {
        int id
        string type_name
    }
    ORDERS {
        int id
        date order_date
        int user_id
        int delivery_id
        int payment_type_id
    }
    ORDER_DETAILS {
        int id
        int order_id
        int product_id
        int quantity
        float item_price
        float item_discount
        float item_total
    }
    DELIVERIES {
        int id
        date ship_date
        string address
        float latitude
        float longitude
    }
    PAYMENT_TYPES {
        int id
        string payment_type_name
    }
    PRODUCTS {
        int id
        string product_name
        int product_category_id
    }
    PRODUCT_CATEGORIES {
        int id
        string category_name
    }

    USERS ||--o{ ORDERS: "places"
    USER_TYPES ||--o{ USERS: "is"
    ORDERS ||--o{ ORDER_DETAILS: "contains"
    ORDER_DETAILS }o--|| PRODUCTS: "for"
    PRODUCTS }o--|| PRODUCT_CATEGORIES: "in"
    ORDERS }o--|| DELIVERIES: "delivered_by"
    ORDERS }o--|| PAYMENT_TYPES: "paid_with"
```

Наша витрина:

```mermaid
erDiagram
    DATA_MART {
        float latitude
        float longitude
        string type_user
        string category_name
        date ship_date
        float price_of_order
        string type_of_payment
    }
```

**Типовой SQL для витрины** (для примера):

```sql
SELECT
  d.latitude,
  d.longitude,
  ut.type_user,
  pc.category_name,
  o.ship_date,
  SUM(od.item_total) as price_of_order,
  pt.type_of_payment
FROM ORDERS o
JOIN USERS u ON o.user_id = u.id
JOIN USER_TYPES ut ON u.user_type_id = ut.id
JOIN ORDER_DETAILS od ON o.id = od.order_id
JOIN PRODUCTS p ON od.product_id = p.id
JOIN PRODUCT_CATEGORIES pc ON p.product_category_id = pc.id
JOIN DELIVERIES d ON o.delivery_id = d.id
JOIN PAYMENT_TYPES pt ON o.payment_type_id = pt.id
GROUP BY
  d.latitude, d.longitude, ut.type_user, pc.category_name, o.ship_date, pt.type_of_payment
```

## Создание виртуального окружения

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

### Добавление новых зависимостей в окружение

```bash
poetry lock && \
poetry install
```

## Запуск приложения

Активируем виртуальное окружение:

```bash
source venv/bin/activate
```

Запускаем приложение:

```bash
python app.py
```