import json
import os
from datetime import datetime

def generate_sample_orders():
    os.makedirs("sample_data", exist_ok=True)
    records = [
        {"order_id": "ORD_101", "customer_id": "CUST_1", "total_amount": 150.50, "order_status": "COMPLETED", "updated_at": "2026-09-01T10:00:00Z"},
        {"order_id": "ORD_102", "customer_id": "CUST_2", "total_amount": -10.00, "order_status": "COMPLETED", "updated_at": "2026-09-01T10:05:00Z"}, # Bad Record
        {"order_id": "ORD_103", "customer_id": "CUST_3", "total_amount": 89.99, "order_status": "PENDING", "updated_at": "2026-09-01T10:10:00Z"},
        {"order_id": "ORD_101", "customer_id": "CUST_1", "total_amount": 150.50, "order_status": "REFUNDED", "updated_at": "2026-09-01T11:00:00Z"}  # CDC Update
    ]
    with open("sample_data/orders.json", "w") as f:
        json.dump(records, f)

if __name__ == "__main__":
    generate_sample_orders()