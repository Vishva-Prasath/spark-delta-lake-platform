import json
import os
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

app = FastAPI(
    title="E-Commerce CDC Event Producer API",
    description="Generates real-time order streams for Spark Bronze ingestion.",
    version="1.0.0"
)

RAW_DATA_PATH = os.path.join(os.getcwd(), "sample_data", "orders.json")

class OrderPayload(BaseModel):
    order_id: Optional[str] = Field(default=None, example="ORD_201")
    customer_id: Optional[str] = Field(default=None, example="CUST_88")
    total_amount: float = Field(..., example=249.99)
    order_status: str = Field(..., example="COMPLETED")
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

@app.post("/api/v1/orders/generate", status_code=status.HTTP_201_CREATED)
def generate_orders_endpoint(orders: List[OrderPayload]):
    """
    Simulates streaming/CDC payload generation by appending incoming 
    REST API records directly to raw sample storage.
    """
    try:
        os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
        
        # Load existing data if file exists
        existing_data = []
        if os.path.exists(RAW_DATA_PATH):
            with open(RAW_DATA_PATH, "r") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        
        # Convert incoming Pydantic models to dicts and append
        new_records = [order.model_dump() for order in orders]
        existing_data.extend(new_records)

        with open(RAW_DATA_PATH, "w") as f:
            json.dump(existing_data, f, indent=4)

        return {
            "status": "success",
            "records_received": len(new_records),
            "total_records_stored": len(existing_data),
            "target_path": RAW_DATA_PATH
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate REST payloads: {str(e)}"
        )