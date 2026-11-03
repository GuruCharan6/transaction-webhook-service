from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class WebhookIn(BaseModel):
    transaction_id: str = Field(min_length=1)
    source_account: str = Field(min_length=1)
    destination_account: str = Field(min_length=1)
    amount: Decimal = Field(lt=Decimal("10000000000000000"))
    currency: str = Field(min_length=3, max_length=3)

    @field_validator("amount")
    @classmethod
    def amount_positive_two_decimals(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("amount must be greater than 0")
        if -v.as_tuple().exponent > 2:
            raise ValueError("amount must have at most 2 decimal places")
        return v

    @field_validator("currency")
    @classmethod
    def currency_alpha_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("currency must be a 3-letter code")
        return v.upper()


class WebhookAccepted(BaseModel):
    status: str = "accepted"
    transaction_id: str


class HealthOut(BaseModel):
    status: str = "HEALTHY"
    current_time: datetime


class TransactionOut(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: str
    created_at: datetime
    processed_at: datetime | None
