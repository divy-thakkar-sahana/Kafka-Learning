from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from pydantic import BaseModel, Field
from app.kafka_poc_3.database import Base


# --- SQLAlchemy Database Models ---
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False, index=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False, index=True)
    description = Column(Text, nullable=True)
    category = Column(String(50), nullable=False, index=True)
    price = Column(Float, nullable=False)
    stock_quantity = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


# --- Pydantic API Schemas for Users ---
class UserCreate(BaseModel):
    first_name: str = Field(..., example="Sahana", description="First Name")
    last_name: str = Field(..., example="Thakkar", description="Last Name")
    email: str = Field(..., example="sahana@example.com", description="User Email")


class UserUpdate(BaseModel):
    first_name: str | None = Field(None, example="Sahana (Updated)")
    last_name: str | None = Field(None, example="Thakkar")
    email: str | None = Field(None, example="sahana.updated@example.com")


class UserResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    email: str

    class Config:
        from_attributes = True


# --- Pydantic API Schemas for Products ---
class ProductCreate(BaseModel):
    name: str = Field(..., example="Wireless Noise-Canceling Headphones")
    description: str | None = Field(None, example="Premium over-ear Bluetooth headphones with active noise cancellation.")
    category: str = Field(..., example="Electronics")
    price: float = Field(..., example=249.99, gt=0)
    stock_quantity: int = Field(..., example=50, ge=0)


class ProductUpdate(BaseModel):
    name: str | None = Field(None, example="Wireless Headphones Pro")
    description: str | None = Field(None)
    category: str | None = Field(None, example="Electronics")
    price: float | None = Field(None, gt=0)
    stock_quantity: int | None = Field(None, ge=0)


class ProductResponse(BaseModel):
    id: int
    name: str
    description: str | None
    category: str
    price: float
    stock_quantity: int

    class Config:
        from_attributes = True
