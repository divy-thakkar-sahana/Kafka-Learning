from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from app.kafka_poc_3.database import get_db
from app.kafka_poc_3.models import (
    User, UserCreate, UserUpdate, UserResponse,
    Product, ProductCreate, ProductUpdate, ProductResponse
)
from app.kafka_poc_3.consumer import get_cdc_events
from app.kafka_poc_3.es_client import get_es_client, INDEX_NAME
from app.utility.logging_config import get_logger

logger = get_logger("poc_3_router")

router = APIRouter()


# ============================================================================
# USER ENDPOINTS (CDC Basics)
# ============================================================================

@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED, summary="Create User in DB (CDC INSERT)")
def create_user_endpoint(user_in: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == user_in.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="User with this email already exists.")

    db_user = User(first_name=user_in.first_name, last_name=user_in.last_name, email=user_in.email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


@router.get("/users", response_model=list[UserResponse], summary="List Users from DB")
def list_users_endpoint(db: Session = Depends(get_db)):
    return db.query(User).all()


@router.put("/users/{user_id}", response_model=UserResponse, summary="Update User in DB (CDC UPDATE)")
def update_user_endpoint(user_id: int, user_in: UserUpdate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found.")

    if user_in.first_name is not None:
        db_user.first_name = user_in.first_name
    if user_in.last_name is not None:
        db_user.last_name = user_in.last_name
    if user_in.email is not None:
        db_user.email = user_in.email

    db.commit()
    db.refresh(db_user)
    return db_user


@router.delete("/users/{user_id}", summary="Delete User from DB (CDC DELETE)")
def delete_user_endpoint(user_id: int, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found.")

    db.delete(db_user)
    db.commit()
    return {"message": f"User ID {user_id} deleted successfully.", "cdc_notice": "Debezium CDC DELETE event streamed to Kafka."}


# ============================================================================
# PRODUCT ENDPOINTS (CDC → Elasticsearch Sync Pipeline)
# ============================================================================

@router.post("/products", response_model=ProductResponse, status_code=status.HTTP_201_CREATED, summary="Create Product in DB (Triggers CDC → ES Indexing)")
def create_product_endpoint(prod_in: ProductCreate, db: Session = Depends(get_db)):
    """
    Inserts a Product into PostgreSQL. 
    Notice: NO DIRECT ELASTICSEARCH OR KAFKA CALL IS MADE HERE.
    PostgreSQL WAL -> Debezium -> Kafka -> ES Sync Consumer -> Elasticsearch.
    """
    db_prod = Product(
        name=prod_in.name,
        description=prod_in.description,
        category=prod_in.category,
        price=prod_in.price,
        stock_quantity=prod_in.stock_quantity,
    )
    db.add(db_prod)
    db.commit()
    db.refresh(db_prod)
    return db_prod


@router.get("/products", response_model=list[ProductResponse], summary="List All Products directly from Database")
def list_products_endpoint(db: Session = Depends(get_db)):
    return db.query(Product).all()


@router.put("/products/{product_id}", response_model=ProductResponse, summary="Update Product in DB (Triggers CDC → ES Re-index)")
def update_product_endpoint(product_id: int, prod_in: ProductUpdate, db: Session = Depends(get_db)):
    db_prod = db.query(Product).filter(Product.id == product_id).first()
    if not db_prod:
        raise HTTPException(status_code=404, detail="Product not found.")

    if prod_in.name is not None:
        db_prod.name = prod_in.name
    if prod_in.description is not None:
        db_prod.description = prod_in.description
    if prod_in.category is not None:
        db_prod.category = prod_in.category
    if prod_in.price is not None:
        db_prod.price = prod_in.price
    if prod_in.stock_quantity is not None:
        db_prod.stock_quantity = prod_in.stock_quantity

    db.commit()
    db.refresh(db_prod)
    return db_prod


@router.delete("/products/{product_id}", summary="Delete Product from DB (Triggers CDC → ES Deletion)")
def delete_product_endpoint(product_id: int, db: Session = Depends(get_db)):
    db_prod = db.query(Product).filter(Product.id == product_id).first()
    if not db_prod:
        raise HTTPException(status_code=404, detail="Product not found.")

    db.delete(db_prod)
    db.commit()
    return {"message": f"Product ID {product_id} deleted from DB.", "cdc_notice": "Debezium will stream DELETE event to remove it from Elasticsearch."}


# ============================================================================
# SEARCH ENDPOINT (Elasticsearch Full-Text Query with DB Fallback)
# ============================================================================

@router.get("/search", summary="Full-Text Product Search via Elasticsearch")
def search_products_endpoint(
    q: str = Query("", description="Search term for name or description"),
    category: str | None = Query(None, description="Filter by category"),
    min_price: float | None = Query(None, ge=0, description="Minimum price filter"),
    max_price: float | None = Query(None, ge=0, description="Maximum price filter"),
    db: Session = Depends(get_db)
):
    """
    Performs full-text search against the Elasticsearch index populated automatically by Debezium CDC!
    If Elasticsearch is unreachable, falls back gracefully to SQL ILIKE query on PostgreSQL.
    """
    es = get_es_client()
    
    # Try Elasticsearch query
    try:
        must_clauses = []
        filter_clauses = []

        if q.strip():
            must_clauses.append({
                "multi_match": {
                    "query": q,
                    "type": "bool_prefix",
                    "fields": ["name^3", "description^1", "category^2"]
                }
            })
        else:
            must_clauses.append({"match_all": {}})

        if category:
            filter_clauses.append({"term": {"category": category}})

        if min_price is not None or max_price is not None:
            range_filter = {}
            if min_price is not None:
                range_filter["gte"] = min_price
            if max_price is not None:
                range_filter["lte"] = max_price
            filter_clauses.append({"range": {"price": range_filter}})

        query_body = {
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            }
        }

        res = es.search(index=INDEX_NAME, body=query_body)
        hits = res["hits"]["hits"]
        results = [hit["_source"] for hit in hits]

        return {
            "source": "ELASTICSEARCH",
            "total": res["hits"]["total"]["value"],
            "query": q,
            "results": results
        }

    except Exception as exc:
        logger.warning(f"Elasticsearch search failed ({exc}). Falling back to PostgreSQL DB search.")
        
        # Fallback to Database SQL search
        query = db.query(Product)
        if q.strip():
            query = query.filter(Product.name.ilike(f"%{q}%") | Product.description.ilike(f"%{q}%"))
        if category:
            query = query.filter(Product.category == category)
        if min_price is not None:
            query = query.filter(Product.price >= min_price)
        if max_price is not None:
            query = query.filter(Product.price <= max_price)

        products = query.all()
        return {
            "source": "POSTGRES_FALLBACK",
            "total": len(products),
            "query": q,
            "results": [
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description,
                    "category": p.category,
                    "price": p.price,
                    "stock_quantity": p.stock_quantity,
                }
                for p in products
            ]
        }


@router.get("/cdc-events", summary="View Real-Time Debezium CDC Events from Kafka")
def get_cdc_events_endpoint(limit: int = 50):
    events = get_cdc_events(limit)
    return {
        "status": "success",
        "topic": "dbserver1.public.users",
        "count": len(events),
        "events": events,
    }


# ============================================================================
# RICH INTERACTIVE SEARCH & CDC UI (/poc-3/ui)
# ============================================================================

@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def search_ui_page():
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kafka Debezium CDC Search Portal</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
        body { background-color: #f8fafc; color: #1e293b; padding: 2rem 1rem; }
        .container { max-width: 1000px; margin: 0 auto; }
        
        .header { text-align: center; margin-bottom: 2rem; }
        .header h1 { font-size: 1.8rem; font-weight: 700; color: #0f172a; }
        .header p { color: #64748b; font-size: 0.95rem; margin-top: 0.3rem; }

        .search-box { margin-bottom: 2rem; }
        .search-input { width: 100%; padding: 1rem 1.25rem; font-size: 1.05rem; border: 2px solid #e2e8f0; border-radius: 10px; background: white; outline: none; transition: border-color 0.2s; box-shadow: 0 2px 4px rgba(0,0,0,0.02); }
        .search-input:focus { border-color: #3b82f6; }

        .grid { display: grid; grid-template-columns: 280px 1fr; gap: 1.5rem; }
        @media(max-width: 768px) { .grid { grid-template-columns: 1fr; } }

        .panel { background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
        .panel h3 { font-size: 1rem; font-weight: 600; color: #0f172a; margin-bottom: 1rem; }

        .form-group { margin-bottom: 0.85rem; }
        .form-group label { display: block; font-size: 0.8rem; font-weight: 600; color: #475569; margin-bottom: 0.3rem; }
        .form-input { width: 100%; padding: 0.6rem 0.75rem; font-size: 0.9rem; border: 1px solid #cbd5e1; border-radius: 6px; outline: none; }
        .form-input:focus { border-color: #3b82f6; }

        .btn { width: 100%; padding: 0.65rem; background: #2563eb; color: white; border: none; border-radius: 6px; font-weight: 600; font-size: 0.9rem; cursor: pointer; }
        .btn:hover { background: #1d4ed8; }

        .status-line { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
        .badge { font-size: 0.75rem; font-weight: 600; padding: 0.25rem 0.6rem; border-radius: 6px; background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }

        .cards-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 1rem; }
        .card { background: white; border: 1px solid #e2e8f0; border-radius: 8px; padding: 1rem; transition: border-color 0.2s; }
        .card:hover { border-color: #93c5fd; }
        .card-category { font-size: 0.7rem; font-weight: 700; color: #2563eb; text-transform: uppercase; }
        .card-title { font-size: 1rem; font-weight: 600; color: #0f172a; margin: 0.2rem 0; }
        .card-desc { font-size: 0.8rem; color: #64748b; margin-bottom: 0.75rem; min-height: 2.4em; }
        .card-footer { display: flex; justify-content: space-between; align-items: center; font-size: 0.85rem; }
        .card-price { font-weight: 700; color: #059669; font-size: 1.05rem; }
        .card-stock { color: #64748b; font-size: 0.75rem; }

        .empty { text-align: center; padding: 2.5rem; color: #94a3b8; font-size: 0.9rem; grid-column: 1 / -1; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Debezium CDC & Elasticsearch Search</h1>
            <p>DB Writes → Debezium CDC → Kafka → Elasticsearch Index</p>
        </div>

        <div class="search-box">
            <input type="text" id="searchInput" class="search-input" placeholder="Type to search products in Elasticsearch (e.g., headphones, electronics)...">
        </div>

        <div class="grid">
            <!-- Left: Add Product Form -->
            <div class="panel">
                <h3>Add Product</h3>
                <form id="productForm">
                    <div class="form-group">
                        <label>Product Name</label>
                        <input type="text" id="pName" class="form-input" placeholder="e.g. Wireless Mouse" required>
                    </div>
                    <div class="form-group">
                        <label>Category</label>
                        <input type="text" id="pCategory" class="form-input" placeholder="e.g. Electronics" required>
                    </div>
                    <div class="form-group">
                        <label>Price ($)</label>
                        <input type="number" step="0.01" id="pPrice" class="form-input" placeholder="29.99" required>
                    </div>
                    <div class="form-group">
                        <label>Stock</label>
                        <input type="number" id="pStock" class="form-input" placeholder="50" required>
                    </div>
                    <div class="form-group">
                        <label>Description</label>
                        <input type="text" id="pDesc" class="form-input" placeholder="Short description">
                    </div>
                    <button type="submit" class="btn">Add Product</button>
                </form>
            </div>

            <!-- Right: Search Results -->
            <div class="panel">
                <div class="status-line">
                    <h3>Products Index</h3>
                    <div>
                        <span id="sourceBadge" class="badge">Source: ELASTICSEARCH</span>
                        <span id="countBadge" class="badge">0 items</span>
                    </div>
                </div>

                <div id="resultsGrid" class="cards-list">
                    <div class="empty">Type a query above or add a product to see live results.</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let timer;

        async function doSearch(q = "") {
            try {
                const res = await fetch(`/poc-3/search?q=${encodeURIComponent(q)}`);
                const data = await res.json();
                
                document.getElementById('sourceBadge').textContent = `Source: ${data.source}`;
                document.getElementById('countBadge').textContent = `${data.total} items`;

                const grid = document.getElementById('resultsGrid');
                if (!data.results || data.results.length === 0) {
                    grid.innerHTML = '<div class="empty">No products found.</div>';
                    return;
                }

                grid.innerHTML = data.results.map(p => `
                    <div class="card">
                        <div class="card-category">${p.category || 'General'}</div>
                        <div class="card-title">${p.name}</div>
                        <div class="card-desc">${p.description || ''}</div>
                        <div class="card-footer">
                            <span class="card-price">$${Number(p.price).toFixed(2)}</span>
                            <span class="card-stock">Stock: ${p.stock_quantity}</span>
                        </div>
                    </div>
                `).join('');
            } catch (err) {
                console.error("Search failed:", err);
            }
        }

        document.getElementById('searchInput').addEventListener('input', (e) => {
            clearTimeout(timer);
            timer = setTimeout(() => doSearch(e.target.value), 200);
        });

        document.getElementById('productForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const body = {
                name: document.getElementById('pName').value,
                category: document.getElementById('pCategory').value,
                price: parseFloat(document.getElementById('pPrice').value),
                stock_quantity: parseInt(document.getElementById('pStock').value),
                description: document.getElementById('pDesc').value
            };

            const res = await fetch('/poc-3/products', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            if (res.ok) {
                document.getElementById('productForm').reset();
                setTimeout(() => doSearch(document.getElementById('searchInput').value), 1200);
            }
        });

        doSearch();
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)
