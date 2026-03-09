from app.core.database import get_session_local, init_db
from app.db.seed import seed_universe


if __name__ == "__main__":
    init_db()
    SessionLocal = get_session_local()
    with SessionLocal() as session:
        seed_universe(session)
    print("ETF universe seeded.")
