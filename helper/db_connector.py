from sqlalchemy import create_engine

# create connection function

def postgres_engine():
    """
    Helper function untuk melakukan koneksi antara Pandas
    dengan PostgreSQL. Sesuaikan username, password,
    host, dan database name dengan milik masing - masing
    """
    engine = create_engine("postgresql://[USERNAME]:[PASSWORD]@[HOSTNAME]/[DB_NAME]")

    return engine

engine = postgres_engine()