from sqlalchemy import create_engine

# create connection function

def postgres_engine():
    """
    Helper function untuk melakukan koneksi antara Pandas
    dengan PostgreSQL. Sesuaikan username, password,
    host, dan database name dengan milik masing - masing
    """
    engine = create_engine("postgresql://postgres:cobapassword@172.23.189.101/data_wrangling")

    return engine

engine = postgres_engine()