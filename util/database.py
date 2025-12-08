from sqlalchemy import create_engine
url = 'mysql+pymysql://web_user:pass@localhost:3306/mydb'

engine = create_engine(url, echo=False, pool_size=1)

def get_engine():
    return engine