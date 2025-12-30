from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

URL= 'mysql+pymysql://web_user:pass@localhost:3306/donga'


engine = create_engine(
    URL,
    echo=False,
    pool_size=5,
    max_overflow=10
)

# DB 커넥션 필요하시면 이 객체 사용하시면 됩니다!
# ex) db = SessionLocal()
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
)
