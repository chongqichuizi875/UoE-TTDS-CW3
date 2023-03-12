from sqlalchemy import create_engine, Integer, Column, String
from sqlalchemy.orm import declarative_base, sessionmaker

# 建立与MySQL的连接
host = "127.0.0.1"
db_name = "wiki"
db_username = "root"
db_password = "wlzxlty200504"
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{host}/{db_name}',
                       echo=False,  # echo 设为 true 会打印出实际执行的 sql，调试的时候更方便
                       future=True,  # 使用 2.0API，向后兼容
                       pool_size=5,  # 连接池的大小默认为 5 个，设置为 0 时表示连接无限制
                       pool_recycle=3600,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
                       pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
                       pool_pre_ping=True  # 悲观方式， 每次执行sql钱会检查连接,解决数据库异常回复后连接依然没有恢复问题
                       )

Base = declarative_base()


class Infos(Base):
    __tablename__ = 'infos'
    id = Column(Integer, primary_key=True)
    title = Column(String(2000), nullable=True)
    introduce = Column(String(5000), nullable=True)
    url = Column(String(2000), nullable=True)


db_session = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True, expire_on_commit=False)()
#Base.metadata.drop_all(engine)
#Base.metadata.create_all(engine)
db_session.commit()
db_session.close()
