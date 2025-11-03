from sqlalchemy import create_engine, text


def create_mysql_engine():
    engine = create_engine(
        'mysql+pymysql://darlie_youli:SE2!5*gg&8MWH$S@rm-wz9v0k73z34y3f6fqeo.mysql.rds.aliyuncs.com:3306/weis')

    return engine


def select_mysql_sql(egine, mysql_str):
    t = text(mysql_str)
    with egine.connect() as conn:

        result = conn.execute(t).fetchall()
        return result


def execute_sql(egine, mysql_str):
    t = text(mysql_str)
    with egine.connect() as conn:

        result = conn.execute(t)
        conn.commit()
        return result
