import pandas as pd
import psycopg2
import psycopg2.extras as extras
from dotenv import dotenv_values

config = dotenv_values(".env")
conn = psycopg2.connect(dbname=config['pgdatabase'], user=config['pgadmin'], password=config['pgpassword'])


def execQry(conn, df, table):
    cols = ','.join(list(df.columns))
    cols = cols.replace("-", "_")
    for index, row in df.iterrows():
        qryVals = tuple(list(row))
        print((qryVals,))
#        qryVals = [tuple(x) for x in row]
        qry = f'INSERT INTO {table}({cols}) VALUES %s'
        cursor = conn.cursor()
        try:
            cursor.execute(qry, (qryVals,))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            print(qry)
            conn.rollback()
            cursor.close()
            return 1
    print("execute_values() done")
    cursor.close()

def bulkInsert(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    cols = cols.replace("-", "_")
    query = f'INSERT INTO {table}({cols}) VALUES %s'
    print(query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


if __name__ == "__main__":
    daMainDF = pd.read_pickle('daMainDF.pkl')
    #daMainDF = daMainDF.replace(r'\x00', ' ', regex=True)
    print(daMainDF.head())
    #bulkInsert(conn, daMainDF, "public.da_master_trailer")
    #execQry(conn, daMainDF, "public.da_master")
