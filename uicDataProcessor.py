import pandas as pd
import psycopg2
from definitions.texas.ebcdicFuncs import yield_blocks, parse_record
from definitions.texas.uicDefs import uicLayout
from definitions.texas.ebdicFormats import pic_yyyymmdd, pic_yyyymm, pic_numeric, pic_any, pic_signed, comp3
import psycopg2.extras as extras
from dotenv import dotenv_values
config = dotenv_values(".env")

DATA_DIR = config['DATA_DIR']
TEXAS_UIC_DIR = 'texas/uicdatabase/'

conn = psycopg2.connect(dbname=config['pgdatabase'], user=config['pgadmin'], password=config['pgpassword'])


def readUICFile():
    block_size = 622
    file = open(f'{DATA_DIR}{TEXAS_UIC_DIR}uif700.ebc', 'rb')

    mainDF = pd.DataFrame()
    Limiting_Counter = True
    wellct = 0
    check_stop = 10000
    for block in yield_blocks(file, block_size):
        ##For testing script
        if Limiting_Counter == True and wellct > check_stop:  ##Stops the loop once a set number of wells has been complete
            break

        startval = pic_any(block[:2])  ## first two characters of a block
        if startval == "01":
            layout = uicLayout(startval)['layout']  ##identifies layout based on record start values
            parsed_vals = parse_record(block, layout)  ##formats the record and returns a formated {dict}

            temp_df = pd.DataFrame([parsed_vals], columns=parsed_vals.keys())  ##convert {dict} to dataframe
            # temp_df['api10'] = API ##adds API number to record (might need to move this to first position)
            mainDF = pd.concat([mainDF, temp_df])
        wellct += 1
    return mainDF


def execute_values(conn, df, table):
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
    uicMainDF = readUICFile()
    uicMainDF.drop(['RRC-TAPE-RECORD-ID'], axis=1, inplace=True)
    execute_values(conn, uicMainDF, "public.rrc_uic")


