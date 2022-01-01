import pandas as pd
import psycopg2
from definitions.texas.ebcdicFuncs import yield_blocks, parse_record
from definitions.texas.masterDefs import daLayout
from definitions.texas.ebdicFormats import pic_yyyymmdd, pic_yyyymm, pic_numeric, pic_any, pic_signed, comp3
import psycopg2.extras as extras
from dotenv import dotenv_values
import numpy as np
config = dotenv_values(".env")

DATA_DIR = config['DATA_DIR']
TEXAS_UIC_DIR = 'texas/drillMasterDatabase/'

conn = psycopg2.connect(dbname=config['pgdatabase'], user=config['pgadmin'], password=config['pgpassword'])

colspecs = [(0,2),(2,9),(9,11),(11,14),(14,46),(46,48),(48,54),(54,59),(59,65),(65,67),(67,97),(97,103),(103,112),
            (112,118),(118,121),(121,129),(129,137),(137,145),(145,153),(153,161),(161,169),(169,170),(170,178),
            (178,186),(186,194),(194,224),(224,225),(225,226),(226,227),(227,228),(228,235),(235,242),(242,243),
            (243,325),(325,333),(333,339),(339,345),(345,358),(358,400),(400,442),(442,470),(470,471),(471,479),
            (479,480),(480,481),(481,482),(482,483),(483,484),(484,492),(492,493),(493,494),(494,495),(495,502),(502,510)]

names = ['rrc_tape_record_id','da_permit_number','da_status_sequence_number','da_permit_county_code','da_lease_name',
         'da_district','da_permit_well_number','da_permit_total_depth','da_permit_operator_number',
         'da_type_application','da_other_explanation','da_address_unique_number','da_zip_code',
         'da_fiche_set_number','da_onshore_county','da_received_date','da_permit_issued_date','da_permit_amended_date',
         'da_permit_extended_date','da_permit_spud_date','da_permit_surface_casing_date','da_well_status',
         'da_permit_well_status_date','da_permit_expired_date','da_permit_cancelled_date','da_cancellation_reason',
         'da_p12_filed_flag','da_substandard_acreage_flag','da_rule_36_flag ','da_h9_flag','da_rule_37_case_number',
         'da_rule_38_docket_number','da_location_formation_flag','da_old_surface_location','da_surface_acres',
         'da_surface_miles_from_city','da_surface_direction_from_city','da_surface_nearest_city',
         'da_surface_lease_distance','da_surface_survey_distance','da_nearest_well','da_nearest_well_format_flag',
         'da_final_update','da_cancelled_flag','da_spud_in_flag','da_directional_well_flag','da_sidetrack_well_flag',
         'da_moved_indicator','da_permit_conv_issued_date','da_rule_37_granted_code','da_horizontal_well_flag',
         'da_duplicate_permit_flag','da_nearest_lease_line','api_number']

dateFmt = "%Y%m%d"

def colTypeDef(df):
    df['da_received_date'] = pd.to_datetime(df['da_received_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_issued_date'] = pd.to_datetime(df['da_permit_issued_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_amended_date'] = pd.to_datetime(df['da_permit_amended_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_extended_date'] = pd.to_datetime(df['da_permit_extended_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_spud_date'] = pd.to_datetime(df['da_permit_spud_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_surface_casing_date'] = pd.to_datetime(df['da_permit_surface_casing_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_well_status_date'] = pd.to_datetime(df['da_permit_well_status_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_expired_date'] = pd.to_datetime(df['da_permit_expired_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_permit_cancelled_date'] = pd.to_datetime(df['da_permit_cancelled_date'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df['da_final_update'] = pd.to_datetime(df['da_final_update'], format=dateFmt, errors='coerce').fillna('1800-01-01')
    df.replace({np.nan: ""}, inplace=True)
    return df


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    cols = cols.replace("-", "_")
    query = f'INSERT INTO {table}({cols}) VALUES %s'
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        print(query)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

if __name__ == "__main__":
    file = open(f'{DATA_DIR}{TEXAS_UIC_DIR}daf804.txt', 'r')
    daMainDF = pd.read_fwf(file, colspecs=colspecs, names=names, encoding='utf8')
    daMainDF.to_csv("test_uic_db.csv")
    daMainDF = colTypeDef(daMainDF)
    execute_values(conn, daMainDF, "public.da_master")


