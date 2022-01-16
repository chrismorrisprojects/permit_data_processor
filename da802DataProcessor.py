import pandas as pd
from dotenv import dotenv_values
import psycopg2
import psycopg2.extras as extras
import csv
import io
from multiprocessing import Pool


config = dotenv_values(".env")
conn = psycopg2.connect(dbname=config['pgdatabase'], user=config['pgadmin'], password=config['pgpassword'])
BASE_DIR = config['DATA_DIR']
DATA_DIR = 'texas/drillingMasterDatabaseWithTrailers/'
naDate = '1800-01-01'
dateFmt = "%Y%m%d"
num_cores = 4


tableDefs = {
    "01": "DA ROOT SEGMENT",
    "02": "DA PERMIT MASTER SEGMENT",
    "03": "DA FIELD SEGMENT",
    "04": "DA FIELD SPECIFIC DATA SEGMENT",
    "05": "DA FIELD BOTTOM-HOLE LOCATION SEGMENT",
    "06": "DA CANNED RESTRICTIONS",
    "07": "DA CANNED RESTRICTION FIELDS",
    "08": "DA FREE-FORM RESTRICTIONS",
    "09": "DA FREE-FORM RESTRICTION FIELDS",
    "10": "DA BOTTOM-HOLE LOCATION SEGMENT",
    "11": "DA ALTERNATE ADDRESS SEGMENT",
    "12": "DA PERMIT REMARKS",
    "13": "DA CHECK REGISTER SEGMENT",
    "14": "DA GIS SURFACE LOCATION COORDINATES",
    "15": "DA GIS BOTTOM HOLE LOCATION COORDINATES"
}
colSpecs = [(0, 2), (2, 9), (9, 11), (11, 14), (14, 46), (46, 48), (48, 54), (54, 59), (59, 65), (65, 67), (67, 97),
            (97, 103), (103, 112), (112, 118), (118, 121), (121, 129), (129, 137), (137, 145), (145, 153), (153, 161),
            (161, 169), (169, 170), (170, 178), (178, 186), (186, 194), (194, 224), (224, 225), (225, 226), (226, 227),
            (227, 228), (228, 235), (235, 242), (242, 243), (243, 325),
            (325, 333), (333, 339), (339, 345), (345, 358), (358, 400), (400, 442), (442, 470), (470, 471), (471, 479),
            (479, 480), (480, 481), (481, 482), (482, 483), (483, 484), (484, 492), (492, 493), (493, 494), (494, 495),
            (495, 502), (502, 510)]
colNames = ['rrc_tape_record_id', 'da_permit_number', 'da_status_sequence_number', 'da_permit_county_code',
            'da_lease_name', 'da_district', 'da_permit_well_number', 'da_permit_total_depth',
            'da_permit_operator_number', 'da_type_application', 'da_other_explanation', 'da_address_unique_number',
            'da_zip_code', 'da_fiche_set_number', 'da_onshore_county', 'da_received_date', 'da_permit_issued_date',
            'da_permit_amended_date', 'da_permit_extended_date', 'da_permit_spud_date', 'da_permit_surface_casing_date',
            'da_well_status', 'da_permit_well_status_date', 'da_permit_expired_date', 'da_permit_cancelled_date',
            'da_cancellation_reason', 'da_p12_filed_flag', 'da_substandard_acreage_flag', 'da_rule_36_flag ',
            'da_h9_flag', 'da_rule_37_case_number', 'da_rule_38_docket_number', 'da_location_formation_flag',
            'da_surface_location',
            'da_surface_acres', 'da_surface_miles_from_city', 'da_surface_direction_from_city',
            'da_surface_nearest_city', 'da_surface_lease_distance', 'da_surface_survey_distance', 'da_nearest_well',
            'da_nearest_well_format_flag', 'da_final_update', 'da_cancelled_flag', 'da_spud_in_flag',
            'da_directional_well_flag', 'da_sidetrack_well_flag', 'da_moved_indicator', 'da_permit_conv_issued_date',
            'da_rule_37_granted_code', 'da_horizontal_well_flag', 'da_duplicate_permit_flag', 'da_nearest_lease_line',
            'api_number']
dateCols = ['da_received_date', 'da_permit_issued_date', 'da_permit_amended_date', 'da_permit_extended_date',
            'da_permit_extended_date', 'da_permit_spud_date', 'da_permit_surface_casing_date',
            'da_permit_well_status_date', 'da_permit_expired_date', 'da_permit_cancelled_date', 'da_final_update']
numCols = ['da_surface_acres', 'da_surface_miles_from_city']

def splitFile(line):
    tempDF = pd.DataFrame(columns=colNames)
    if line[0:2] == "02":
        ioStr = io.StringIO(line)
        tempDF = pd.read_fwf(ioStr, colspecs=colSpecs, names=colNames, converters={34:str, 35:str})
        return tempDF
    else:
        return tempDF


def fmtColDate(df, colList, errAct, naFill, fmtStr):
    for col in colList:
        df[col] = pd.to_datetime(df[col], format=fmtStr, errors=errAct).fillna(naFill)
    return df

def fmtColNumeric(df, colList, errAct, naFill):
    #TODO: Need to check and see if any other columns need to be treated as decimals.
    for col in colList:
        if col == 'da_surface_miles_from_city':
            df[col] = df[col].astype('string')
            df[col] = df[col].str[0:4] + "." + df[col].str[4:6]
            df[col] = pd.to_numeric(df[col], errors=errAct).fillna(naFill)
        elif col == 'da_surface_acres':
            df[col] = df[col].astype('string')
            df[col] = df[col].str[0:6] + "." + df[col].str[6:8]
            df[col] = pd.to_numeric(df[col], errors=errAct).fillna(naFill)
    return df

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
    file = open(f'{BASE_DIR}{DATA_DIR}daf802.txt', 'r')
    lines = file.readlines()
    pool = Pool(num_cores)
    daMainDF = pd.concat(pool.map(splitFile, lines))
    pool.close()
    pool.join()
    daMainDF.to_pickle('completed_802_df.pkl')
    daMainDF = fmtColDate(daMainDF, dateCols, "coerce", naDate, dateFmt)
    daMainDF = fmtColNumeric(daMainDF, numCols, "coerce", 0)
    daMainDF = daMainDF.replace(r'\x00', ' ', regex=True)
    bulkInsert(conn, daMainDF, "public.da_master_trailer")
