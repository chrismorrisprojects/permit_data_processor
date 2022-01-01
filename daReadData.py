import pandas as pd
from dotenv import dotenv_values

config = dotenv_values(".env")
DATA_DIR = config['DATA_DIR']
TEXAS_UIC_DIR = 'texas/drillMasterDatabase/'
naDate = '1800-01-01'
dateFmt = "%Y%m%d"
colSpecs = [(0, 2), (2, 9), (9, 11), (11, 14), (14, 46), (46, 48), (48, 54), (54, 59), (59, 65), (65, 67), (67, 97),
            (97, 103), (103, 112), (112, 118), (118, 121), (121, 129), (129, 137), (137, 145), (145, 153), (153, 161),
            (161, 169), (169, 170), (170, 178), (178, 186), (186, 194), (194, 224), (224, 225), (225, 226), (226, 227),
            (227, 228), (228, 235), (235, 242), (242, 243), (243, 251), (251, 261), (261, 316), (316, 322), (322, 325),
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
            'da_surface_section', 'da_surface_block', 'da_surface_survey', 'da_surface_abstract', 'da_surface_filler',
            'da_surface_acres', 'da_surface_miles_from_city', 'da_surface_direction_from_city',
            'da_surface_nearest_city', 'da_surface_lease_distance', 'da_surface_survey_distance', 'da_nearest_well',
            'da_nearest_well_format_flag', 'da_final_update', 'da_cancelled_flag', 'da_spud_in_flag',
            'da_directional_well_flag', 'da_sidetrack_well_flag', 'da_moved_indicator', 'da_permit_conv_issued_date',
            'da_rule_37_granted_code', 'da_horizontal_well_flag', 'da_duplicate_permit_flag', 'da_nearest_lease_line',
            'api_number']
dateCols = ['da_received_date', 'da_permit_issued_date', 'da_permit_amended_date', 'da_permit_extended_date',
            'da_permit_extended_date', 'da_permit_spud_date', 'da_permit_surface_casing_date',
            'da_permit_well_status_date', 'da_permit_expired_date', 'da_permit_cancelled_date', 'da_final_update']
numCols = ['da_surface_acres']


def fmtColDate(df, colList, errAct, naFill, fmtStr):
    for col in colList:
        df[col] = pd.to_datetime(df[col], format=fmtStr, errors=errAct).fillna(naFill)
    return df

def fmtColNumeric(df, colList, errAct, naFill):
    for col in colList:
        df[col] = pd.to_datetime(df[col], errors=errAct).fillna(naFill)
    return df

if __name__ == "__main__":
    file = open(f'{DATA_DIR}{TEXAS_UIC_DIR}daf804.txt', 'r')
    daMainDF = pd.read_fwf(file, colspecs=colSpecs, names=colNames)
    daMainDF = fmtColDate(daMainDF, dateCols, "coerce", naDate, dateFmt)
    daMainDF = fmtColNumeric(daMainDF, numCols, "coerce", 0)
    daMainDF.to_pickle('daMainDF.pkl')
    daMainDF.to_csv('test_da_main.csv')
