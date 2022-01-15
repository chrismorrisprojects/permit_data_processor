import pandas as pd
from dotenv import dotenv_values
import psycopg2
import psycopg2.extras as extras
from pathlib import Path


config = dotenv_values(".env")
conn = psycopg2.connect(dbname=config['pgdatabase'], user=config['pgadmin'], password=config['pgpassword'])
DATA_DIR = config['DATA_DIR']
TEXAS_UIC_DIR = 'texas/pendingPermitFiles/'
naDate = '1800-01-01'
dateFmt = "%Y%m%d"

drillingPermitPendingCols = ["SWR38_ABBR_NOTICE", "IS_REAPPLIED", "UNIVERSAL_DOC_NO", "STATUS_NUMBER",
                             "EFFECTIVE_DT", "RETURN_DT", "TOTAL_DEPTH", "IS_AMENDMENT", "SWR_36_FLAG",
                             "DEVELOP_MINERALS_FLAG", "CASE_DOCKET_NO", "FINAL_PROTEST_DT", "STATUS_SEQ_NO",
                             "SPUD_DT", "EXPEDITE_FLAG", "EXPEDITE_DATE_TIME", "FILING_PURPOSE_CODE",
                             "SURFACE_CASING_DT", "DEFAULT_LEASE_NAME", "DEFAULT_WELL_NUMBER", "DEFAULT_VERTICAL",
                             "DEFAULT_HORIZANTAL", "DEFAULT_SIDETRACK", "LOCKED_BY", "DEFAULT_DIRECTIONAL",
                             "STATUS_CODE", "EXPIRATION_DATE", "WALKIN_CONTACT_NAME", "WALKIN_CONTACT_PHONE",
                             "COMPLETION_CODE", "SWR_SUBSECT_CODE", "STAT_DT", "CURRENT_STATE_CODE", "BRIDGE_FLAG",
                             "SWR_LIST", "BRIDGE_PRINT_FLAG", "HAS_DISCREPANCY", "SUBMIT_DATE", "CREATE_DATE",
                             "UNIQUE_ADDRESS_NUMBER", "DKT_SUFFIX_CODE", "DKT_EXAMINER_CODE", "REAPPLIED_STATUS_NO",
                             "OVERRIDE_FA_OP_CONSULT_YN", "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE",
                             "DISTRICT"]
latLongsPendingCols = ["API_SEQUENCE_NUMBER", "LATITUDE", "LONGITUDE", "LOCATION_TYPE"]
mailingAddressPendingCols = ["MAILING_ADDRESS_ID", "ADDRESS_LINE1", "ADDRESS_LINE2", "CITY", "COUNTRY_CODE",
                             "STATE_CODE", "FOREIGN_DELIVERY_AREA", "UNIVERSAL_DOC_NO", "POSTAL_CODE",
                             "POSTAL_EXTENSION_CODE", "MODIFIED_BY", "MODIFIED_DT", "OPERATOR_NAME",
                             "OPERATOR_NUMBER", "OPERATOR_PHONE", "DISTRICT"]
permitRestrictionPendingCols = ["PERMIT_RESTRICTION_ID", "RESTRICTION_CODE", "UNIVERSAL_DOC_NO", "RESTRICTION_TEXT",
                                "MODIFIED_BY", "MODIFIED_DT", "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE",
                                "DISTRICT"]
swrResolutionPendingCols = ["UNIVERSAL_DOC_NO", "SWR_RESLTN_CODE", "MODIFIED_BY", "MODIFIED_DT", "SWR_RESOLUTION_ID",
                            "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE", "DISTRICT"]
wellBorePendingCols = ["LATLONG_TYPE_CODE", "LAT_DEGREES", "LAT_MINUTES", "LAT_SECONDS", "LONG_DEGREES",
                       "LONG_MINUTES", "LONG_SECONDS", "STATE_PLANE_ZONE_CODE", "STATE_PLANE_X", "GW1_FLAG",
                       "LAT_DEGREES_S", "LAT_MINUTES_S", "LAT_SECONDS_S", "LONG_DEGREES_S", "LONG_MINUTES_S",
                       "LONG_SECONDS_S", "API_SEQUENCE_NUMBER", "DIRECTIONS", "MODIFIED_BY", "NEAREST_TOWN_DISTANCE",
                       "NEAREST_TOWN", "MODIFIED_DT", "API_LINKED_FLAG", "LOCATION_DESCRIPTION", "COUNTY_CODE",
                       "SURFACE_LOCATION_CODE", "WELLBORE_ID", "OFFSHORE_COUNTY_CODE", "UNIVERSAL_DOC_NO",
                       "HORIZ_WELLBORE_TYPE_CODE", "STACKED_LAT_STATUS_NO", "PSA_FLAG", "ALLOCATION_FLAG",
                       "STACKED_LATERAL_FLAG", "STATE_PLANE_Y", "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE",
                       "DISTRICT"]
wellBoreProfileCols = ["PROFILE_NAME", "WELLBORE_PROFILE_ID", "PROFILE_CODE", "MODIFIED_BY", "PERMITTED_FIELD_ID",
                       "MODIFIED_DT", "NRST_LEASE_DIST_BOTM_LOC", "NRST_LEASE_DIST_FST_LST_TK_PT",
                       "NRST_LEASE_DIST_PRP_ANY_TK_PT", "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE", "DISTRICT"]

perpFieldPendingCols = ["LOCATION_TOWNSHIP", "LOCATION_LOT", "LOCATION_PORCION", "LOCATION_SHARE", "LOCATION_LEAGUE",
                        "LOCATION_LABOR", "LOCATION_TRACT", "PERP_ID", "MODIFIED_BY", "SECTION_LINE1_DISTANCE",
                        "SECTION_LINE1_DIRECTION", "MODIFIED_DT", "SECTION", "SECTION_LINE2_DISTANCE",
                        "ABSTRACT_NUMBER", "SECTION_LINE2_DIRECTION", "SURVEY_NAME", "LOCATION_COMMENTS",
                        "BLOCK_NUMBER", "PERP_TYPE_CODE", "WELLBORE_PROFILE_ID", "PERMITTED_FIELD_ID", "COUNTY_CODE",
                        "PERP_LOC_CODE", "WELLBORE_ID", "MEASURE_LINE_TYPE_CODE", "OPERATOR_NAME", "OPERATOR_NUMBER",
                        "OPERATOR_PHONE", "DISTRICT"]
permittedFieldPendingCols = ["OFF_LEASE_PNTRN_PT_FLAG", "OFF_LEASE_SURF_LOC_FLAG", "REX_OLPP_OWN_OFFSET_YN",
                             "REX_OLPP_WAVIER_YN", "REX_OLPP_NOTICE_YN", "REX_OLPP_PUBLICATION_YN",
                             "REX_OLPP_HEARING_REQUEST_YN", "REX_OLPP_LAST_NOTICE_DT", "REX_OLPP_DOCKET_NO",
                             "WELL_NUMBER", "UNIVERSAL_DOC_NO", "FIELD_ID", "WELL_TYPE_CODE", "MODIFIED_BY",
                             "COMPLETION_DEPTH", "MODIFIED_DT", "PRIMARY_FIELD_FLAG", "NEAREST_WELL_DISTANCE",
                             "NEAREST_LEASE_DISTANCE", "TOTAL_ACRES", "NON_CONCURRENT_37WELLS", "NON_CONCURRENT_38WELLS",
                             "POOLED_UNIT_FLAG", "UNITIZED_DOCKET_NO", "SWR39_RESOLUTION", "REPORTED_LEASE_NAME",
                             "WELL_COUNT", "ENTITY_DENSITY_DOCKET_NO", "FIELD_VALIDATED_DT", "W1A_TRACT_DT",
                             "COMPLETION_DT", "COMPLETION_WELL_CODE", "PERMITTED_FIELD_ID", "TEXT_FOR_85279201",
                             "REX_OLPP_HEARING_OUTCOME_CODE", "HORIZ_DEPTH_SEVERANCE_LOWER", "NRST_LEASE_DIST_SURF_LOC",
                             "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE", "DISTRICT"]
perpPendingCols = ["LOCATION_TOWNSHIP", "LOCATION_LOT", "LOCATION_PORCION", "LOCATION_SHARE", "LOCATION_LEAGUE",
                   "LOCATION_LABOR", "LOCATION_TRACT", "PERP_ID", "MODIFIED_BY", "SECTION_LINE1_DISTANCE",
                   "SECTION_LINE1_DIRECTION", "MODIFIED_DT", "SECTION", "SECTION_LINE2_DISTANCE", "ABSTRACT_NUMBER",
                   "SECTION_LINE2_DIRECTION", "SURVEY_NAME", "LOCATION_COMMENTS", "BLOCK_NUMBER", "PERP_TYPE_CODE",
                   "WELLBORE_PROFILE_ID", "PERMITTED_FIELD_ID", "COUNTY_CODE", "PERP_LOC_CODE", "WELLBORE_ID",
                   "MEASURE_LINE_TYPE_CODE", "OPERATOR_NAME", "OPERATOR_NUMBER", "OPERATOR_PHONE", "DISTRICT"]
perpWellBorePendingCols = ["LOCATION_TOWNSHIP", "LOCATION_LOT", "LOCATION_PORCION", "LOCATION_SHARE", "LOCATION_LEAGUE",
                           "LOCATION_LABOR", "LOCATION_TRACT", "PERP_ID", "MODIFIED_BY", "SECTION_LINE1_DISTANCE",
                           "SECTION_LINE1_DIRECTION", "MODIFIED_DT", "SECTION", "SECTION_LINE2_DISTANCE",
                           "ABSTRACT_NUMBER", "SECTION_LINE2_DIRECTION", "SURVEY_NAME", "LOCATION_COMMENTS",
                           "BLOCK_NUMBER", "PERP_TYPE_CODE", "WELLBORE_PROFILE_ID", "PERMITTED_FIELD_ID", "COUNTY_CODE",
                           "PERP_LOC_CODE", "WELLBORE_ID", "MEASURE_LINE_TYPE_CODE", "OPERATOR_NAME", "OPERATOR_NUMBER",
                           "OPERATOR_PHONE", "DISTRICT"]

wellBoreProfile = pd.DataFrame()
wellBorePending = pd.DataFrame()
swrResolutionPending = pd.DataFrame()
perpWellBorePending = pd.DataFrame()
perpPending = pd.DataFrame()
perpFieldPending = pd.DataFrame()
permittedFieldPending = pd.DataFrame()
permitRestrictionPending = pd.DataFrame()
mailingAddressPending = pd.DataFrame()
latLongsPending = pd.DataFrame()
drillingPermitPending = pd.DataFrame()

def readFolder(dir):
    path = Path(dir)
    for file in path.iterdir():
        print(file)
        if "dp_drilling_permit_pending" in str(file):
            drillingPermitPending = pd.read_csv(file, sep="}", skiprows=[0, 1, 2], names=drillingPermitPendingCols,
                                                header=0, index_col=False)
            drillingPermitPending = drillingPermitPending.append(drillingPermitPending)

        elif "dp_latlongs_pending" in str(file):
            latLongsPending = pd.read_csv(file, sep="}", skiprows=[0], names=latLongsPendingCols, header=0,
                                          index_col=False)
            latLongsPending = latLongsPending.append(latLongsPending)

        elif "dp_mailing_address_pending" in str(file):
            mailingAddressPending = pd.read_csv(file, sep="}", skiprows=[0], names=mailingAddressPendingCols, header=0,
                                          index_col=False)
            mailingAddressPending = mailingAddressPending.append(mailingAddressPending)

        elif "dp_permit_restriction_pending" in str(file):
            permitRestrictionPending = pd.read_csv(file, sep="}", skiprows=[0], names=permitRestrictionPendingCols, header=0,
                                          index_col=False)
            permitRestrictionPending = permitRestrictionPending.append(permitRestrictionPending)

        elif "dp_swr_resolution_pending" in str(file):
            swrResolutionPending = pd.read_csv(file, sep="}", skiprows=[0], names=swrResolutionPendingCols, header=0,
                                          index_col=False)
            swrResolutionPending = swrResolutionPending.append(swrResolutionPending)
        elif "dp_wellbore_pending" in str(file):
            wellBorePending = pd.read_csv(file, sep="}", skiprows=[0], names=wellBorePendingCols, header=0,
                                          index_col=False)
            wellBorePending = wellBorePending.append(wellBorePending)

        elif "dp_wellbore_profile_pending" in str(file):
            wellBoreProfile = pd.read_csv(file, sep="}", skiprows=[0], names=wellBoreProfileCols, header=0,
                                          index_col=False)
            wellBoreProfile = wellBoreProfile.append(wellBoreProfile)

        elif "dp_perp_field_pending" in str(file):
            perpFieldPending = pd.read_csv(file, sep="}", skiprows=[0], names=perpFieldPendingCols, header=0,
                                          index_col=False)
            perpFieldPending = perpFieldPending.append(perpFieldPending)

        elif "dp_permitted_field_pending" in str(file):
            permittedFieldPending = pd.read_csv(file, sep="}", skiprows=[0, 1], names=permittedFieldPendingCols, header=0,
                                          index_col=False)
            permittedFieldPending = permittedFieldPending.append(permittedFieldPending)

        elif "dp_perp_pending" in str(file):
            perpPending = pd.read_csv(file, sep="}", skiprows=[0, 1], names=perpPendingCols, header=0,
                                          index_col=False)
            perpPending = perpPending.append(perpPending)

        elif "dp_perp_wellbore_pending" in str(file):
            perpWellBorePending = pd.read_csv(file, sep="}", skiprows=[0, 1], names=perpWellBorePendingCols, header=0,
                                          index_col=False)
            perpWellBorePending = perpWellBorePending.append(perpWellBorePending)

        else:
            print("undefined pending file type!!")
            break

    return [drillingPermitPending, latLongsPending, mailingAddressPending, permitRestrictionPending,
            swrResolutionPending, wellBorePending, wellBoreProfile, perpFieldPending, permittedFieldPending,
            perpPending, perpWellBorePending]


if __name__ == "__main__":
    collectedFrames = readFolder(f'{DATA_DIR}{TEXAS_UIC_DIR}')
    for frame in collectedFrames:
        print(frame.head())
    #TODO: The text to dataframes works. Need to make tables for it next and then load the data into the tables.
    #file = open(f'{DATA_DIR}{TEXAS_UIC_DIR}daf802.txt', 'r')
    #lines = file.readlines()
    #daMainDF = splitFile(lines)
    #daMainDF.to_csv('testTrailer-pre-num-format.csv')
    #daMainDF = fmtColDate(daMainDF, dateCols, "coerce", naDate, dateFmt)
    #daMainDF = fmtColNumeric(daMainDF, numCols, "coerce", 0)
    #daMainDF = daMainDF.replace(r'\x00', ' ', regex=True)
    #bulkInsert(conn, daMainDF, "public.da_master_trailer")
