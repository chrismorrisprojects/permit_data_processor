#name, start position, length, planned decoding method
DECODER_DEFAULT = 'pic_any'
DECODER_NUMERIC = 'pic_numeric'
DATE_DECODER_DEFAULT = 'pic_yyyymmdd'

UICROOT01 = [
    ('RRC-TAPE-RECORD-ID', 0, 2, DECODER_DEFAULT),
    ('UIC-CNTL-NO', 2, 9, DECODER_NUMERIC),
    ('UIC-NEXT-AVAILABLE', 11, 15, DECODER_DEFAULT),
    ('UIC-OPER', 26, 6, DECODER_DEFAULT),
    ('UIC-API-NUMBER', 32, 8, DECODER_NUMERIC),
    ('UIC-FIELD-NO', 40, 8, DECODER_NUMERIC),
    ('UIC-CLASS', 48, 1, DECODER_NUMERIC),
    ('UIC-APPR-DATE', 49, 8, DATE_DECODER_DEFAULT),
    ('UIC-W14-DATE', 57, 8, DATE_DECODER_DEFAULT),
    ('UIC-H1-DATE', 65, 8, DATE_DECODER_DEFAULT),
    ('UIC-LETTER-DATE', 73, 8, DATE_DECODER_DEFAULT),
    ('UIC-PERMIT-ADDED-DATE', 81, 8, DATE_DECODER_DEFAULT),
    ('UIC-ACTIVATED-FLAG', 89, 1, DECODER_DEFAULT),
    ('UIC-CANCEL-DATE', 90, 8, DATE_DECODER_DEFAULT),
    ('UIC-W2-G1-DATE', 98, 8, DATE_DECODER_DEFAULT),
    ('UIC-W3-DATE', 106, 8, DATE_DECODER_DEFAULT),
    ('UIC-TYPE-INJ', 114, 1, DECODER_NUMERIC),
    ('UIC-TYPE-INJ-CMT', 115, 30, DECODER_DEFAULT),
    ('UIC-TYPE-FLU-CMT', 145, 30, DECODER_DEFAULT),
    ('UIC-BBL-VOL-INJ', 175, 9, DECODER_NUMERIC),
    ('UIC-MCF-VOL-INJ', 184, 9, DECODER_NUMERIC),
    ('UIC-TOP-INJ-ZONE', 193, 4, DECODER_NUMERIC),
    ('UIC-BOT-INJ-ZONE', 198, 5, DECODER_NUMERIC),
    ('UIC-MAX-INJ-PRESSURE', 203, 5, DECODER_NUMERIC),
    ('UIC-H1-NO', 208, 5, DECODER_NUMERIC),
    ('UIC-W14-NO', 213, 5, DECODER_NUMERIC),
    ('UIC-INJ-SW', 218, 1, DECODER_DEFAULT),
    ('UIC-INJ-FW', 219, 1, DECODER_DEFAULT),
    ('UIC-INJ-FRAC-WATER', 220, 1, DECODER_DEFAULT),
    ('UIC-INJ-NORM', 221, 1, DECODER_DEFAULT),
    ('UIC-INJ-CO2', 222, 1, DECODER_DEFAULT),
    ('UIC-INJ-GAS', 223, 1, DECODER_DEFAULT),
    ('UIC-INJ-H2S', 224, 1, DECODER_DEFAULT),
    ('UIC-INJ-POLYMER', 225, 1, DECODER_DEFAULT),
    ('UIC-INJ-STEAM', 226, 1, DECODER_DEFAULT),
    ('UIC-INJ-AIR', 227, 1, DECODER_DEFAULT),
    ('UIC-INJ-NITROGEN', 228, 1, DECODER_DEFAULT),
    ('UIC-INJ-OTHER', 229, 1, DECODER_DEFAULT),
    ('UIC-INJ-BW', 230, 1, DECODER_DEFAULT),
    ('UIC-INJ-LPG ', 231, 1, DECODER_DEFAULT),
    ('UIC-MAX-INJ-PRESSURE-2', 232, 5, DECODER_NUMERIC),
    ('UIC-SPEC-ANN-PRESS-TEST', 237, 1, DECODER_DEFAULT),
    ('UIC-SPEC-ANN-RAD-TRACER-SUR', 238, 1, DECODER_DEFAULT),
    ('UIC-SPEC-ANN-TEMP-SURVEY', 239, 1, DECODER_DEFAULT),
    ('UIC-SPEC-DOWNHOLE-SURVEY', 240, 1, DECODER_DEFAULT),
    ('UIC-SPEC-SEMI-ANNUAL-PT', 241, 1, DECODER_DEFAULT),
    ('UIC-SPEC-TBG-CSG-ANNULUS', 242, 1, DECODER_DEFAULT),
    ('UIC-SPEC-TBG-CSG-FREQ', 243, 1, DECODER_DEFAULT),
    ('UIC-SPEC-MONTR-PRESS', 244, 1, DECODER_DEFAULT),
    ('UIC-SPEC-MONTR-PRESS-CODE-1', 245, 2, DECODER_DEFAULT),
    ('UIC-SPEC-MONTR-PRESS-CODE-2', 246, 2, DECODER_DEFAULT),
    ('UIC-SPEC-MONTR-PRESS-FREQ', 249, 1, DECODER_DEFAULT),
    ('UIC-SPEC-MEAS-FLUID-LEVEL', 250, 1, DECODER_DEFAULT),
    ('UIC-SPEC-MEAS-FLUID-FREQ', 251, 1, DECODER_DEFAULT),
    ('UIC-SPEC-COMMERCIAL', 242, 1, DECODER_DEFAULT),
    ('UIC-SPEC-CEMENT-SQZ', 253, 1, DECODER_DEFAULT),
    ('UIC-SPEC-CEMENT-SQZ-AMT1', 254, 6, DECODER_NUMERIC),
    ('UIC-SPEC-CEMENT-SQZ-AMT2', 260, 3, DECODER_NUMERIC),
    ('UIC-SPEC-CEMENT-SQZ-2', 263, 1, DECODER_DEFAULT),
    ('UIC-SPEC-CEMENT-SQZ-2-AMT1', 264, 6, DECODER_NUMERIC),
    ('UIC-SPEC-CEMENT-SQZ-2-AMT2', 270, 3, DECODER_NUMERIC),
    ('UIC-SPEC-BLOCK-SQZ-SX', 273, 1, DECODER_DEFAULT),
    ('UIC-SPEC-BLOCK-SQZ-SX-AMT1', 274, 6, DECODER_NUMERIC),
    ('UIC-SPEC-BLOCK-SQZ-SX-AMT2', 280, 3, DECODER_NUMERIC),
    ('UIC-SPEC-BLOCK-SQZ', 283, 1, DECODER_DEFAULT),
    ('UIC-SPEC-BLOCK-SQZ-AMT', 284, 6, DECODER_NUMERIC),
    ('UIC-SPEC-SQZ-PERF', 290, 1, DECODER_DEFAULT),
    ('UIC-SPEC-SQZ-PERF-DPTH', 291, 6, DECODER_NUMERIC),
    ('UIC-SPEC-BRIDGE-PLUG', 297, 1, DECODER_DEFAULT),
    ('UIC-SPEC-BRIDGE-PLUG-DPTH', 298, 6, DECODER_NUMERIC),
    ('UIC-SPEC-FLUID-SOURCE-LIMIT', 304, 1, DECODER_DEFAULT),
    ('UIC-SPEC-PLUG-AREA-WELLS', 305, 1, DECODER_DEFAULT),
    ('UIC-SPEC-PLUG-AREA-WELLS-NO', 306, 2, DECODER_NUMERIC),
    ('UIC-SPEC-PERMIT-EXP', 308, 1, DECODER_DEFAULT),
    ('UIC-EXCEPTIONS', 319, 33, DECODER_DEFAULT),
    ('UIC-DPTH-BOT-OF-TOP-ZONE', 352, 4, DECODER_NUMERIC),
    ('UIC-DPTH-TOP-OF-SPLIT-ZONE', 356, 4, DECODER_NUMERIC),
    ('UIC-DPTH-BOT-OF-SPLIT-ZONE', 360, 4, DECODER_NUMERIC),
    ('UIC-LOCATION', 364, 52, DECODER_DEFAULT),
    ('UIC-SURVEY-LINES', 416, 28, DECODER_DEFAULT),
    ('UIC-STATUS', 444, 1, DECODER_DEFAULT),
    ('UIC-GEOTHERMAL', 445, 1, DECODER_DEFAULT),
    ('UIC-MISMATCH', 446, 1, DECODER_DEFAULT),
    ('UIC-DEPTH-BOZ', 447, 5, DECODER_NUMERIC),
    ('UIC-DEPTH-PKR', 452, 5, DECODER_NUMERIC),
    ('UIC-INJ-MODE', 457, 1, DECODER_DEFAULT),
    ('UIC-TECHNICIAN-REVIEW-DATE', 458, 8, DATE_DECODER_DEFAULT),
    ('UIC-TECHNICIAN-INITIALS', 466, 3, DECODER_DEFAULT),
    ('UIC-TECHNICIAN-RESULTS', 469, 6, DECODER_DEFAULT),
    ('UIC-PROD-CASING-AMT', 475, 2, DECODER_NUMERIC),
    ('UIC-PROD-CASING-FRACTION-1', 477, 2, DECODER_NUMERIC),
    ('UIC-PROD-CASING-FRACTION-2', 479, 2, DECODER_NUMERIC),
    ('UIC-PROD-CASING-DEPTH', 481, 5, DECODER_NUMERIC),
    ('UIC-PROD-CASING-CEMENT', 486, 4, DECODER_NUMERIC),
    ('UIC-PROD-CASING-TOP-CEMENT', 490, 5, DECODER_DEFAULT),
    ('UIC-PROD-CASING-PKR-DEPTH', 495, 5, DECODER_NUMERIC),
    ('UIC-GAS-PLANT-COMMENT', 500, 5, DECODER_DEFAULT),
    ('UIC-SPECIAL-COND-CMT', 520, 28, DECODER_DEFAULT),
    ('UIC-FILE-REVIEW-DATE', 548, 8, DATE_DECODER_DEFAULT),
    ('UIC-FILE-REVIEW-INIT', 556, 3, DECODER_DEFAULT),
    ('UIC-DOCKET-NO-DIST', 559, 2, DECODER_DEFAULT),
    ('UIC-DOCKET-NO-FILLER', 561, 2, DECODER_DEFAULT),
    ('UIC-DOCKET-NO-UIC-OLD-DOCKET-NO', 563, 5, DECODER_NUMERIC),
    ('UIC-UIROOT-FILLER', 568,  54, DECODER_DEFAULT)
]


def uicLayout(startVal):
    layouts_map = {
        '01': {'name': 'UICROOT01', 'layout': UICROOT01},
    }
    returnval = layouts_map[startVal]

    return returnval