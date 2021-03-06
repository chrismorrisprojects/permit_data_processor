colSpecs01 = [(1, 3),
(3, 10),
(10, 12),
(12, 15),
(15, 47),
(47, 49),
(49, 55),
(55, 59),
(59, 67),
(67, 99),
(99, 100),
(100, 101),
(101, 102),
(102, 103),
(103, 104),
(104, 105),
(105, 106),
(106, 107),
(107, 108),
(108, 109),
(109, 110),
(110, 111),
(111, 112),
(112, 113),
(113, 120),
(120, 128),
(128, 136),
(136, 137),
(137, 157),
(157, 163),
(163, 164),
(164, 173),
(173, 182),
(182, 183),
(183, 184),
(184, 187),
(187, 511)]

colNames01 = ['rrc_tape_record_id',
'da_status_number',
'da_status_sequence_number',
'da_county_code',
'da_lease_name',
'da_district',
'da_operator_number',
'da_converted_date',
'da_date_app_received.',
'da_operator_name',
'filler',
'da_hb1407_problem_flag',
'da_status_of_app_flag',
'da_not_enough_money_flag',
'da_too_much_money_flag',
'da_p5_problem_flag',
'da_p12_problem_flag',
'da_plat_problem_flag',
'da_w1a_problem_flag',
'da_other_problem_flag',
'da_rule37_problem_flag',
'da_rule38_problem_flag',
'da_rule39_problem_flag',
'da_no_money_flag',
'da_permit',
'da_issue_date.',
'da_withdrawn_date.',
'da_walkthrough_flag',
'da_other_problem_text',
'da_well_number',
'da_built_from_old_master_flag',
'da_status_renumbered_to',
'da_status_renumbered_from',
'da_application_returned_flag',
'da_ecap_filing_flag',
'filler',
'rrc_tape_filler']


colSpecs02 = [(0, 2), (2, 9), (9, 11), (11, 14), (14, 46), (46, 48), (48, 54), (54, 59), (59, 65), (65, 67), (67, 97),
            (97, 103), (103, 112), (112, 118), (118, 121), (121, 129), (129, 137), (137, 145), (145, 153), (153, 161),
            (161, 169), (169, 170), (170, 178), (178, 186), (186, 194), (194, 224), (224, 225), (225, 226), (226, 227),
            (227, 228), (228, 235), (235, 242), (242, 243), (243, 251), (251, 261), (261, 316), (316, 322), (322, 325),
            (325, 333), (333, 339), (339, 345), (345, 358), (358, 400), (400, 442), (442, 470), (470, 471), (471, 479),
            (479, 480), (480, 481), (481, 482), (482, 483), (483, 484), (484, 492), (492, 493), (493, 494), (494, 495),
            (495, 502), (502, 510)]
colNames02 = ['rrc_tape_record_id', 'da_permit_number', 'da_status_sequence_number', 'da_permit_county_code',
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
dateCols02 = ['da_received_date', 'da_permit_issued_date', 'da_permit_amended_date', 'da_permit_extended_date',
            'da_permit_extended_date', 'da_permit_spud_date', 'da_permit_surface_casing_date',
            'da_permit_well_status_date', 'da_permit_expired_date', 'da_permit_cancelled_date', 'da_final_update']
numCols02 = ['da_surface_acres']

daPermitMasterSeg = [colSpecs02, colNames02, dateCols02, numCols02]


