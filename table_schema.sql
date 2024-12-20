CREATE TABLE uec_raw_data (
    provider_code varchar(3) NOT NULL,
    site_code varchar(5) NOT NULL,
    reporting_date date NOT NULL,
    submission_datetime datetime2 NOT NULL,
    ae_type1 int, 
    ae_type1_resus int,  
    ae_type1_majors int,
    ae_type1_minors int,
    ae_type1_paeds int,
    ae_type2 int,
    ae_type3 int,
    breach_4h_type1 int,
    breach_4h_type1_resus int,
    breach_4h_type1_majors int,
    breach_4h_type1_minors int,
    breach_4h_type1_paeds int,
    breach_4h_type2 int,
    breach_4h_type3 int,
    seen_in_60m_type1 int,
    streamed_to_pc_streaming int,
    breach_4h_pc_streaming int,
    admissions int,
    emergency_admissions int,
    emergency_admissions_ae int,
    discharges int,
    amb_arrivals int,
    amb_handover_delay_30to60 int,
    amb_handover_delay_60plus int,
    trolley_wait_4to12 int,
    trolley_wait_12plus int,
    urg_ops_cancelled int,
    ae_closures int,
    ae_closures_mins int,
    ae_diverts int,
    ae_diverts_duration int,
    ae_diverts_from varchar(300),
    ae_diverts_to varchar(300),
    booked_type1 int,
    breach_4h_booked_type1 int,
    booked_type2 int,
    breach_4h_booked_type2 int,
    booked_other int,
    breach_4h_other int,
    beds_open_core_adult_ga int,
    beds_open_adult_ga_esc int,
    beds_open_adult_ga int,
    beds_occupied_adult_ga int,
    beds_open_core_paeds int,
    beds_open_paeds_esc int,
    beds_open_paeds_ga int,
    beds_open_paeds int,
    bed_stock_open_core_ga int,
    beds_open_ga_esc int,
    beds_open_ga int,
    beds_occupied int,
    beds_closed_dv int,
    beds_closed_unoccupied int,
    beds_closed_dv_adult_ga int,
    beds_closed_dv_adult_ga_unoccupied int,
    beds_closed_dv_paeds_ga int,
    beds_closed_dv_paeds_ga_unoccupied int,
    beds_closed_covid_adult_ga int,
    beds_closed_covid_adult_ga_unoccupied int,
    beds_closed_covid_paeds_ga int,
    beds_closed_covid_paeds_ga_unoccupied int,
    beds_closed_other_adult_ga int,
    beds_closed_other_adult_ga_unoccupied int,
    beds_closed_other_paeds_ga int,
    beds_closed_other_paeds_ga_unoccupied int,
    beds_closed_rsv_paeds_ga int,
    beds_closed_rsv_paeds_ga_unoccupied int,
    ga_influenza int,
    hdu_influenza int,
    influenza_24h int,
    long_stay_7plus int,
    long_stay_14plus int,
    long_stay_21plus int,
    beds_av_adult_cc int,
    beds_av_adult_l3cc int,
    beds_occ_adult_cc int,
    beds_occ_adult_l3cc int,
    beds_av_paeds_ic int,
    beds_occ_paeds_ic int,
    cots_av_neonatal_ic int,
    cots_av_neonatal_l3ic int,
    cots_occ_neonatal_ic int,
    cots_occ_neonatal_l3ic int,
    p1_admission_ordinary_cancellations int,
    p2_admission_ordinary_cancellations int,
    p3_admission_ordinary_cancellations int,
    p4_admission_orindary_cancellations int,
    p1_admission_daycase_cancellations int,
    p2_admission_daycase_cancellations int,
    p3_admission_daycase_cancellations int,
    p4_admission_daycase_cancellations int
    PRIMARY KEY (provider_code, site_code, reporting_date)
)