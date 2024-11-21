from io import BufferedIOBase

import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="CollateData")
# @app.route(route="save")
@app.sql_input(
    arg_name="dataIn",
    command_text="select * from dbo.uec_raw_data where reporting_date = \'" + datetime.datetime.now().strftime("%Y/%m/%d") + "\'",
    command_type="Text",
    connection_string_setting="SqlConnectionString"
)
@app.timer_trigger(
    schedule="0 30 9 * * *", # At 9:30am each day
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
# HTTP Trigger (FOR TESTING) - def CollateData(req : func.HttpRequest, dataIn : func.SqlRowList) -> func.HttpResponse:
def CollateData(mytimer : func.TimerRequest, dataIn : func.SqlRowList) -> None:
    # Put the data from our SQL query and put it into a list
    # For context, rows is a list of dictionaries
    rows = list(map(lambda r: json.loads(r.to_json()), dataIn))

    # Now, we prepare the data in a csv-style string fomrat
    headers = list(rows[0].keys())
    data = []
    for row in rows:
        data.append(",".join(map(str, list(row.values()))))

    csv_string_out = ",".join(headers) + "\n" + "\n".join(data)

    # Set up our connection to the storage container
    blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsMyStorage"])
    container_name = "spreadsheet-store"
    container_client = blob_service_client.get_container_client(container_name)

    # Declare the blob's name and establish the client
    blob_name = "uec-daily-report-" + datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    # Write data to CSV
    blob_client.upload_blob(csv_string_out, blob_type="BlockBlob", overwrite=True)

    logging.info(csv_string_out)

@app.function_name(name="UploadData")
@app.route(route="HttpTrigger", auth_level=func.AuthLevel.ANONYMOUS)
@app.generic_output_binding(
    arg_name="uploadData",
    type="sql",
    CommandText="dbo.uec_raw_data",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
def UploadData(req: func.HttpRequest, uploadData : func.Out[func.SqlRow]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    req_json = req.get_json()
    
    # Data validation checks
    # Only included a&e attendance data for proof-of-concept
    invalid_data = False
    if int(req_json.get("ae_type1")) != int(req_json.get("ae_type1_resus")) + int(req_json.get("ae_type1_majors")) + int(req_json.get("ae_type1_minors")) + int(req_json.get("ae_type1_paeds")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1")) > int(req_json.get("ae_type1")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1")) != int(req_json.get("breach_4h_type1_resus")) + int(req_json.get("breach_4h_type1_majors")) + int(req_json.get("breach_4h_type1_minors")) + int(req_json.get("breach_4h_type1_paeds")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1_resus")) > int(req_json.get("ae_type1_resus")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1_majors")) > int(req_json.get("ae_type1_majors")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1_minors")) > int(req_json.get("ae_type1_minors")):
        invalid_data = True
    if int(req_json.get("breach_4h_type1_paeds")) > int(req_json.get("ae_type1_paeds")):
        invalid_data = True
    if int(req_json.get("breach_4h_type2")) > int(req_json.get("ae_type2")):
        invalid_data = True
    if int(req_json.get("breach_4h_type3")) > int(req_json.get("ae_type3")):
        invalid_data = True

    if invalid_data == False:
        uploadData.set(func.SqlRow({
            "provider_code": req_json.get("provider_code"),
            "site_code": req_json.get("site_code"),
            "reporting_date": req_json.get("reporting_date"),
            "submission_datetime": req_json.get("submission_datetime"),
            "ae_type1": req_json.get("ae_type1"),
            "ae_type1_resus": req_json.get("ae_type1_resus"),
            "ae_type1_majors": req_json.get("ae_type1_majors"),
            "ae_type1_minors": req_json.get("ae_type1_minors"),
            "ae_type1_paeds": req_json.get("ae_type1_paeds"),
            "ae_type2": req_json.get("ae_type2"),
            "ae_type3": req_json.get("ae_type3"),
            "breach_4h_type1": req_json.get("breach_4h_type1"),
            "breach_4h_type1_resus": req_json.get("breach_4h_type1_resus"),
            "breach_4h_type1_majors": req_json.get("breach_4h_type1_majors"),
            "breach_4h_type1_minors": req_json.get("breach_4h_type1_minors"),
            "breach_4h_type1_paeds": req_json.get("breach_4h_type1_paeds"),
            "breach_4h_type2": req_json.get("breach_4h_type2"),
            "breach_4h_type3": req_json.get("breach_4h_type3"),
            "seen_in_60m_type1": req_json.get("seen_in_60m_type1"),
            "streamed_to_pc_streaming": req_json.get("streamed_to_pc_streaming"),
            "breach_4h_pc_streaming": req_json.get("breach_4h_pc_streaming"),
            "admissions": req_json.get("admissions"),
            "emergency_admissions": req_json.get("emergency_admissions"),
            "emergency_admissions_ae": req_json.get("emergency_admissions_ae"),
            "discharges": req_json.get("discharges"),
            "amb_arrivals": req_json.get("amb_arrivals"),
            "amb_handover_delay_30to60": req_json.get("amb_handover_delay_30to60"),
            "amb_handover_delay_60plus": req_json.get("amb_handover_delay_60plus"),
            "trolley_wait_4to12": req_json.get("trolley_wait_4to12"),
            "trolley_wait_12plus": req_json.get("trolley_wait_12plus"),
            "urg_ops_cancelled": req_json.get("urg_ops_cancelled"),
            "ae_closures": req_json.get("ae_closures"),
            "ae_closures_mins": req_json.get("ae_closures_mins"),
            "ae_diverts": req_json.get("ae_diverts"),
            "ae_diverts_duration": req_json.get("ae_diverts_duration"),
            "ae_diverts_from": req_json.get("ae_diverts_from"),
            "ae_diverts_to": req_json.get("ae_diverts_to"),
            "booked_type1": req_json.get("booked_type1"),
            "breach_4h_booked_type1": req_json.get("breach_4h_booked_type1"),
            "booked_type2": req_json.get("booked_type2"),
            "breach_4h_booked_type2": req_json.get("breach_4h_booked_type2"),
            "booked_other": req_json.get("booked_other"),
            "breach_4h_other": req_json.get("breach_4h_other"),
            "beds_open_core_adult_ga": req_json.get("beds_open_core_adult_ga"),
            "beds_open_adult_ga_esc": req_json.get("beds_open_adult_ga_esc"),
            "beds_open_adult_ga": req_json.get("beds_open_adult_ga"),
            "beds_occupied_adult_ga": req_json.get("beds_occupied_adult_ga"),
            "beds_open_core_paeds": req_json.get("beds_open_core_paeds"),
            "beds_open_paeds_esc": req_json.get("beds_open_paeds_esc"),
            "beds_open_paeds_ga": req_json.get("beds_open_paeds_ga"),
            "beds_open_paeds": req_json.get("beds_open_paeds"),
            "bed_stock_open_core_ga": req_json.get("bed_stock_open_core_ga"),
            "beds_open_ga_esc": req_json.get("beds_open_ga_esc"),
            "beds_open_ga": req_json.get("beds_open_ga"),
            "beds_occupied": req_json.get("beds_occupied"),
            "beds_closed_dv": req_json.get("beds_closed_dv"),
            "beds_closed_unoccupied": req_json.get("beds_closed_unoccupied"),
            "beds_closed_dv_adult_ga": req_json.get("beds_closed_dv_adult_ga"),
            "beds_closed_dv_adult_ga_unoccupied": req_json.get("beds_closed_dv_adult_ga_unoccupied"),
            "beds_closed_dv_paeds_ga": req_json.get("beds_closed_dv_paeds_ga"),
            "beds_closed_dv_paeds_ga_unoccupied": req_json.get("beds_closed_dv_paeds_ga_unoccupied"),
            "beds_closed_covid_adult_ga": req_json.get("beds_closed_covid_adult_ga"),
            "beds_closed_covid_adult_ga_unoccupied": req_json.get("beds_closed_covid_adult_ga_unoccupied"),
            "beds_closed_covid_paeds_ga": req_json.get("beds_closed_covid_paeds_ga"),
            "beds_closed_covid_paeds_ga_unoccupied": req_json.get("beds_closed_covid_paeds_ga_unoccupied"),
            "beds_closed_other_adult_ga": req_json.get("beds_closed_other_adult_ga"),
            "beds_closed_other_adult_ga_unoccupied": req_json.get("beds_closed_other_adult_ga_unoccupied"),
            "beds_closed_other_paeds_ga": req_json.get("beds_closed_other_paeds_ga"),
            "beds_closed_other_paeds_ga_unoccupied": req_json.get("beds_closed_other_paeds_ga_unoccupied"),
            "beds_closed_rsv_paeds_ga": req_json.get("beds_closed_rsv_paeds_ga"),
            "beds_closed_rsv_paeds_ga_unoccupied": req_json.get("beds_closed_rsv_paeds_ga_unoccupied"),
            "ga_influenza": req_json.get("ga_influenza"),
            "hdu_influenza": req_json.get("hdu_influenza"),
            "influenza_24h": req_json.get("influenza_24h"),
            "long_stay_7plus": req_json.get("long_stay_7plus"),
            "long_stay_14plus": req_json.get("long_stay_14plus"),
            "long_stay_21plus": req_json.get("long_stay_21plus"),
            "beds_av_adult_cc": req_json.get("beds_av_adult_cc"),
            "beds_av_adult_l3cc": req_json.get("beds_av_adult_l3cc"),
            "beds_occ_adult_cc": req_json.get("beds_occ_adult_cc"),
            "beds_occ_adult_l3cc": req_json.get("beds_occ_adult_l3cc"),
            "beds_av_paeds_ic": req_json.get("beds_av_paeds_ic"),
            "beds_occ_paeds_ic": req_json.get("beds_occ_paeds_ic"),
            "cots_av_neonatal_ic": req_json.get("cots_av_neonatal_ic"),
            "cots_av_neonatal_l3ic": req_json.get("cots_av_neonatal_l3ic"),
            "cots_occ_neonatal_ic": req_json.get("cots_occ_neonatal_ic"),
            "cots_occ_neonatal_l3ic": req_json.get("cots_occ_neonatal_l3ic"),
            "p1_admission_ordinary_cancellations": req_json.get("p1_admission_ordinary_cancellations"),
            "p2_admission_ordinary_cancellations": req_json.get("p2_admission_ordinary_cancellations"),
            "p3_admission_ordinary_cancellations": req_json.get("p3_admission_ordinary_cancellations"),
            "p4_admission_orindary_cancellations": req_json.get("p4_admission_orindary_cancellations"),
            "p1_admission_daycase_cancellations": req_json.get("p1_admission_daycase_cancellations"),
            "p2_admission_daycase_cancellations": req_json.get("p2_admission_daycase_cancellations"),
            "p3_admission_daycase_cancellations": req_json.get("p3_admission_daycase_cancellations"),
            "p4_admission_daycase_cancellations": req_json.get("p4_admission_daycase_cancellations"),
        }))

        return func.HttpResponse(f"Data successfully uploaded.")
    else:
        return func.HttpResponse("Bad data :(")