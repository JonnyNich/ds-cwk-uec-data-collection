from io import BufferedIOBase

import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

"""
@app.timer_trigger(
    schedule="0 30 9 * * *", # At 9:30am each day
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
"""
@app.function_name(name="SaveRecord")
@app.route(route="save/{date}")
@app.sql_input(
    arg_name="dataIn",
    command_text="select * from dbo.uec_raw_data where reporting_date = @rep_date",
    parameters="@rep_date={date}",
    command_type="Text",
    connection_string_setting="SqlConnectionString"
)
# def CollateData(mytimer : func.TimerRequest, dataIn : func.SqlRowList) -> None:
def CollateData(req : func.HttpRequest, dataIn : func.SqlRowList) -> func.HttpResponse:
    # Put the data from our SQL query and put it into a list
    # For context, rows is a list of dictionaries
    rows = list(map(lambda r: json.loads(r.to_json()), dataIn))

    # Now, we prepare the data in a csv-style string fomrat
    if len(rows) == 0:
        return func.HttpResponse("No rows found :(")
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
    # blob_name = "uec-daily-report-" + str(req.get_json("date")) + ".csv"
    blob_name = "bleh.csv"
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    # Write data to CSV
    blob_client.upload_blob(csv_string_out, blob_type="BlockBlob", overwrite=True)

    logging.info(csv_string_out)

    return func.HttpResponse("Done")

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

    uploadData.set(func.SqlRow({
        "provider_code": req.get_json().get("provider_code"),
        "site_code": req.get_json().get("site_code"),
        "reporting_date": req.get_json().get("reporting_date"),
        "submission_datetime": req.get_json().get("submission_datetime"),
        "ae_type1": req.get_json().get("ae_type1"),
        "ae_type1_resus": req.get_json().get("ae_type1_resus"),
        "ae_type1_majors": req.get_json().get("ae_type1_majors"),
        "ae_type1_minors": req.get_json().get("ae_type1_minors"),
        "ae_type1_paeds": req.get_json().get("ae_type1_paeds"),
        "ae_type2": req.get_json().get("ae_type2"),
        "ae_type3": req.get_json().get("ae_type3"),
        "breach_4h_type1": req.get_json().get("breach_4h_type1"),
        "breach_4h_type1_resus": req.get_json().get("breach_4h_type1_resus"),
        "breach_4h_type1_majors": req.get_json().get("breach_4h_type1_majors"),
        "breach_4h_type1_minors": req.get_json().get("breach_4h_type1_minors"),
        "breach_4h_type1_paeds": req.get_json().get("breach_4h_type1_paeds"),
        "breach_4h_type2": req.get_json().get("breach_4h_type2"),
        "breach_4h_type3": req.get_json().get("breach_4h_type3"),
        "seen_in_60m_type1": req.get_json().get("seen_in_60m_type1"),
        "streamed_to_pc_streaming": req.get_json().get("streamed_to_pc_streaming"),
        "breach_4h_pc_streaming": req.get_json().get("breach_4h_pc_streaming"),
        "admissions": req.get_json().get("admissions"),
        "emergency_admissions": req.get_json().get("emergency_admissions"),
        "emergency_admissions_ae": req.get_json().get("emergency_admissions_ae"),
        "discharges": req.get_json().get("discharges"),
        "amb_arrivals": req.get_json().get("amb_arrivals"),
        "amb_handover_delay_30to60": req.get_json().get("amb_handover_delay_30to60"),
        "amb_handover_delay_60plus": req.get_json().get("amb_handover_delay_60plus"),
        "trolley_wait_4to12": req.get_json().get("trolley_wait_4to12"),
        "trolley_wait_12plus": req.get_json().get("trolley_wait_12plus"),
        "urg_ops_cancelled": req.get_json().get("urg_ops_cancelled"),
        "ae_closures": req.get_json().get("ae_closures"),
        "ae_closures_mins": req.get_json().get("ae_closures_mins"),
        "ae_diverts": req.get_json().get("ae_diverts"),
        "ae_diverts_duration": req.get_json().get("ae_diverts_duration"),
        "ae_diverts_from": req.get_json().get("ae_diverts_from"),
        "ae_diverts_to": req.get_json().get("ae_diverts_to"),
        "booked_type1": req.get_json().get("booked_type1"),
        "breach_4h_booked_type1": req.get_json().get("breach_4h_booked_type1"),
        "booked_type2": req.get_json().get("booked_type2"),
        "breach_4h_booked_type2": req.get_json().get("breach_4h_booked_type2"),
        "booked_other": req.get_json().get("booked_other"),
        "breach_4h_other": req.get_json().get("breach_4h_other"),
        "beds_open_core_adult_ga": req.get_json().get("beds_open_core_adult_ga"),
        "beds_open_adult_ga_esc": req.get_json().get("beds_open_adult_ga_esc"),
        "beds_open_adult_ga": req.get_json().get("beds_open_adult_ga"),
        "beds_occupied_adult_ga": req.get_json().get("beds_occupied_adult_ga"),
        "beds_open_core_paeds": req.get_json().get("beds_open_core_paeds"),
        "beds_open_paeds_esc": req.get_json().get("beds_open_paeds_esc"),
        "beds_open_paeds_ga": req.get_json().get("beds_open_paeds_ga"),
        "beds_open_paeds": req.get_json().get("beds_open_paeds"),
        "bed_stock_open_core_ga": req.get_json().get("bed_stock_open_core_ga"),
        "beds_open_ga_esc": req.get_json().get("beds_open_ga_esc"),
        "beds_open_ga": req.get_json().get("beds_open_ga"),
        "beds_occupied": req.get_json().get("beds_occupied"),
        "beds_closed_dv": req.get_json().get("beds_closed_dv"),
        "beds_closed_unoccupied": req.get_json().get("beds_closed_unoccupied"),
        "beds_closed_dv_adult_ga": req.get_json().get("beds_closed_dv_adult_ga"),
        "beds_closed_dv_adult_ga_unoccupied": req.get_json().get("beds_closed_dv_adult_ga_unoccupied"),
        "beds_closed_dv_paeds_ga": req.get_json().get("beds_closed_dv_paeds_ga"),
        "beds_closed_dv_paeds_ga_unoccupied": req.get_json().get("beds_closed_dv_paeds_ga_unoccupied"),
        "beds_closed_covid_adult_ga": req.get_json().get("beds_closed_covid_adult_ga"),
        "beds_closed_covid_adult_ga_unoccupied": req.get_json().get("beds_closed_covid_adult_ga_unoccupied"),
        "beds_closed_covid_paeds_ga": req.get_json().get("beds_closed_covid_paeds_ga"),
        "beds_closed_covid_paeds_ga_unoccupied": req.get_json().get("beds_closed_covid_paeds_ga_unoccupied"),
        "beds_closed_other_adult_ga": req.get_json().get("beds_closed_other_adult_ga"),
        "beds_closed_other_adult_ga_unoccupied": req.get_json().get("beds_closed_other_adult_ga_unoccupied"),
        "beds_closed_other_paeds_ga": req.get_json().get("beds_closed_other_paeds_ga"),
        "beds_closed_other_paeds_ga_unoccupied": req.get_json().get("beds_closed_other_paeds_ga_unoccupied"),
        "beds_closed_rsv_paeds_ga": req.get_json().get("beds_closed_rsv_paeds_ga"),
        "beds_closed_rsv_paeds_ga_unoccupied": req.get_json().get("beds_closed_rsv_paeds_ga_unoccupied"),
        "ga_influenza": req.get_json().get("ga_influenza"),
        "hdu_influenza": req.get_json().get("hdu_influenza"),
        "influenza_24h": req.get_json().get("influenza_24h"),
        "long_stay_7plus": req.get_json().get("long_stay_7plus"),
        "long_stay_14plus": req.get_json().get("long_stay_14plus"),
        "long_stay_21plus": req.get_json().get("long_stay_21plus"),
        "beds_av_adult_cc": req.get_json().get("beds_av_adult_cc"),
        "beds_av_adult_l3cc": req.get_json().get("beds_av_adult_l3cc"),
        "beds_occ_adult_cc": req.get_json().get("beds_occ_adult_cc"),
        "beds_occ_adult_l3cc": req.get_json().get("beds_occ_adult_l3cc"),
        "beds_av_paeds_ic": req.get_json().get("beds_av_paeds_ic"),
        "beds_occ_paeds_ic": req.get_json().get("beds_occ_paeds_ic"),
        "cots_av_neonatal_ic": req.get_json().get("cots_av_neonatal_ic"),
        "cots_av_neonatal_l3ic": req.get_json().get("cots_av_neonatal_l3ic"),
        "cots_occ_neonatal_ic": req.get_json().get("cots_occ_neonatal_ic"),
        "cots_occ_neonatal_l3ic": req.get_json().get("cots_occ_neonatal_l3ic"),
        "p1_admission_ordinary_cancellations": req.get_json().get("p1_admission_ordinary_cancellations"),
        "p2_admission_ordinary_cancellations": req.get_json().get("p2_admission_ordinary_cancellations"),
        "p3_admission_ordinary_cancellations": req.get_json().get("p3_admission_ordinary_cancellations"),
        "p4_admission_orindary_cancellations": req.get_json().get("p4_admission_orindary_cancellations"),
        "p1_admission_daycase_cancellations": req.get_json().get("p1_admission_daycase_cancellations"),
        "p2_admission_daycase_cancellations": req.get_json().get("p2_admission_daycase_cancellations"),
        "p3_admission_daycase_cancellations": req.get_json().get("p3_admission_daycase_cancellations"),
        "p4_admission_daycase_cancellations": req.get_json().get("p4_admission_daycase_cancellations"),
    }))
    
    return func.HttpResponse(f"Data successfully uploaded.")