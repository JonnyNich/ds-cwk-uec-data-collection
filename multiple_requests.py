import requests
import datetime
import random
import string
import aiohttp
import asyncio
import time

from aiohttp import ClientResponse

url = "https://uec-collection.azurewebsites.net/api/HttpTrigger?"
local_url = "http://localhost:7071/api/HttpTrigger"
fails = 0
first_times = 0

def generate_data() -> dict:
    # We will generate our data locally and place it in to an array of size 95
    # Each element in the array corresponds to a data-point that'll be passed to the JSON body
    # The order correlates to the order outlined in the database schema

    # We'll start off by defining some variables to use in our body
    provider_code = "".join(random.sample(string.ascii_uppercase * 3, 3))  # Generate a random 3-letter code
    site_code = "".join([provider_code, "".join(
        random.sample(string.ascii_uppercase * 2, 2))])  # Same as above, but append two more letters
    hospitals = ["Alpha Hospital", "Bravo Hospital", "Charlie Hospital", "Delta Hospital", "Echo Hospital",
                 "Foxtrot Hospital"]

    ae_type1_resus = random.randint(0, 40)
    ae_type1_minors = random.randint(0, 40)
    ae_type1_majors = random.randint(0, 40)
    ae_type1_paeds = random.randint(0, 40)
    ae_type1 = ae_type1_resus + ae_type1_majors + ae_type1_minors + ae_type1_paeds
    breach_4h_type1_resus = ae_type1_resus - 1
    breach_4h_type1_majors = ae_type1_majors - 1
    breach_4h_type1_minors = ae_type1_minors - 1
    breach_4h_type1_paeds = ae_type1_paeds - 1
    breach_4h_type1 = breach_4h_type1_resus + breach_4h_type1_majors + breach_4h_type1_minors + breach_4h_type1_paeds
    ae_type2 = random.randint(0, 2000)
    ae_type3 = random.randint(0, 2000)
    breach_4h_type2 = ae_type2 - 1
    breach_4h_type3 = ae_type3 - 1

    body = {
        "provider_code": provider_code,
        "site_code": site_code,
        "reporting_date": datetime.datetime.now().strftime("%Y/%m/%d"),
        "submission_datetime": datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "ae_diverts_from": ";".join(random.sample(hospitals, 2)),
        "ae_diverts_to": ";".join(random.sample(hospitals, 2)),
        "ae_type1": ae_type1,
        "ae_type1_resus": ae_type1_resus,
        "ae_type1_majors": ae_type1_majors,
        "ae_type1_minors": ae_type1_minors,
        "ae_type1_paeds": ae_type1_paeds,
        "ae_type2": ae_type2,
        "ae_type3": ae_type3,
        "breach_4h_type1": breach_4h_type1,
        "breach_4h_type1_resus": breach_4h_type1_resus,
        "breach_4h_type1_majors": breach_4h_type1_majors,
        "breach_4h_type1_minors": breach_4h_type1_minors,
        "breach_4h_type1_paeds": breach_4h_type1_paeds,
        "breach_4h_type2": breach_4h_type2,
        "breach_4h_type3": breach_4h_type3,
        "seen_in_60m_type1": random.randint(0, 2000),
        "streamed_to_pc_streaming": random.randint(0, 2000),
        "breach_4h_pc_streaming": random.randint(0, 2000),
        "admissions": random.randint(0, 2000),
        "emergency_admissions": random.randint(0, 2000),
        "emergency_admissions_ae": random.randint(0, 2000),
        "discharges": random.randint(0, 2000),
        "amb_arrivals": random.randint(0, 2000),
        "amb_handover_delay_30to60": random.randint(0, 2000),
        "amb_handover_delay_60plus": random.randint(0, 2000),
        "trolley_wait_4to12": random.randint(0, 2000),
        "trolley_wait_12plus": random.randint(0, 2000),
        "urg_ops_cancelled": random.randint(0, 2000),
        "ae_closures": random.randint(0, 2000),
        "ae_closures_mins": random.randint(0, 2000),
        "ae_diverts": random.randint(0, 2000),
        "ae_diverts_duration": random.randint(0, 2000),
        "booked_type1": random.randint(0, 2000),
        "breach_4h_booked_type1": random.randint(0, 2000),
        "booked_type2": random.randint(0, 2000),
        "breach_4h_booked_type2": random.randint(0, 2000),
        "booked_other": random.randint(0, 2000),
        "breach_4h_other": random.randint(0, 2000),
        "beds_open_core_adult_ga": random.randint(0, 2000),
        "beds_open_adult_ga_esc": random.randint(0, 2000),
        "beds_open_adult_ga": random.randint(0, 2000),
        "beds_occupied_adult_ga": random.randint(0, 2000),
        "beds_open_core_paeds": random.randint(0, 2000),
        "beds_open_paeds_esc": random.randint(0, 2000),
        "beds_open_paeds_ga": random.randint(0, 2000),
        "beds_open_paeds": random.randint(0, 2000),
        "bed_stock_open_core_ga": random.randint(0, 2000),
        "beds_open_ga_esc": random.randint(0, 2000),
        "beds_open_ga": random.randint(0, 2000),
        "beds_occupied": random.randint(0, 2000),
        "beds_closed_dv": random.randint(0, 2000),
        "beds_closed_unoccupied": random.randint(0, 2000),
        "beds_closed_dv_adult_ga": random.randint(0, 2000),
        "beds_closed_dv_adult_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_dv_paeds_ga": random.randint(0, 2000),
        "beds_closed_dv_paeds_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_covid_adult_ga": random.randint(0, 2000),
        "beds_closed_covid_adult_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_covid_paeds_ga": random.randint(0, 2000),
        "beds_closed_covid_paeds_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_other_adult_ga": random.randint(0, 2000),
        "beds_closed_other_adult_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_other_paeds_ga": random.randint(0, 2000),
        "beds_closed_other_paeds_ga_unoccupied": random.randint(0, 2000),
        "beds_closed_rsv_paeds_ga": random.randint(0, 2000),
        "beds_closed_rsv_paeds_ga_unoccupied": random.randint(0, 2000),
        "ga_influenza": random.randint(0, 2000),
        "hdu_influenza": random.randint(0, 2000),
        "influenza_24h": random.randint(0, 2000),
        "long_stay_7plus": random.randint(0, 2000),
        "long_stay_14plus": random.randint(0, 2000),
        "long_stay_21plus": random.randint(0, 2000),
        "beds_av_adult_cc": random.randint(0, 2000),
        "beds_av_adult_l3cc": random.randint(0, 2000),
        "beds_occ_adult_cc": random.randint(0, 2000),
        "beds_occ_adult_l3cc": random.randint(0, 2000),
        "beds_av_paeds_ic": random.randint(0, 2000),
        "beds_occ_paeds_ic": random.randint(0, 2000),
        "cots_av_neonatal_ic": random.randint(0, 2000),
        "cots_av_neonatal_l3ic": random.randint(0, 2000),
        "cots_occ_neonatal_ic": random.randint(0, 2000),
        "cots_occ_neonatal_l3ic": random.randint(0, 2000),
        "p1_admission_ordinary_cancellations": random.randint(0, 2000),
        "p2_admission_ordinary_cancellations": random.randint(0, 2000),
        "p3_admission_ordinary_cancellations": random.randint(0, 2000),
        "p4_admission_orindary_cancellations": random.randint(0, 2000),
        "p1_admission_daycase_cancellations": random.randint(0, 2000),
        "p2_admission_daycase_cancellations": random.randint(0, 2000),
        "p3_admission_daycase_cancellations": random.randint(0, 2000),
        "p4_admission_daycase_cancellations": random.randint(0, 2000),
    }

    return body

async def send_data(post_url):
    global fails, first_times
    async with aiohttp.ClientSession() as session:
        attempts = 0
        async with session.post(post_url, json=generate_data()) as response:
            attempts += 1
            code = response.status
            # Retry four more times, as deadlocking can occur
            if code == 200:
                first_times += 1
            while code == 500 and attempts < 5:
                fails += 1
                attempts += 1
                async with session.post(post_url, json=generate_data()) as retry_response:
                    code = retry_response.status

async def main():
    # Local
    start = time.time()
    async with asyncio.TaskGroup() as tg:
        for i in range (200):
            tg.create_task(send_data(local_url))
    print ("Time: " + str(round(time.time() - start, 2)))
    print (f"First-time successes: {str(first_times)}")
    print (f"Fails: {str(fails)}")

asyncio.run(main())