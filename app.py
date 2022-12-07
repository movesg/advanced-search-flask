import pyairtable
import pandas as pd
from pyairtable import Table
import os
import pyairtable.formulas as fm
from dotenv import load_dotenv
from flask import Flask, request, Response
app = Flask(__name__)
import tqdm

load_dotenv()
# CANDIDATES - CANDIDATES (GET)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")

# Pipeline - Pipeline (POST)
POST_BASE_ID = os.getenv("POST_BASE_ID")
POST_TABLE_ID = os.getenv("POST_TABLE_ID")

table_candidates = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)
table_post = Table(AIRTABLE_API_KEY, POST_BASE_ID, POST_TABLE_ID)

# Search for


# Data Format function
def format_data(result):
    fields = result.get("fields")
    format_result = {
        "Candidate ID": str(fields.get("Candidate ID"))
    }
    return format_result

def searchby_pqe(search_pqe):
    for pqe in search_pqe:
        result = table_candidates.all(formula=fm.match({"PQE": pqe}))
        # print("results: "+str(len(result)))
        return result


def searchby_job(search_jobtags):
    for job in search_jobtags:
        result = table_candidates.all(
            formula=fm.FIND(fm.STR_VALUE("Data Privacy & Cybersecurity"), fm.FIELD("Job Tags")))
        # print("results: "+str(len(result)))
        return result


def searchby_location(search_location):
    for location in search_location:
        result = table_candidates.all(formula=fm.FIND(fm.STR_VALUE(location), fm.FIELD("Location")))
        # print("results: "+str(len(result)))
        return result


def filter_by_pqe(result, search_pqe):
    pqe = result.get("fields").get("PQE")  # get job tag from result0
    if (len(search_pqe) == 0):
        pqe_match = True
    elif (pqe == None):
        pqe_match = False
    else:
        for search in search_pqe:  # tag to search
            # print(len(search_jobtags))
            pqe_match = all(n in pqe for n in search.split())
    return pqe_match


def filter_by_job(result, search_jobtags):
    job_tags = result.get("fields").get("Job Tags")  # get job tag from result0
    if (len(search_jobtags) == 0):
        job_match = True
    elif (job_tags == None):
        job_match = False
    else:
        for search in search_jobtags:  # tag to search
            # print(len(search_jobtags))
            job_match = all(n in job_tags for n in search.split())
    return job_match


def filter_by_location(result, search_location):

    location_tags = result.get("fields").get("Location")  # get location tag from result0
    if (len(search_location) == 0):
        match = True
    elif (location_tags == None):
        match = False
    else:
        for search1 in search_location:  # for each location search keyword
            match = all(m in location_tags for m in search1.split())
    return match
def convert_json_to_text(list):
    listreturn = []
    for item in list:
        x = item.replace("/xa0", " ")
        listreturn.append(x)
    return listreturn
        

@app.route('/my_webhook', methods=['POST'])
def return_response():
    initial_search_results = []
    search_pqe = request.form.getlist('PQE')
    print(search_pqe)
    search_pqe = convert_json_to_text(search_pqe)
    print(search_pqe)

    search_jobtags = request.form.getlist('jobtags')
    print(search_jobtags)
    search_jobtags = convert_json_to_text(search_jobtags)
    print(search_jobtags)

    search_location = request.form.getlist('location')
    print(search_location)
    search_location = convert_json_to_text(search_location)
    print(search_location)

    x = str.replace
    if (len(search_pqe) > 0):  # if search_pqe exists
        initial_search_results += searchby_pqe(search_pqe)
        print("search pqe")
    elif (len(search_jobtags) > 0):
        print("search job")
        initial_search_results += searchby_job(search_jobtags)
    elif (len(search_location) > 0):
        print("search loc")
        initial_search_results += searchby_location(search_location)

    # print((filter_pqe_results))
    print(len(initial_search_results))
    # print(len(filter_pqe_results))


    correct_results = []

    i = 0
    print("\nStart filtering ... \n")

    for result0 in initial_search_results:  # for each result
        i += 1
        pqe_match = filter_by_pqe(result0, search_pqe)
        if (pqe_match):

            job_match = filter_by_job(result0, search_jobtags)
            if (job_match):  # if no jobtags or contains jobtag
                # print("job match")

                location_match = filter_by_location(result0, search_location)
                if (location_match):  # if no location tag or contains loc tag

                    data = format_data(result0)
                    correct_results.append(data)
                    # print("MATCH")
    print(i)

    print("Matching results: \n------------------\n")
    print(len(correct_results))

    table_post.batch_create(correct_results, typecast=True)
    ## Do something with the request.json data.
    return Response(status=200)

if __name__ == "__main__": app.run()
