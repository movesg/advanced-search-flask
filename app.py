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


# Data Format function

def format_data(result,job_id):
    format_result=[]
    for item in result:
        fields = item.get("fields")
        format_result.append({"Candidate ID": str(fields.get("Candidate ID")),"Job ID":job_id})
    return format_result

def convert_json_to_text(list):
    listreturn = []
    for item in list:
        x = item.replace("\xa0", "")
        listreturn = x.split(",")
    return listreturn
        
def search_matched_results(search_pqe,search_jobtags,search_location, job_id):
    #check if exist
    if(len(search_pqe)<=0): #if no values
        search_pqe.append("")
    if(len(search_jobtags)<=0):
        search_jobtags.append("")
    if(len(search_location)<=0):
        search_location.append("")

    print(len(search_pqe))
    print(len(search_jobtags))
    print(len(search_location))

    list_results=[]
    for pqe in search_pqe:
        for job in search_jobtags:
            for loc in search_location:
                Formula = fm.AND(fm.match({"PQE": pqe}),fm.AND(fm.FIND(fm.STR_VALUE(job),fm.FIELD("Job Tags")),fm.FIND(fm.STR_VALUE(loc),fm.FIELD("Location"))))
                list_results += table_candidates.all(formula=Formula)
                #print(list_results)
    list_results=format_data(list_results,job_id)
    return list_results


def match_pipelineID(corr_list):
    pipelineID_list = []
    return_list =[]
    for corr in corr_list:
        pipelineID = corr.get("Candidate ID")+corr.get("Job ID")
        print(pipelineID)
        res = table_post.all(formula = fm.match({"Pipeline ID":str(pipelineID)}))
        if(len(res)==0): # if pipeline ID doesnt exists in table
            return_list.append(corr)
        else:
            print("DUPLICATE DATA FOUND:")
    return return_list

@app.route('/my_webhook', methods=['POST'])
def return_response():
    print("____________SEARCH STARTED___________")
    correct_results=[]

    print("KEYWORD SEARCHES: \n---------------")
    search_pqe = request.form.getlist('PQE')
    search_pqe = convert_json_to_text(search_pqe)
    print(search_pqe)

    search_jobtags = request.form.getlist('jobtags')
    search_jobtags = convert_json_to_text(search_jobtags)
    print(search_jobtags)

    search_location = request.form.getlist('location')
    search_location = convert_json_to_text(search_location)
    print(search_location)

    job_id = request.form.get("job_id")

    print("------------------\nGETTING RESULTS: \n------------------\n")

    correct_results = search_matched_results(search_pqe,search_jobtags,search_location,job_id)

    print(len(correct_results))
    correct_results=match_pipelineID(correct_results)

    print("# RESULTS FOUND: " + str(len(correct_results))+"\n---------------")

    print("POSTING TO AIRTABLE...")
    table_post.batch_create(correct_results, typecast=True)
    print(str(len(correct_results)) +" RESULTS FOUND\nWITH KEYWORDS:\n"+str(search_pqe)+"\n"+str(search_jobtags)+"\n"+str(search_location)+"\n========END========\n")
    ## Do something with the request.json data.
    return Response(status=200)

if __name__ == "__main__": app.run()
