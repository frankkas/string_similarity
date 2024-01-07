import pymongo
import time
import datetime
from pathlib import Path
import json
import argparse

# calculate cosinus similaritiy for 2 samples
def calculate(file_1, file_2):
    query = [{
              "$project":
              {
                  "_id": 1,
                  "string": 1,
                  "files": 1,
                  "numberOfFiles": { "$size": "$files" }
              }
        },
        {
            "$match":
            {
                "files.file":
                {
                    "$in": [file_1, file_2]
                }
            }
        },
        { "$unwind": '$files'},
        { "$match": {"files.file": {"$in": [file_1, file_2]}}},
        {
            "$project" :
            {
                "_id" : 1,
                "string" : 1,
                "files.file" : 1,
                "files.tf" : 1,
                "file1":
                {
                    "$cond":
                    {
                        "if": {"$eq": [file_1, "$files.file"]},
                        "then": "$files.tf",
                        "else": 0
                    }
                },
                "file2":
                {
                    "$cond":
                    {
                        "if": {"$eq": [file_2, "$files.file"]},
                        "then": "$files.tf",
                        "else": 0
                    }
                },
                "numberOfFiles": 1
            }
        },
        {
            "$group":
            {
                "_id": '$_id',
                "string": {"$first": "$string"},
                "file1": { "$max": "$file1" },
                "file2": { "$max": "$file2" },
                "numberOfFiles": {"$first": "$numberOfFiles"}
            }
        },
        {
            "$project":
            {
                "_id": 1,
                "string": 1,
                "file1": 1,
                "file2": 1,
                "numberOfFiles": 1,
                "idf":
                {
                    "$log10":
                    {
                        "$divide": [all_files_len, {"$sum": [1, "$numberOfFiles"]}]
                    }
                }
            }
        },
        {
            "$project":
            {
                "_id": 1,
                "string": 1,
                "file1_tfidf": {"$multiply": ["$file1", "$idf"]},
                "file2_tfidf": {"$multiply": ["$file2", "$idf"]},
                "file1": 1,
                "file2": 1,
                "numberOfFiles": 1,
                "idf": 1,
                "file1_file2_tfidf": {"$multiply": ["$file1", "$idf", "$file2", "$idf"]}
             }
        },    
        {
            "$group":
            {
                "_id": None,
                "sum_file1_file2": { "$sum": "$file1_file2_tfidf" },
                "sum_pow_file1_tfidf": {"$sum": {"$pow": ["$file1_tfidf", 2]}},
                "sum_pow_file2_tfidf": {"$sum": {"$pow": ["$file2_tfidf", 2]}},
            }
        },
        {
            "$project":
            {
                "_id": 0,
                "sum_file1_file2": 1,
                "sum_pow_file1_tfidf": 1,
                "sum_pow_file2_tfidf": 1,
                "cos_tfidf":
                {
                    "$divide":
                    [
                        "$sum_file1_file2",
                        {
                            "$multiply":
                            [
                                {"$sqrt": "$sum_pow_file1_tfidf"},
                                {"$sqrt": "$sum_pow_file2_tfidf"}
                            ]
                        }
                    ]
                }
            }
        }]

#    calc_result = {"filename": file_2}
   
    result_cos_sim = strings_col.aggregate(query)

    for i in result_cos_sim:
        print(i["cos_tfidf"])
        return i["cos_tfidf"]
    



##########################################################################################################
##########################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument("--filename", type=str, help="Get Decoded Similarity for following <filename>")
parser.add_argument("--family", type=str, help="Get Decoded Similarity for following <family>")
parser.add_argument("--mode", type=str, default="all", choices=["file", "family", "all"], help="Get Decoded Similarity for all known samples")

args = parser.parse_args()


# connect to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:9000/")
print(myclient)
mydb = myclient["stringsdatabase"]
files_col = mydb["samples"]
strings_col = mydb["strings"]
cos_sim_col = mydb["cos_sim"]

# set sample lists for outer and inner loop depending on mode
if (args.mode == "file"):
    samples_outerloop_cursor = files_col.find({"filename": args.filename})
    samples_innerloop_cursor = files_col.find({})
        
elif (args.mode == "family"):
    samples_outerloop_cursor = files_col.find({"family": args.family})
    samples_innerloop_cursor = files_col.find({})

elif ("all"):
    samples_outerloop_cursor = files_col.find({})
    samples_innerloop_cursor = files_col.find({})


samples_outerloop = list(samples_outerloop_cursor)
samples_innerloop = list(samples_innerloop_cursor)

all_samples_cursor = files_col.find({})
all_files_len = len(list(all_samples_cursor))

all_time_start = time.time()



# loop over all samples and calculate similarity if not already calculated
for main_sample in samples_outerloop:
    file_1 = main_sample["filename"]
    print(file_1)
    timestamp_file_1 = main_sample["string_timestamp"]

    for sample in samples_innerloop:
        file_2 = sample["filename"]
        print(file_2)
        timestamp_file_2 = sample["string_timestamp"]
        
        
        myquery = {
            '$or': 
            [
                {'file1': file_1, 'file2': file_2},
                {'file1': file_2, 'file2': file_1}
            ],
            '$and':
            [
                {'timestamp' : { '$gt' : timestamp_file_1}},
                {'timestamp' : { '$gt' : timestamp_file_2}}
            ]
        }
        
        find_result = cos_sim_col.find_one(myquery)
        
        if (find_result is not None):
            print("Cosinus Similarity: ", find_result["cos_sim"])

        else:
            print("Wird berechnet")
            result = calculate(file_1, file_2)
            
            new_value = {"$set": {"file1": file_1, "file2": file_2, "cos_sim": result, "timestamp": datetime.datetime.now()}}
            
            cos_sim_col.update_one(myquery, new_value, True)
            print("Cosinus Similarity: ", result)



all_time_end = time.time()
all_duration = all_time_end - all_time_start

print("Dauer: ")
print(all_duration)

# close connection
myclient.close()