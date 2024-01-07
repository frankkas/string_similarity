import pymongo
import time
import datetime
import argparse


def jaccard_similarity(setA, setB):
    intersection = setA.intersection(setB)
    union = setA.union(setB)   
    jaccard = float(len(intersection)) / float(len(union))
    
    return jaccard


def dice_similarity(setA, setB):
    intersection = setA.intersection(setB)
    dice = float((2* len(intersection))) / float((len(setA) + len(setB)))
    
    return dice


def overlap_similarity(setA, setB):
    intersection = setA.intersection(setB)
    divisor = len(setA) if (len(setA) <= len(setB)) else len(setB)
    overlap = float(len(intersection)) / float(divisor)
    
    return overlap


def getQuery(file):
    query = [
        { "$unwind": '$decoded'},
        { "$match": { "decoded.file": file}},
        { "$unwind": '$decoded.decoding_routine'},
        { "$group": {"_id": {"file": "$decoded.file", "decoding_routine": "$decoded.decoding_routine"}, "strings": {"$addToSet": "$string"}}}
    ];
    
    return query;

def calculate_sim(file1_decoded_set, file2_decoded_set):
    result_list = []
    
    # initialize sets to count hits for file1 and file2
    file1_sim_set = set()
    file2_sim_set = set()
    
    # calculate for every string-set from file1 similarity to strings-set from file2
    # if dice similarity is over threshold (0.7) its a hit
    for i in file1_decoded_set: 
        for j in file2_decoded_set:
            j_sim = jaccard_similarity(set(i["strings"]), set(j["strings"]))
            d_sim = dice_similarity(set(i["strings"]), set(j["strings"]))
            o_sim = overlap_similarity(set(i["strings"]), set(j["strings"]))

            if (d_sim >= 0.7):
                result_list.append({"function1_offset": i["_id"]["decoding_routine"], "function2_offset": j["_id"]["decoding_routine"], "jaccard": j_sim, "dice": d_sim, "overlap": o_sim})
                file1_sim_set.add(i["_id"]["decoding_routine"])
                file2_sim_set.add(j["_id"]["decoding_routine"])


    # calculate ratio between hits and all number of string-sets
    all_count_files = len(file1_decoded_set) + len(file2_decoded_set)
    
    if(all_count_files > 0):
        decoded_sim = ( len(file1_sim_set) + len(file2_sim_set) ) / all_count_files
    else:
        decoded_sim = 0

    return result_list, decoded_sim



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
strings_col = mydb["strings"]
files_col = mydb["samples"]
decoded_sim_col = mydb["decoded_sim"]


# set sample lists for outer and inner loop depending on mode
if (args.mode == "file"):
    samples_outerloop_cursor = files_col.find({"filename": args.filename})
    samples_innerloop_cursor = files_col.find({})
        
elif (args.mode == "family"):
    samples_outerloop_cursor = files_col.find({"family": args.family})
    samples_innerloop_cursor = files_col.find({})

elif ("all"):
    all_samples_sorted_query = [{
        "$lookup": {
        "from": 'strings',
        "localField": 'filename',
        "foreignField": 'decoded.file',
        "as": 'decoded'
        }},
        { "$sort": { "decoded" : 1 } }
    ]
    samples_outerloop_cursor = files_col.aggregate(all_samples_sorted_query)
    samples_innerloop_cursor = files_col.aggregate(all_samples_sorted_query)



samples_outerloop = list(samples_outerloop_cursor)
samples_innerloop = list(samples_innerloop_cursor)


decoded_sim_col.create_index([('file1')])
decoded_sim_col.create_index([('file2')])

decoded_json_dict = {}

all_time_start = time.time()


# loop over all samples and calculate similarity if not already calculated
for main_sample in samples_outerloop:
    file1 = main_sample["filename"]
    print(file1)
    timestamp_file_1 = main_sample["string_timestamp"]
    

    key = file1
    if key in decoded_json_dict:
        file1_decoded = decoded_json_dict[key]
    else:
        file1_decoded = list(strings_col.aggregate(getQuery(file1)))
        decoded_json_dict[key] = file1_decoded
    
    
    for sample in samples_innerloop:
        file2 = sample["filename"]
        print(file2)
        timestamp_file_2 = sample["string_timestamp"]

        myquery = {
            '$or': 
            [
                {'file1': file1, 'file2': file2},
                {'file1': file2, 'file2': file1}
            ],
            '$and':
            [
                {'timestamp' : { '$gt' : timestamp_file_1}},
                {'timestamp' : { '$gt' : timestamp_file_2}}
            ]
        }


        find_result = decoded_sim_col.find_one(myquery)
        
        if (find_result is not None):
            print("Decoded Similarity: ", find_result["decoded_sim"])
        
        else:
            print("Wird berechnet")
        
     
            if (len(file1_decoded) > 0):
                key = file2
                if key in decoded_json_dict:
                    file2_decoded = decoded_json_dict[key]
                else:
                    file2_decoded = list(strings_col.aggregate(getQuery(file2)))
                    decoded_json_dict[key] = file2_decoded
                
                result, decoded_sim = calculate_sim(file1_decoded, file2_decoded)

            else:
                result = [];
                decoded_sim = 0;

            

            new_value = {"$set": {"file1": file1, "file2": file2,
                                  "result_sim": result,
                                  "decoded_sim": decoded_sim,
                                  "timestamp": datetime.datetime.now()}}
            
            decoded_sim_col.update_one(myquery, new_value, True)
            print("Decoded Similarity: ", decoded_sim)            


all_time_end = time.time()
all_duration = all_time_end - all_time_start

print(all_duration)

# close connection
myclient.close()
