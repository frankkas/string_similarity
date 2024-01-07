import pymongo
import time
import datetime
import numpy as np
import argparse

# get query to find sentences
def getQuery(filename):
    query = [
        { "$addFields": { "resultObject": { "$replaceAll": { "input": "$string", "find": "\t", "replacement": " " } }}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": ".", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": ",", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": ";", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": ":", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": "!", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": "?", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": "`", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": "'", "replacement": "" }}, "files": 1}},
        { "$project": { "resultObject": {"$replaceAll": { "input": "$resultObject", "find": '"', "replacement": "" }}, "files": 1}},   
        { "$project" : { "split_string" : { "$split": ["$resultObject", " "] }, "split_string_lower" : { "$split": [ {"$toLower": "$resultObject"}, " "] }, "files": 1} },
        { "$project": { "split_string": { "$filter": { "input": "$split_string", "cond": { "$ne": ["$$this", ""] } }}, "split_string_lower": { "$filter": { "input": "$split_string_lower", "cond": { "$ne": ["$$this", ""] } }}, "files": 1 }},
        { "$project": { "split_string": 1, "size_of_split": {"$size": "$split_string"}, "split_string_lower": 1, "files": 1 }},
        { "$match": {"size_of_split": {"$gt": 2}, "files.file": filename}},
        { "$project": {"files": 0}}
    ]

    return query


# calculate sentence similaritiy for 2 samples
def calculate_sim(file1_string_set, file2_string_set):
    result_list = []
    
    # initialize sets to count hits for file1 and file2
    file1_sim_set = set();
    file2_sim_set = set();
    same_count = 0;
    
    list_file1 = list(file1_string_set)
    list_file2 = list(file2_string_set)

    # calculate for every sentence from file1 similarity to sentence from file2
    # if dice similarity is over threshold (0.6) its a hit
    for object_file1 in list_file1:
        sentence_original_file1 = list(object_file1.get("split_string"))
        sentence_original_file1_set = set(sentence_original_file1)
        sentence_lower_file1 = list(object_file1.get("split_string_lower"))
        sentence_lower_file1_set = set(sentence_lower_file1)
        sentence_id_file1 = object_file1.get("_id")
        
        for object_file2 in list_file2:
            sentence_original_file2 = list(object_file2.get("split_string"))
            sentence_original_file2_set = set(sentence_original_file2)
            sentence_lower_file2 = list(object_file2.get("split_string_lower"))
            sentence_lower_file2_set = set(sentence_lower_file2)
            sentence_id_file2 = object_file2.get("_id")

            # count same sentences for calculation
            if(sentence_id_file1 == sentence_id_file2):
                same_count += 1;
            
            # if both sentences have 1 word in commen (lowercase) calculate sentence similarity
            inter = sentence_lower_file1_set.intersection(sentence_lower_file2_set)
            if(len(inter) > 0):
                # calculate similarity of same words in lowercase
                intersection = sentence_original_file1_set.intersection(sentence_original_file2_set)
                d_sim = float((2* len(intersection))) / float((len(sentence_original_file1_set) + len(sentence_original_file2_set)))

                # calculate word order similarity (case sensitive)
                word_list = list(sentence_lower_file1_set.union(sentence_lower_file2_set))
                file1_word_array = []
                file2_word_array = []
                
                for word in word_list:
                    index_file1 = sentence_lower_file1.index(word) + 1 if word in sentence_lower_file1 else 0
                    file1_word_array.append(index_file1)
                    index_file2 = sentence_lower_file2.index(word) + 1 if word in sentence_lower_file2 else 0
                    file2_word_array.append(index_file2)

                
                file1_word_vector = np.array(file1_word_array)
                file2_word_vector = np.array(file2_word_array)
                
                sum_vector = file1_word_vector + file2_word_vector
                diff_vector = file1_word_vector - file2_word_vector

                order_sim = 1 - (np.linalg.norm(diff_vector) / np.linalg.norm(sum_vector))

                # calculate sentence similarity
                sentence_sim = (d_sim + order_sim) / 2
                
                
                if (sentence_sim >= 0.6):
                    result_list.append({"string_set_file1": sentence_original_file1, "string_set_file2": sentence_original_file2, "sentence_sim": sentence_sim})

                    file1_sim_set.add(str(sentence_original_file1))
                    file2_sim_set.add(str(sentence_original_file2))
     
    
    return result_list, len(file1_sim_set), len(file2_sim_set), file1_sim_set, file2_sim_set, same_count




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
sentence_sim_col = mydb["sentence_sim"]

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

sentence_sim_col.create_index([('file1')])
sentence_sim_col.create_index([('file2')])

sentences_json_dict = {}

all_time_start = time.time()



# loop over all samples and calculate similarity if not already calculated
for main_sample in samples_outerloop:
    file1 = main_sample["filename"]
    print(file1)
    timestamp_file_1 = main_sample["string_timestamp"]
    
    
    key = file1
    if key in sentences_json_dict:
        file1_sentences = sentences_json_dict[key]
    else:
        query_file1 = getQuery(file1)
        file1_sentences = list(strings_col.aggregate(query_file1))
        sentences_json_dict[key] = file1_sentences
    

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
        
        find_result = sentence_sim_col.find_one(myquery)
        
        if (find_result is not None):
            print("Sentence Similarity: ", find_result["sim_var2"])
        
        else:
            print("Wird berechnet")
            
            key = file2
            if key in sentences_json_dict:
                file2_sentences = sentences_json_dict[key]
            else:
                query_file2 = getQuery(file2)
                file2_sentences = list(strings_col.aggregate(query_file2))
                sentences_json_dict[key] = file2_sentences
        

            if(file1 != file2):
                result, count_file1, count_file2, list_file1, list_file2, count_same_files = calculate_sim(file1_sentences, file2_sentences)

                # calculate ratio between hits and all number of sentences
                len_all_sentences = (len(file1_sentences) + len(file2_sentences))
                if (len_all_sentences > 0):
                    # ratio of hits to sentences
                    sim_var1 = (count_file1 + count_file2) / len_all_sentences
                
                    # weighted ratio of hits and sentences with exact sentences
                    sim_exact = ( 2 * count_same_files) / len_all_sentences 
                    sim_var2 = sim_var1 + (0.1 * sim_exact)
                
                else:
                    sim_var1 = 0
                    sim_var2 = 0
                

            else:
                print("Same File")
                result = []
                count_file1 = 0
                count_file2 = 0
                sim_var1 = 1
                sim_var2 = 1.1
                count_same_files = 1

#            if(len(result) > 500):
#                result = []

            new_value = {"$set": {"file1": file1, "file2": file2,
#                                      "sim_sentences": result,
                                      "count_file1": count_file1,
                                      "count_file2": count_file2,
                                      "sim_var1": sim_var1,
                                      "sim_var2": sim_var2,
                                      "same_strings": count_same_files,
                                      "other_strings": (len(file1_sentences) + len(file2_sentences)),
                                      "timestamp": datetime.datetime.now()}}

            
            sentence_sim_col.update_one(myquery, new_value, True)
            print("Sentence Similarity: ", sim_var2)
            
        

all_time_end = time.time()
all_duration = all_time_end - all_time_start

print(all_duration)
    

