import pymongo
import time
import datetime
import argparse


def getQuery(filename, param):
    query = {param+".file": filename}

    return query


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
decoded_sim_col = mydb["decoded_sim"]
stack_sim_col = mydb["stack_sim"]
tight_sim_col = mydb["tight_sim"]
sentence_sim_col = mydb["sentence_sim"]
calculated_sim_col = mydb["calculated_sim"]

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


calculated_sim_col.create_index([('file1')])
calculated_sim_col.create_index([('file2')])



all_time_start = time.time()



# loop over all samples and calculate similarity if not already calculated
for main_sample in samples_outerloop:   
    file1 = main_sample["filename"]
    print(file1)
    timestamp_file_1 = main_sample["string_timestamp"]

    # get 1 decoded, stack and tight string for file1 if exists 
    decoded_file1_result = strings_col.find_one(getQuery(file1, "decoded"))
    stack_file1_result = strings_col.find_one(getQuery(file1, "stack"))
    tight_file1_result = strings_col.find_one(getQuery(file1, "tight"))
    
    
    for sample in samples_innerloop:
        file2 = sample["filename"]
        timestamp_file_2 = sample["string_timestamp"]
        
        print(file2)
        
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
        
        find_result = calculated_sim_col.find_one(myquery)
        
        if (find_result is not None):
            print("Similarity: ", find_result["sim"])
        
        else:
            print("Wird berechnet")

            # get cosinus and sentence similarity
            cos_result = cos_sim_col.find_one({"$or": [{"file1": file1, "file2": file2}, {"file1": file2, "file2": file1}]})
            sentence_result = sentence_sim_col.find_one({"$or": [{"file1": file1, "file2": file2}, {"file1": file2, "file2": file1}]}) 
            
            cos_sim = cos_result.get("cos_sim")
            sentence_sim = sentence_result.get("sim_var2")
            calculated_similarity = 0;
            
            # if file1 has no decoded, stack and tight strings calculate similaroty only with cosinus and sentence similarity
            if((decoded_file1_result is None) and (stack_file1_result is None) and (tight_file1_result is None)):
                calculated_similarity = (0.9 * cos_sim) + (0.1 * sentence_sim);
            
            else:              
                deobfucation_variants = 0;
                decoded_sim = 0;
                stack_sim = 0;
                tight_sim = 0;
                
                no_decoded = False;
                no_stack = False;
                no_tight = False;
               
                # check if file1 and file2 has no decoded strings
                if(decoded_file1_result is None):
                    no_decoded = True;
                else:
                    decoded_file2_result = strings_col.find_one(getQuery(file2, "decoded"))
                    
                    if(decoded_file2_result is None):
                        no_decoded = True;
                
                # check if file1 and file2 has no stack strings
                if(stack_file1_result is None):
                    no_stack = True;
                else:
                    stack_file2_result = strings_col.find_one(getQuery(file2, "stack"))
                    
                    if(stack_file2_result is None):
                        no_stack = True;
                
                # check if file1 and file2 has no tight strings
                if(tight_file1_result is None):
                    no_tight = True;
                else:
                    tight_file2_result = strings_col.find_one(getQuery(file2, "tight"))
                    
                    if(tight_file2_result is None):
                        no_tight = True;
                
                # if file1 and file2 has no decoded, stack and tight strings calculate similarity only with cosinus and sentence similarity
                if(no_decoded and no_stack and no_tight):
                    calculated_similarity = (0.9 * cos_sim) + (0.1 * sentence_sim);
                
                # if file1 and file1 has both decoded, stack or tight strings calculate similarity with similarity of decoded, stack and/or tight similarity
                else:
                    if(not no_decoded):
                        decoded_sim = decoded_sim_col.find_one({"$or": [{"file1": file1, "file2": file2}, {"file1": file2, "file2": file1}]}).get("decoded_sim")
                        deobfucation_variants += 1;

                    if(not no_stack):
                        stack_sim = stack_sim_col.find_one({"$or": [{"file1": file1, "file2": file2}, {"file1": file2, "file2": file1}]}).get("stack_sim")
                        deobfucation_variants += 1;
                    
                    if(not no_tight):
                        tight_sim = tight_sim_col.find_one({"$or": [{"file1": file1, "file2": file2}, {"file1": file2, "file2": file1}]}).get("tight_sim")
                        deobfucation_variants += 1;
                    
                    deobfuscation_sim = (decoded_sim + stack_sim + tight_sim) / deobfucation_variants
                    
                    calculated_similarity = (0.8 * cos_sim) + (0.15 * deobfuscation_sim) + (0.05 * sentence_sim);
                    

            new_value = {"$set": {"file1": file1, "file2": file2, "sim": calculated_similarity, "timestamp": datetime.datetime.now()}}                
            
            calculated_sim_col.update_one(myquery, new_value, True)
            print("Similarity: ", calculated_similarity)
            
    
all_time_end = time.time()
all_duration = all_time_end - all_time_start

print("Dauer: ")
print(all_duration)

myclient.close()

