from pathlib import Path
from JSONHelper import buildJSON, mergeDuplicates
import pymongo
import re
import json
import subprocess
import time
import datetime
import argparse


def save_strings(strings_json, file_name, strings_count):

    for string_object in strings_json:
        print(string_object["string"])

        string_myquery = { "string": string_object["string"] }
        
        # delete any existing entry for file
        del_values = {"$pull": {"files": {"file": file_name}}}
        strings_col.update_one(string_myquery, del_values, True)
        

        tf = string_object["count"] / strings_count
        
        # generate new mongoDB document 
        newvalues = {"$push": {"files": {"file": file_name, "count": string_object["count"], "tf": tf }}}
        
        if(string_object["decoding_routine"]):
            
            decoded_value = {"file": file_name, "decoding_routine": string_object["decoding_routine"], "decoded_at": string_object["decoded_at"]}
            newvalues["$push"]["decoded"] = decoded_value
            
        if(string_object["stack_function"]):
            
            stack_value = {"file": file_name, "function": string_object["stack_function"]}
            newvalues["$push"]["stack"] = stack_value
            
        if(string_object["tight_function"]):
            
            tight_value = {"file": file_name, "function": string_object["tight_function"]}
            newvalues["$push"]["tight"] = tight_value
            
        
        # save string to mongoDB
        strings_col.update_one(string_myquery, newvalues, True)
        
        

def extract_strings(file_path, json_file_path):
    print(file_path)
    
    json_file_path.parent.mkdir(parents=True, exist_ok=True)

    strings_json = json.dumps("{}")

    # check if path already exists. if true load file, else extract strings with floss
    if(json_file_path.exists()):
        print("Existiert")
        strings_json = json.load(json_file_path.open("r"))
        print(strings_json)
        
    else:
        print("Existiert nicht")
        
        # extract strings with floss as json format
        cmd = "floss -j -q -n 6 "+ file_path.as_posix()
        result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
        if(result.stdout):
            strings_json = json.loads(result.stdout)
            print(strings_json)              
            
            # save to json file
            with json_file_path.open("w") as target: 
                json.dump(strings_json, target, indent=4)
        
        # if floss don't work extract only static strings with floss
        else:
            cmd_static = "floss -j -n 6 --only static -- "+ file_path.as_posix()
            result_static = subprocess.run([cmd_static], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


            if(result_static.stdout):
                strings_json = json.loads(result_static.stdout)
                print(strings_json) 
                
                with json_file_path.open("w") as target: 
                    json.dump(strings_json, target, indent=4)
            
            # if floss don't work extract only static strings with string.exe
            else:
                cmd_ascii = "strings -e s -n 6 "+ file_path.as_posix()
                cmd_utf16 = "strings -e l -n 6 "+ file_path.as_posix()

                result_ascii = subprocess.run([cmd_ascii], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                result_utf16 = subprocess.run([cmd_utf16], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                raw_strings = result_ascii.stdout + result_utf16.stdout
                strings_list = raw_strings.split("\n")
                strings_list = list(filter(None, strings_list))
                
                strings_dict = dict()
                strings_json = {'strings': {'static_strings': []}}
                
                for elem in strings_list:
                    strings_json["strings"]["static_strings"].append({"string": elem})
                
                with json_file_path.open("w") as target: 
                    json.dump(strings_json, target, indent=4)
                    
            if(result_static.stderr):
                print("Error Static: "+ result_static.stderr)
                        
                
        if(result.stderr):
            print("Error: "+ result.stderr)

    return strings_json      
                

            
               

def save_file_strings(file_name, file_family, file_version, strings_json):
    time_start = time.time()

    if('strings' in strings_json):
        # build one json list for all strings (static, decoded, stack, tight)
        data = buildJSON(strings_json)
        print(data)
        
        # merge all duplicate strings; with count
        result = mergeDuplicates(data, 'string')
        print(result)

        # save strings
        save_strings(result, file_name, len(data))

    else:
        empty_files.append(file_family +"/"+ file_version +"/"+ file_name)
        data = {}
        
    time_end = time.time()
    duration = time_end - time_start
    string_timestamp = datetime.datetime.now()
    
    # save Sample in samples collection
    file_myquery = { "filename": file_name }
    print(file_myquery)
    

    file_row = { 
        "filename": file_name,
        "family": file_family,
        "version": file_version,
        "statistics.num_strings": len(data),
        "extract_version": glob_extract_version,
        "string_timestamp": string_timestamp,
        "duration": duration
    }
 
    # save sample (name, family, version) in mongoDB
    files_col.update_one(file_myquery, {"$set": file_row}, True)


def handle_file(args):
    
    if (files_col.count_documents({"filename": malpedia_home_path.name, "extract_version": glob_extract_version})):
        print("schon erfasst: "+ malpedia_home_path.name)
    else:
        print("file: ")
        print(malpedia_home_path.name)
        family = args.family if args.family else 'unknown'
        version = args.version if args.version else ''
        print(family)
        print(version)
        
        malpedia_file = malpedia_home_path.name
        json_file_path = Path.joinpath(json_home_path, family, version, malpedia_file + ".json")
        
        print(json_file_path)
        
        # extract strings
        extracted_strings = extract_strings(args.filepath, json_file_path)
        
        # save extracted strings
        save_file_strings(malpedia_home_path.name, family, version, extracted_strings)


def handle_dir(args):
    family = args.family if args.family else 'unknown'
    version = args.version if args.version else ''
    print(family)
    print(version)
    
    # loop over all files in directoy matching malpedia pattern (not recursive)
    for malpedia_file_path in malpedia_home_path.glob('*'):
        if (malpedia_file_path.is_file() and re.match(malpedia_file_pattern, malpedia_file_path.name)):
            print(malpedia_file_path)
            
            if (files_col.count_documents({"filename": malpedia_file_path.name, "extract_version": glob_extract_version})):
                print("schon erfasst: "+ malpedia_file_path.name)
            else:
                malpedia_file = malpedia_file_path.name
                json_file_path = Path.joinpath(json_home_path, family, version, malpedia_file + ".json")
                
                print(json_file_path)
                
                # extract strings
                extracted_strings = extract_strings(malpedia_file_path, json_file_path)
                
                # save extracted strings
                save_file_strings(malpedia_file_path.name, family, version, extracted_strings)
                
                
                
def handle_rec(args):
    family = args.family if args.family else 'unknown'
    
    # loop over all files in directoy recursive matching malpedia pattern
    for malpedia_file_path in malpedia_home_path.rglob('*'):
        if (malpedia_file_path.is_file() and re.match(malpedia_file_pattern, malpedia_file_path.name)):
            print(malpedia_file_path)
            
            malpedia_file = malpedia_file_path.name            

            if (files_col.count_documents({"filename": malpedia_file, "extract_version": glob_extract_version})):
                print("schon erfasst: "+ malpedia_file)
            else:
                # get version for actual sample from filepath
                path_parts = malpedia_file_path.relative_to(malpedia_home_path).parts
                version = path_parts[0] if path_parts[0] != malpedia_file else ""
                
                print("Version: ")
                print(version)
     
                json_file_path = Path.joinpath(json_home_path, family, version, malpedia_file + ".json")
                print("JSON: ")
                print(json_file_path)
                
                # extract strings
                extracted_strings = extract_strings(malpedia_file_path, json_file_path)
                
                # save extracted strings
                save_file_strings(malpedia_file, family, version, extracted_strings)



def handle_malpedia(args):
    # loop over all malpedia files matching malpedia pattern
    for malpedia_file_path in malpedia_home_path.rglob('*'):
        if (malpedia_file_path.is_file() and re.match(malpedia_file_pattern, malpedia_file_path.name)):
            malpedia_file = malpedia_file_path.name
            json_dir = malpedia_file_path.relative_to(malpedia_home_path).parent
            json_file_path = Path.joinpath(json_home_path, json_dir, malpedia_file + ".json")
            
            if (files_col.count_documents({"filename": malpedia_file_path.name, "extract_version": glob_extract_version})):
                print("schon erfasst: "+ malpedia_file_path.name)
            else:
                print(malpedia_file_path)
                
                # extract strings
                extracted_strings = extract_strings(malpedia_file_path, json_file_path)
                
                # get family and version for actual sample from filepath
                path_parts = malpedia_file_path.relative_to(malpedia_home_path).parts
                family = path_parts[0]
                version = path_parts[len(path_parts)-2] if (len(path_parts)-2) > 0 else ""
                
                # save extracted strings
                save_file_strings(malpedia_file_path.name, family, version, extracted_strings)



##########################################################################################################
##########################################################################################################



glob_extract_version = 1.0

parser = argparse.ArgumentParser()
parser.add_argument("filepath", type=Path, help="Submit the following <filepath>, indicating a (file/dir).")
parser.add_argument("jsonpath", type=Path, help="Save JSON in following <filepath>, indicating a (file/dir).")
parser.add_argument("--mode", type=str, default="file", choices=["file", "dir", "recursive", "malpedia"], help="Submit a single <file> or all files in a <dir>. Use <recursive> submission for a folder structured as ./family_name/version/files. Synchronize <malpedia>. Default: <file>.")
parser.add_argument("-f", "--family", type=str, help="Set/Override StringReport with this family (only in modes: file/dir)")
parser.add_argument("-v", "--version", type=str, help="Set/Override StringReport with this version (only in modes: file/dir/recursive)")


args = parser.parse_args()

malpedia_home_path = Path(args.filepath).expanduser().resolve()
json_home_path = Path(args.jsonpath).expanduser().resolve() 


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:9000/")
print(myclient)
#mydb = myclient["stringsdatabase"]
mydb = myclient["testdatabase"]
strings_col = mydb["strings"]
files_col = mydb["samples"]

strings_col.create_index([('string')])

malpedia_file_pattern = re.compile("^[0-9a-f]{64}(_unpacked|_dump7?_0x[0-9a-fA-F]{8,16})")


failed_files = []
empty_files = []

all_time_start = time.time()


if (args.mode == "file"):
    if (not malpedia_home_path.is_file()):
        print("Mode <file> only works when <filepath> is a file.")
    else:
        handle_file(args)
        
elif (args.mode in ["dir", "recursive", "malpedia"]):
    if (not malpedia_home_path.is_dir()):
        print("Modes <dir|recursive|malpedia> only work when <filepath> is a directory.")
    elif (args.mode == "dir"):
        handle_dir(args)
    elif (args.mode == "recursive"):
        handle_rec(args)
    elif (args.mode == 'malpedia'):
        handle_malpedia(args)




print("Failed files: "+ " ".join(failed_files))
print("Empty files: "+ " ".join(empty_files))

all_time_end = time.time()
all_duration = all_time_end - all_time_start

print("Dauer: ")
print(all_duration)


myclient.close()