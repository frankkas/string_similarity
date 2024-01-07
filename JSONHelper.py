from pathlib import Path
import json




def buildJSON(strings_json):
    # get static, decoded, stack and tight strings
    data_static = strings_json["strings"]["static_strings"] if strings_json.get("strings").get("static_strings") else []
    data_decoded = strings_json["strings"]["decoded_strings"] if strings_json.get("strings").get("decoded_strings") else []
    data_stack = strings_json["strings"]["stack_strings"] if strings_json.get("strings").get("stack_strings") else []
    data_tight = strings_json["strings"]["tight_strings"] if strings_json.get("strings").get("tight_strings") else []

    # set property stack_function and tight_function to avoid conflicts
    for item in data_stack:
        item['stack_function'] = item['function']

    for item in data_tight:
        item['tight_function'] = item['function']
        
    # merge all strings to one list
    data_all = data_static
    data_all.extend(data_decoded)
    data_all.extend(data_stack)
    data_all.extend(data_tight)
    
    return data_all


# merge same values and count
def mergeDuplicates(data, propToMerge):
    counts = {}
    for obj in data:
        # get actual string
        propValue = obj[propToMerge]
        
        # if string already in counts object increase count by one; add additional information of decoded, stack or tight if it exists
        if propValue in counts:
            counts[propValue]['counts'] += 1
          
            if (obj.get('decoding_routine')):
                counts[propValue]['decoding_routine'].append(obj.get('decoding_routine'))
            if (obj.get('decoded_at')):
                counts[propValue]['decoded_at'].append(obj.get('decoded_at'))
            if (obj.get('stack_function')):
                counts[propValue]['stack_function'].append(obj.get('stack_function'))
            if (obj.get('tight_function')):
                counts[propValue]['tight_function'].append(obj.get('tight_function')) 
    
        # add actual string to counts object with count 1
        else:
            decoding = [obj.get('decoding_routine')] if obj.get('decoding_routine') else []
            decoded_at = [obj.get('decoded_at')] if obj.get('decoded_at') else []
            stack_function = [obj.get('stack_function')] if obj.get('stack_function') else []
            tight_function = [obj.get('tight_function')] if obj.get('tight_function') else []
            counts[propValue] = {'counts': 1, 'decoding_routine': decoding, 'decoded_at': decoded_at, 'stack_function': stack_function, 'tight_function': tight_function }
  
    result = []

    # add all strings to result
    for propValue in counts:
        result.append({ propToMerge: propValue, 'count': counts[propValue]['counts'], 'decoding_routine': list(set(counts[propValue].get('decoding_routine'))), 'decoded_at': list(set(counts[propValue].get('decoded_at'))), 'stack_function': counts[propValue].get('stack_function'), 'tight_function': counts[propValue].get('tight_function') })
  
    return result

