import numpy as np
from config import CONFIG
import re

def compute_counts(loc_info: str, sal_json_info: tuple) -> np.array:
    
    sal_json_append, sal_json_original = sal_json_info
    loc_detail = loc_info.split(", ")
    counts_array = init_count_array()

    # TASK 2 COUNT: Total number of tweets
    counts_array[0] += 1

    # Assume that len(loc_detail) <= 2
    loc_info_1 = loc_detail[0].strip().lower().split(" - ")
    loc_info_2 = loc_detail[1].strip().lower() if len(loc_detail) >= 2 else None 

    for suburb in loc_info_1:
        key = (suburb, loc_info_2)
        if loc_info_2 is None: # Use origin
            gcc = sal_json_original[key] if key in sal_json_original else None
        else: # Use append
            gcc = sal_json_append[key] if key in sal_json_append else None

        if gcc:
            counts_array = gcc2count(gcc, counts_array)
    
    return counts_array

def gcc2count(gcc: str, count_array: np.array):
    if gcc == "1gsyd":
        count_array[1] += 1
    elif gcc == "2gmel":
        count_array[2] += 1
    elif gcc == "3gbri":
        count_array[3] += 1
    elif gcc == "4gade":
        count_array[4] += 1
    elif gcc == "5gper":
        count_array[5] += 1
    elif gcc == "6ghob":
        count_array[6] += 1
    
    return count_array

def process_sal(sal_json):
    regex_sal = re.compile(r"""
        ([^()]+) {1}  
        (
            \( 
                ([^-\s()]+ ((-|\s)[^-\s()]+)+ | [^-\s()]+) 
                (\s-\s)? 
                ([^-()]+)? 
            \) 
        ) ?
    """, re.X)

    sal_json_append = {}
    sal_json_original = {}
    for k in sal_json.keys():
        match = regex_sal.search(k)

        loc_info_0 = match.group(0)
        loc_info_1 = abbr2full(match.group(2)) 
        # The third info is in group(6), not useful here.

        if loc_info_1 is None:
            sal_json_original[(loc_info_0, None)] = sal_json[k]["gcc"]
            state = get_state(sal_json[k]["ste"])

            # state below could still be None, indicating islands. 
            # Indicating that tweets from islands with ste == 9 can not be correctly identified.
            sal_json_append[(loc_info_0, state)] = sal_json[k]["gcc"]
        else:
            sal_json_append[(loc_info_0, loc_info_1)] = sal_json[k]["gcc"]



    return (sal_json_append, sal_json_original)


def abbr2full(potential_abbr):
    state_abbr = {
        'vic.': 'victoria', 
        'tas.': 'tasmania', 
        'nsw': 'new south wales', 
        'qld': 'queensland', 
        'wa': 'western australia', 
        'sa': 'south australia'
    }

    if potential_abbr in state_abbr.keys():
        return state_abbr[potential_abbr]
    else:
        return potential_abbr

def get_state(ste: str) -> str:
     if ste == "1":
         return "new south wales"
     elif ste == "2": 
         return "victoria"
     elif ste == "3": 
         return "queensland"
     elif ste == "4": 
         return "south australia"
     elif ste == "5": 
         return "western australia"
     elif ste == "6": 
         return "tasmania"
     elif ste == "7": 
         return "northern territory"
     elif ste == "8": 
         return "australian capital territory"
     else:
        #  logging.info("Islands belong to various states. Mapping not implemented.")
         return None
     
def init_count_array():
    return np.zeros(CONFIG[3], dtype=np.int32)