import numpy as np
import pandas as pd
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
    elif gcc == "7gdar":
        count_array[7] += 1
    elif gcc == "8acte":
        count_array[8] += 1
    
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
        'sa': 'south australia',
        'act': 'australian capital territory',
        'nt': 'northern territory',
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

def report(counts):
    """
        counts: dictionary[author_id, np.array]
    """
    print("#########Task 1#########")
    df_1 = pd.DataFrame(counts).T.iloc[:, :1].sort_values(by=[0], ascending=False)
    df_1 = format_df(df_1, 1)
    print(df_1.to_string(index=False))
    print("\n")

    print("#########Task 2#########")
    df_2 = pd.DataFrame(counts).iloc[1:, :]
    df_2 = format_df(df_2, 2)
    print(df_2.to_string(index=False))
    print("\n")

    print("#########Task 3#########")
    df_3 = pd.DataFrame(counts).iloc[1:, :].T
    df_3 = format_df(df_3, 3)
    print(df_3.to_string(index=False))
    print("\n")

def format_df(df, task_num):
    # Define column names according to assignment spec.
    ## General
    aid = "Author ID"
    count_field = "Number of Tweets Made"
    rank = "Rank"
    max_rank = 10

    # Task 2
    gcc = "Greater Capital City"

    # Task 3
    city_unique = "Number of Unique City"
    total_count_cap = "total_count_cap"

    if task_num == 1:
        df.rename(columns={0: count_field}, inplace=True)
        df.index.name = aid
        df.reset_index(inplace=True)
        df[rank] = [f"#{i+1}" for i in range(len(df))]
        df[rank] = df[rank].apply(lambda x: x if int(x[1:]) <= max_rank else f"#{max_rank}")
        end_row_df1 = explore_df_end(df, [count_field])
        df = df[[rank, aid, count_field]]
        return df.iloc[:end_row_df1+1, :]
    elif task_num == 2:
        df[count_field] = df.sum(axis=1)
        df[gcc] = [f"{ste2gcc(i+1)}" for i in range(len(df))]
        return df[[gcc, count_field]]
    elif task_num == 3:
        # Compute variety and counts in capitals respectively
        df["gcc_varNcount"] = df.apply(lambda x: get_gcc_varNcount([x[i+1] for i in range(len(x))]), axis=1)
        df[city_unique] = df["gcc_varNcount"].apply(lambda x: x[0])
        df[total_count_cap] = df["gcc_varNcount"].apply(lambda x: x[1])
        field_3 = city_unique + " Locations and # Tweets"
        df[field_3] = df.apply(lambda x: render_task_3_field(x), axis=1)
    
        # sort priority: tweet variety, number of tweet in capatals, and ste in ascending order.
        sort_sequence = [city_unique, total_count_cap] + [i+1 for i in range(CONFIG[3]-1)]
        df.sort_values(by=sort_sequence, inplace=True, ascending=False)
        df[rank] = [f"#{i+1}" for i in range(len(df))]
        df[rank] = df[rank].apply(lambda x: x if int(x[1:]) <= max_rank else f"#{max_rank}")
        end_row_df1 = explore_df_end(df, sort_sequence)
        
        df.index.name = aid
        df.reset_index(inplace=True)
        df = df[[rank, aid, field_3]]
        return df.iloc[:end_row_df1+1, :]

def render_task_3_field(df_row):
    nuc = str(df_row["Number of Unique City"])
    tcc = str(df_row["total_count_cap"])

    respective_count_cap = ""
    for i in range(CONFIG[3]-1):
        respective_count_cap += f"#{df_row[i+1]}{ste2gcc(i+1)[1:]}, "
    respective_count_cap = respective_count_cap[:-2]
    r = f"{nuc} (#{tcc} tweets - {respective_count_cap})"
    return r

def get_gcc_varNcount(gcc_counts):
    var = 0
    total_count_cap = 0
    for count in gcc_counts:
        total_count_cap += int(count)
        if count != 0:
            var += 1
    return (var, total_count_cap)

def ste2gcc(ste):
    if ste == 1:
         return "1gsyd"
    elif ste == 2: 
        return "2gmel"
    elif ste == 3: 
        return "3gbri"
    elif ste == 4: 
        return "4gade"
    elif ste == 5: 
        return "5gper"
    elif ste == 6: 
        return "6ghob"
    elif ste == 7: 
        return "7gdar"
    elif ste == 8: 
         return "8acte"

def explore_df_end(df, compare_keys, default_end=9):
    """
        compare_keys: 
            A list of strings that specify the fields for ranking the df row.
    """
    is_equal_priority = True
    i = default_end
    while is_equal_priority:
        for key in compare_keys:
            if df[key][i] != df[key][i+1]:
                is_equal_priority = False
                break
        if not is_equal_priority:
            break
        i += 1
        if i == len(df) - 1:
            break
    return i
    