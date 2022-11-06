import pm4py
import os
if __name__ == "__main__":
    log = pm4py.read_xes(os.path.join("example.xes"))
    from copy import deepcopy
    filtered_log = pm4py.filter_variants_top_k(log, 3)
    print("Filtered log:")
    print(filtered_log)

