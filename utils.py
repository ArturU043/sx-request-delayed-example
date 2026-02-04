# saving preprocessing output
import dataclasses
import gzip
import json
import math
import re
import time



def dsid_rtag_campaign(name: str) -> tuple[str, str, str]:
    """extract information from container name"""
    data_tag = re.findall(r":(data\d+)_", name)
    if data_tag:
        return "data", None, data_tag[0]

    dsid = re.findall(r".(\d{6}).", name)[0]
    rtag = re.findall(r"_(r\d+)_", name)[0]

    if rtag in ["r13167", "r14859", "r13297", "r14862"]:
        campaign = "mc20a"
    elif rtag in ["r13144", "r14860", "r13298", "r14863"]:
        campaign = "mc20d"
    elif rtag in ["r13145", "r14861", "r13299", "r14864"]:
        campaign = "mc20e"
    elif rtag in ["r14622", "r15540"]:
        campaign = "mc23a"
    elif rtag in ["r15224", "r15530"]:
        campaign = "mc23d"
    elif rtag in ["r16083"]:
        campaign = "mc23e"
    else:
        print("cannot classify", name)
        campaign = None

    return dsid, rtag, campaign


def check_for_ntuples(dataset_info):
    keys_with_ntuples = {}

    for key, sample in dataset_info.items():
        container_with_ntuples = 0
        total_output = 0
        n_containers = len(sample.keys())
        for info in sample.values():
            if info["output"] is not None:
                container_with_ntuples += 1
                total_output += info["size_output_GB"]
        if container_with_ntuples > 0:
            keys_with_ntuples[key] = {
                "samples": n_containers,
                "samples_with_ntuples": container_with_ntuples,
                "total_output_GB": round(total_output, 2),
            }
    return keys_with_ntuples
