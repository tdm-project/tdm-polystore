import os, shutil
import numpy as np
import requests
import json
import pickle
import copy
import glob
import osmnx as ox
ox.config(log_console=True, use_cache=True)

def get_coords_from_code(code):
    string = "https://mhohmann.dev.openstreetmap.org/tmc/tmcview.php?cid=25&tabcd=1&lcd=%s" % code
    page = requests.get(string)
    for row in page.content.decode().split("\n"):
        if "\"coordinates\"" in row: data = row[:-2]
    d_dict = eval(data)
    data = d_dict["geometry"]["coordinates"]        
    
    return data[1], data[0]


def plot_coords(location_dict):
    list_coords = list(location_dict.values())
    figure(figsize=(6,12))
    scatter(*zip(*list_coords), s=1)
    
    
def load_json(name):
    with open(name) as f:
        data = json.load(f)
    return data["RWS"]


def save_position_codes_from_osm(rws):
    location_dict = {}
    osm_edges_dict = {}

    summ = 0

    for el in rws[0]["RW"][:]:
        for eel in el["FIS"]:
            if True or len(eel["FI"])==2:
                print("-" * 50)
                print("NOME:", el["DE"])
                print()
                for eeel in eel["FI"]:
                    loc = eeel["TMC"]["DE"]
                    pc = eeel["TMC"]["PC"]
                    print(loc,)
                    location_dict[pc] = get_coords_from_code(pc)
                    osm_edges_dict[pc] = ox.get_nearest_node(G, location_dict[pc], return_dist=False)
                    summ += 1
                print()

    print("Total number of edges:", summ)
    
    pickle.dump(location_dict, open("location_id_here.pickle", "wb"))
    pickle.dump(osm_edges_dict, open("osm_edges_here.pickle", "wb"))
    
    
def load_from_file(name):
    tmp_dict = pickle.load(open(name, "rb"))
    return tmp_dict


def extract_traffic_data(rws, location_dict, osm_edges_dict):
    traffic_info_dict = {}

    for el in rws[0]["RW"][:]:
        date_time = el["PBT"]
        for eel in el["FIS"]:
            if True or len(eel["FI"])==2:
                data_dict = {}
                if verbose:
                    print("-" * 50)
                    print("NOME:", el["DE"])
                    print()
                for eeel in eel["FI"]:
                    try:
                        loc = eeel["TMC"]["DE"]
                    except:
                        print("Place description not present!")
                        continue
                    try:
                        pc = eeel["TMC"]["PC"]
                    except:
                        print("Position code not present!")
                        continue
                    try:
                        road_len = eeel["TMC"]["LE"]
                    except:
                        print("road length not present")
                        road_len = np.nan
                    try:
                        flux = eeel["CF"][0]["FF"]
                    except:
                        print("flux not present")
                        flux = np.nan
                    try:
                        conf = eeel["CF"][0]["CN"]
                    except:
                        print("confidence information not present")
                        conf = np.nan
                    try:
                        jam = eeel["CF"][0]["JF"]
                    except:
                        print("jam factor not present")
                        jam = np.nan
                    try:
                        speed = (eeel["CF"][0]["SP"], eeel["CF"][0]["SU"])
                    except:
                        speed = (np.nan, np.nan)
                    if verbose:
                        print(loc, pc, "*", )
                    data_dict["flux"] = flux
                    data_dict["length"] = road_len
                    data_dict["conf"] = conf
                    data_dict["jam"] = jam
                    data_dict["speed"] = speed
                    data_dict["loc"] = loc
                    data_dict["date"] = date_time
                    
                    if pc not in location_dict:
                        try:
                            location_dict[pc] = get_coords_from_code(pc) # this is painfully slow!
                        except:
                            print("Not able to find node coordinates for PC %s" % pc)
                            continue
                    data_dict["coord"] = location_dict[pc]
                    
                    if pc not in osm_edges_dict:
                        try:
                            osm_edges_dict[pc] = ox.get_nearest_node(G, location_dict[pc], return_dist=False)
                        except:
                            print("Not able to find the nearest node to PC %s" % pc)
                            continue
                    data_dict["osm_edge"] = osm_edges_dict[pc]
                    
                    traffic_info_dict[pc] = copy.deepcopy(data_dict)
                    if verbose:
                        print()
                
    return traffic_info_dict, location_dict, osm_edges_dict

def load_save_osm_map(place_name):
    place = place_name

    try:
        G = ox.save_load.load_graphml(place)
        print("Loading graphML version of the map")
    except:
        print("Downloading map from OSM and saving it to disk")
        G = ox.graph_from_place(place_name, network_type='drive')
        ox.save_load.save_graphml(G, filename=place, folder=None)
        
    return G

#########################################################################

verbose = False
processed_path = "./processed/"

G = load_save_osm_map("Sardinia")


try:
    traffic_info_dict = load_from_file("traffic_info_here.pickle")
    print("Loaded Traffic Info file...")
except:
    traffic_info_dict = {}
    
try:
    location_dict = load_from_file("location_id_here_updated.pickle")
    print ("LOADED location_id file...")
except:
    location_dict = {}
try:
    osm_edges_dict = load_from_file("osm_edges_here_updated.pickle")
    print ("LOADED OSM edges file...")
except:
    osm_edges_dict = {}

for i, name in enumerate(glob.glob('*.json')):
    if os.path.getsize(name) == 0:
        continue
    date_time = name[13:-5]
    print(date_time) 
    if date_time in traffic_info_dict:
        print("SKIPPED, ALREADY PRESENT...")
        continue
    rws = load_json(name)
    traffic_info_dict[date_time], location_dict, osm_edges_dict = extract_traffic_data(rws, location_dict, osm_edges_dict)
    
    shutil.move(name, processed_path+name)

#optionally save the updated location dictionary to file
pickle.dump(location_dict, open("location_id_here_updated.pickle", "wb"))
pickle.dump(osm_edges_dict, open("osm_edges_here_updated.pickle", "wb"))

pickle.dump(traffic_info_dict, open("traffic_info_here.pickle", "wb"))




