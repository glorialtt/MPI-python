import mpi4py.MPI as MPI
import json
import time
import sys


def coordinatesJudgement(postCoordinate, grid):
    '''
    Judge which region the Twitter belongs to.
    Parameters:
        postCoordinate: List, coordinates of each Twitter
        grid: Dictionary, information/coordinates of regions
    '''
    satisifed_criterion = 0
    record_key = None
    for key, value in grid.items():
        xmin = value["xmin"]
        xmax = value["xmax"]
        ymin = value["ymin"]
        ymax = value["ymax"]
        if ((postCoordinate[0] < xmax) and (postCoordinate[0] > xmin) and (postCoordinate[1] < ymax) and (postCoordinate[1] > ymin)):
            record_key = key
            break
        elif (((postCoordinate[0] == xmax) or (postCoordinate[0] == xmin)) and (postCoordinate[1] < ymax) and (postCoordinate[1] > ymin)):
            if (postCoordinate[0] == xmax):
                record_key = key
                break
            else:
                record_key = key
        elif ((postCoordinate[0] < xmax) and (postCoordinate[0] > xmin) and ((postCoordinate[1] == ymax) or (postCoordinate[1] == ymin))):
            if (postCoordinate[1] == ymin):
                record_key = key
                break
            else:
                record_key = key
        elif (((postCoordinate[0] == xmax) or (postCoordinate[0] == xmin)) and ((postCoordinate[1] == ymax) or (postCoordinate[1] == ymin))):
            if ((postCoordinate[0] == xmax) and (postCoordinate[1] == ymin)):
                record_key = key
                break
            elif((postCoordinate[0] == xmax) or (postCoordinate[1] == ymin)):
                satisifed_criterion = 1
                record_key = key
            else:
                if satisifed_criterion == 0:
                    record_key = key
        else:
            record_key = None
    return record_key


def sumAllNumberOfPost(dic_a, dic_b):
    '''
    Emerge two dictionaries to get final results.
    Parameters:
        dic_a, dic_b: Dictionary, two dictionaries to be merged.
    '''
    for key in dic_a.keys():
        for sub_key_a in dic_a[key]:
            if sub_key_a in dic_b[key]:
                dic_a[key][sub_key_a] += dic_b[key].pop(sub_key_a)
    if dic_b is not None:
        for key in dic_b.keys():
            for sub_key_b in dic_b[key]:
                dic_a[key][sub_key_b] = dic_b[key][sub_key_b]
    return dic_a

# Config multi-processing
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()
batch_size = 7000   # Number of lines processor 0 can read at a time
all_sum = {}    # Variable to hold final results
region_post = {}    # Number of posts of each region
region_hashtags = {}    # Hashtags and their corresponding occurrences
num_hash = {}  
grid_file = sys.argv[-2]
file_name = sys.argv[-1]

# default grid_file is melbGrid.json, file_name is 'bigTwitter.json'
if grid_file != 'melbGrid.json' and grid_file != 'tinyTwitter.json' and grid_file != 'smallTwitter.json' and grid_file != 'bigTwitter.json':
    grid_file = 'melbGrid.json'
if file_name != 'melbGrid.json' and file_name != 'tinyTwitter.json' and file_name != 'smallTwitter.json' and file_name != 'bigTwitter.json':
    file_name = 'bigTwitter.json'

if comm_rank == 0:
    start = time.time()

# read melbGrid.json file
if comm_rank == 0:
    with open(grid_file) as json_file:
        data = json.load(json_file)
        features = data["features"]
        grid = {}
        for i in features:
            info = {}
            x = i["properties"]
            info["xmin"] = x["xmin"]
            info["xmax"] = x["xmax"]
            info["ymin"] = x["ymin"]
            info["ymax"] = x["ymax"]
            grid[x["id"]] = info
            all_sum[x["id"]] = {}
            region_hashtags[x['id']] = {}
            num_hash[x['id']] = {}
else:
    grid = None
    chunks = None

# broadcast grid info
grid = comm.bcast(grid if comm_rank == 0 else None, root=0)

exit = False    # flag to 

with open(file_name, 'r') as twitters:

    # skip the first line since it does not contain twitter posts
    twitters.readline()

    while not exit:

        if comm_rank == 0:
            data = []
            # streaming pre-process and collect batch
            while len(data) < batch_size:
                line = twitters.readline().strip().strip(',')
                if line == ']}':
                    exit = True
                    break
                else:
                    data.append(line)

            # send batch
            chunks = [[] for _ in range(comm_size)]
            for i, chunk in enumerate(data):
                chunks[i % comm_size].append(chunk)
        else:
            chunks = None

        # scatter works to each processor
        local_data = comm.scatter(chunks, root=0)
        exit = comm.bcast(exit if comm_rank == 0 else None, root=0)

        result = {}

        for key in grid.keys():
            result[key] = {}
            result[key]['numberofPost'] = 0

        for i in range(len(local_data)):
            # turn one line into a json object, if possible; otherwise, skip
            try:
                line = json.loads(local_data[i])
            except:
                continue
            region = ''
            temp = line
            try:
                # find the coordinates in each twitter object
                if file_name != 'bigTwitter.json':
                    postCoordinate = temp["value"]["geometry"]["coordinates"]
                else:
                    postCoordinate = temp["doc"]["coordinates"]["coordinates"]
            except:
                continue

            # check whether there are coordinates
            if postCoordinate is not None:
                # identify the region of each post
                key = coordinatesJudgement(postCoordinate, grid)
                if key is not None:
                    result[key]['numberofPost'] += 1
                    region = key

            # check whether there are hashtags. 
            # Assume that ['doc']['entities']['hashtags'] area contains the information of hashtags
            if (temp['doc']['entities']['hashtags'] is not None) and (len(region) != 0):
                tmp_tag = {}
                # traverse hashtags
                for h in temp['doc']['entities']['hashtags']:
                    # ignore repeated hashtags
                    if h['text'].lower() not in tmp_tag:
                        if h['text'].lower() in result[region]:
                            result[region][h['text'].lower()] += 1
                        else:
                            result[region][h['text'].lower()] = 1
                        tmp_tag[h['text'].lower()] = 1

        # gather results from all processoors
        temp_sum = comm.reduce(result, root=0, op=sumAllNumberOfPost)

        # combine the result with the previous batch
        if comm_rank == 0:
            all_sum = sumAllNumberOfPost(all_sum, temp_sum)

        if comm_rank == 0 and exit is True:
            for a in all_sum:
                for h in all_sum[a]:
                    if h == 'numberofPost':
                        region_post[a] = all_sum[a][h]
                    else:
                        region_hashtags[a][h] = all_sum[a][h]
            #sort
            num_posts = sorted(region_post.items(),
                               key=lambda x: x[1], reverse=True)

            for region in region_hashtags:
                num_hash[region] = sorted(
                    region_hashtags[region].items(), key=lambda x: x[1], reverse=True) [:5]

    if comm_rank == 0:
        end = time.time()
        print("The total number of Twitter posts made in each box are shown in below in the descending order: ")
        for i in range(len(num_posts)):
            print(str(num_posts[i][0]) + ": " + str(num_posts[i][1]) + " posts")

        print("The top 5 hashtags in each grid cells: ")
        for key in num_hash.keys():
            print(str(key) + "'s top 5 hashtags are: ")
            print(num_hash[key])

        print("The running time is: " + str(end - start) + "s")
