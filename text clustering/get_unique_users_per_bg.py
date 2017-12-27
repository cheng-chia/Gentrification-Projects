import pymongo
import csv
from datetime import datetime
from time import time
from collections import Counter

"==================================================================================="
''' This code is used to remove the extreme users and count how many unique users in a census block group every year'''



''' Connect to MongoDB '''
client = pymongo.MongoClient()  # make a connection with MongoClient(connect on the default host and port)
db = client.SLC  # connect to a certain database, SLC


''' setup '''
"==================================================================================="
'''take block-group ID'''  "block_group IDs will be a list with total bg ID"
"==================================================================================="
block_group_IDs = []
with open('2010_block_groups.csv', 'rU') as f:
    reader = csv.reader(f)
    for row in reader:
        # print row[0],row[1]
        if row[0]!= "GEOID10":              # remove the header: "GEOID10"
            block_group_IDs.append(row[0])   # create a list with BG IDs
# print block_group_IDs

"==================================================================================="
"create a list with extreme users' ID. Extreme users are above the 90th percentile"
"==================================================================================="

with open(r'C:\Users\geoguser\Desktop\thesis\method\data_cleaning\outliers_id_90.csv', 'rU') as f:
# with open('data_cleaning/outliers_id_90.csv', 'rU') as f:
    reader = csv.reader(f)
    myList = list(reader)
    filter_IDs = myList[1][1:]

extreme_user_lst = [ str(x) for x in filter_IDs ]  ### 90 percentile, 576.6 posts/per user
# print extreme_user_lst
# print type(extreme_user_lst[0])


''' setup years '''
years = [2013,2014,2015]



'''  Define time-range for time query '''
for year in years:

    final_results =[]
    start = datetime(year, 1, 1, 0, 0, 0)
    end = datetime(year+1, 1, 1, 0, 0, 0)


    ''' Aggregation '''
    t0 = time()

    for BG_ID in block_group_IDs:

        BG_UU_count = 0

        theBlockGroup= db.census2010_blockgroup_geo.find_one({"_id": BG_ID}) # get the specific document with BG_ID
        geojson = theBlockGroup["geometry"]["rings"]                         # get the specific geoJson of a block group


        total = db.Instagram.aggregate([
            {'$match':
                {'$and':[
                    {"coordinates_geojson":
                        {"$geoWithin":
                            {"$geometry":
                                {"type": "Polygon",
                                 "coordinates": geojson}
                             }
                         }
                     },
                    {'created_datetime_local': {'$gte': start, '$lt': end}},
                    # !! local time is a attribute created by myself, and it dose not exist in the raw data

                    {"user.id":{"$nin":extreme_user_lst}}
                ]}
            },

            ###### !! This will create an Array of user.ids, can use Counter() later to get unique users counts ####
            {'$group': {
                '_id': 0,
                'userID': {'$addToSet': "$user.id"}
            }}


            ],allowDiskUse=True)



        for result in total:

            BG_UU_count = len(Counter(result['userID']))## Unique Users
            print BG_ID
            # unique_user = len(Counter(result['userID']))## Unique Users
            # final_results.append([BG_ID,unique_user])

        final_results.append([str(year)+"_"+BG_ID,BG_UU_count])

        print " "
        print "Finish a block group"
        print " "



    "write unique users per BG into a CSV file"

    filename = "./"+"unique_user_BG_test_{}".format(year) + ".csv"
    with open(filename, "wb") as f:         # create and open an empty CSV. f is a handle
        writer = csv.writer(f)              # create a writer
        header = ["block_group_id", "unique_user"+ str(year)]   # create a header
        writer.writerow(header)             # write header into CSV file
        writer.writerows(final_results)



    print "Finish year: ", year





#
# final_results =[]
#
#
#
# ''' Aggregation '''
# t0 = time()
#
# for BG_ID in block_group_IDs:
#
#     BG_UU_count = 0
#
#     theBlockGroup= db.census2010_blockgroup_geo.find_one({"_id": BG_ID}) # get the specific document with BG_ID
#     geojson = theBlockGroup["geometry"]["rings"]                         # get the specific geoJson of a block group
#
#
#     total = db.Instagram.aggregate([
#         {'$match':
#             {'$and':[
#                 {"coordinates_geojson":
#                     {"$geoWithin":
#                         {"$geometry":
#                             {"type": "Polygon",
#                              "coordinates": geojson}
#                          }
#                      }
#                  },
#
#                 {"user.id":{"$nin":extreme_user_lst}}
#             ]}
#         },
#
#         ###### !! This will create an Array of user.ids, can use Counter() later to get unique users counts ####
#         {'$group': {
#             '_id': 0,
#             'userID': {'$addToSet': "$user.id"}
#         }}
#
#
#         ],allowDiskUse=True)
#
#
#
#     for result in total:
#
#         BG_UU_count = len(Counter(result['userID']))## Unique Users
#         print BG_ID
#         # unique_user = len(Counter(result['userID']))## Unique Users
#         # final_results.append([BG_ID,unique_user])
#
#     final_results.append([BG_ID,BG_UU_count])
#
#     print " "
#     print "Finish a block group"
#     print " "
#
#
#
# "write unique users per BG into a CSV file"
#
# filename = "./unique_user_BG_3Y.csv"
# with open(filename, "wb") as f:         # create and open an empty CSV. f is a handle
#     writer = csv.writer(f)              # create a writer
#     header = ["block_group_id", "unique_user"]   # create a header
#     writer.writerow(header)             # write header into CSV file
#     writer.writerows(final_results)
#


