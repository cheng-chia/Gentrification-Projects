
import pymongo
import csv
import string

from time import time
from datetime import datetime

import re # for text cleaning
import nltk


"==================================================================================="
'''This code is used to 1) do spatial query and clean the Insatgram text stored in my Mongo database;
 2) output the text for each year or output the text for three year combined '''

''' function to remove emoji from text string '''
def clean_emoji(text):
    emoji_pattern = re.compile(u'('
    u'\ud83c[\udf00-\udfff]|'
    u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
    u'[\u2600-\u26FF\u2700-\u27BF])+',
    re.UNICODE)

    return (emoji_pattern.sub(r'', text)) # no emoji


''' function to remove non-english word '''
def clean_nonEng(text):
    return re.sub(r'\w*\d\w*', '', text).strip()
    # regex = re.compile('[^a-zA-Z]')
    # return regex.sub('', text)

sno = nltk.stem.SnowballStemmer('english')

"==================================================================================="
''' Connect to MongoDB '''
client = pymongo.MongoClient()  # making a connection with MongoClient(connect on the default host and port)
db = client.SLC  # connect to a certain database, SLC

''' setup '''

"==================================================================================="

'''take block-group ID'''

with open('C:/Users/geoguser/Desktop/thesis/method/anova_allwords_SLCity/gentrification_typos_SLCity.csv', 'rb') as f:
    reader = csv.reader(f)
    block_group_IDs = []
    for row in reader:
        # print row[0],row[1]
        if row[0]!= "geoid10_bg":              # remove the header: "GEOID10"
            block_group_IDs.append(row[0])   # create a list with block group's IDs
    print block_group_IDs

print len(block_group_IDs)
"==================================================================================="
"create a list with extreme users' ID. Extreme users are above the 90th percentile"

with open(r'C:\Users\geoguser\Desktop\thesis\method\data_cleaning\outliers_id_90.csv', 'rU') as f:
# with open('data_cleaning/outliers_id_90.csv', 'rU') as f:
    reader = csv.reader(f)
    myList = list(reader)
    filter_IDs = myList[1][1:]

extreme_user_lst = [ str(x) for x in filter_IDs ]  ### 90 percentile, 576.6 posts/per user
# print extreme_user_lst
# print type(extreme_user_lst[0])

"==================================================================================="

year_list = [2013,2014,2015]

for year in year_list:  # run through the years in year_list
    print "working on: ", year
    t_year = time()
    start = datetime(year, 1, 1, 0, 0, 0)  # set started year
    end = datetime(year + 1, 1, 1, 0, 0, 0)  # set ended year


    filename = "C:/Users/geoguser/Desktop/thesis/method/clustering_SLCity/redo_for_stemming/cleaned_caption_SLCity_" + str(year) + ".csv"  # create a file name
    with open(filename, "wb") as f:  # create a writable file
        writer = csv.writer(f)
        outer_lst=[]



        for BG_ID in block_group_IDs:  # tract is a string in a list
            t_BG = time()  # for checking time

            theBlockGroup = db.census2010_blockgroup_geo.find_one(
                        {"_id": BG_ID})
            # find the document with a certain block group id from collection "census2010_blockgroup_geo", and assign it to theBlockGroup
            # theBlockGroup is a cursor
            geojson = theBlockGroup["geometry"]["rings"]
            # find the geojson of the BG id, and assign it to geojson.
            # (geojson represents the boundary of the block group)
            results = db.Instagram.aggregate([
                        {'$match':
                            {'$and': [
                                {"coordinates_geojson": # the first condition
                                    {"$geoWithin":
                                        {"$geometry":
                                            {"type": "Polygon",
                                             "coordinates": geojson
                                             }
                                        }
                                    }
                                },
                                {'created_datetime_local': {'$gte': start, '$lt': end}},  # the second condition
                                ### !!! local time is the attribute create by myself, and it dose not exist in the raw data

                                {"user.id":{"$nin":extreme_user_lst}}
                            ]}
                        }
            ], allowDiskUse=True)

            inner_lst = []
            for document in results:
                if document["caption"]:  # if the "caption" exists in the document
                    text = clean_emoji(document["caption"]["text"].replace('\n', ' ').replace('\r', ' '))
                    text_lst = text.split()
                    com_lst = []
                    for word in text_lst:
                        com_lst.append(sno.stem(word))
                    text =' '.join(com_lst)
                    text = sno.stem(text)
                    # text = clean_nonEng(text)
                    text = text.encode("utf-8")
                    text = text.translate(None,string.punctuation)
                    text = text.lower()


                    inner_lst.append(text)
                    # this list will append all texts in the same block group in a certain year

            entire_text = ''.join(inner_lst)  # convert lst into a big string text
            outer_lst.append([str(year)+"_"+BG_ID,entire_text])

            t_BG_1 = time()
            t_BG_cost = t_BG_1-t_BG
            print ("Block group: " + BG_ID + " is done.")  # in order to check if a block group is finished
            print ( "it took: " + str(t_BG_cost))


        writer.writerow(["block_group_ID","captions"])
        writer.writerows(outer_lst)

"========================================================================" \
"get the 3 year combined data" \
"========================================================================"


# filename = "C:/Users/geoguser/Desktop/thesis/method/clustering_SLCity/merge_files/cleaned_caption_SLCity_3Y_stem.csv"  # create a file name
# with open(filename, "wb") as f:  # create a writable file
#     writer = csv.writer(f)
#     outer_lst=[]
#
#
#
#     for BG_ID in block_group_IDs:  # tract is a string in a list
#         t_BG = time()  # for checking time
#
#         theBlockGroup = db.census2010_blockgroup_geo.find_one(
#                     {"_id": BG_ID})
#         # find the document with a certain block group id from collection "census2010_blockgroup_geo", and assign it to theBlockGroup
#         # theBlockGroup is a cursor
#         geojson = theBlockGroup["geometry"]["rings"]
#         # find the geojson of the BG id, and assign it to geojson.
#         # (geojson represents the boundary of the block group)
#         results = db.Instagram.aggregate([
#                     {'$match':
#                         {'$and': [
#                             {"coordinates_geojson": # the first condition
#                                 {"$geoWithin":
#                                     {"$geometry":
#                                         {"type": "Polygon",
#                                          "coordinates": geojson
#                                          }
#                                     }
#                                 }
#                             },
#
#                             {"user.id":{"$nin":extreme_user_lst}}
#                         ]}
#                     }
#         ], allowDiskUse=True)
#
#         inner_lst = []
#
#         for document in results:
#             if document["caption"]:  # if the "caption" exists in the document
#                 text = clean_emoji(document["caption"]["text"].replace('\n', ' ').replace('\r', ' '))
#                 # print "0",text
#
#                 text_lst = text.split()
#                 com_lst = []
#                 for word in text_lst:
#                     com_lst.append(sno.stem(word))
#                 text =' '.join(com_lst)
#                 # print "1",text
#
#
#                 # print text
#                 text = sno.stem(text)
#                 # text = clean_nonEng(text)
#                 text = text.encode("utf-8")
#                 # text = document["caption"]["text"].replace('\n', ' ').replace('\r', ' ').encode(
#                 #         "utf-8") #replace break line, and transform text into human readable form, and write text
#                 text = text.translate(None,string.punctuation)
#
#                 text = text.lower()
#                 # print "2",text
#                 inner_lst.append(text)
#             # this list will append all texts in the same block group in a certain year
#
#         entire_text = ''.join(inner_lst)  # convert lst into a big string text
#         outer_lst.append([BG_ID,entire_text])
#
#         t_BG_1 = time()
#         t_BG_cost = t_BG_1-t_BG
#         print ("Block group: " + BG_ID + " is done.")  # in order to check if a block group is finished
#         print ( "it took: " + str(t_BG_cost))
#
#
#     writer.writerow(["block_group_ID","captions"])
#     writer.writerows(outer_lst)
#
