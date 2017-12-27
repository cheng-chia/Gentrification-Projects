
import graphlab as gl
import math
import pandas as pd
import numpy as np
import scipy
from scipy.spatial.distance import pdist, squareform
from scipy.cluster.hierarchy import ward, dendrogram
from time import time
import matplotlib.pyplot as plt
import matplotlib as mpl
from scipy.cluster.hierarchy import fcluster
import csv

"=========================================================="
'''This code is used to do text clustering. Before running this code, a file which contains
 Instagram text of each census block group need to be prepared. '''


"=========================================================="
"Setting up"
"=========================================================="

# file_name = "./combined_text_user.csv"
file_name ="./combined_text_user_3Y.csv"
k=7   # choose how many groups the census block groups will be divide into


out_file = "text_clustering.csv"

"=========================================================="
"load csv file as SFrame type. And assign it to document"
"=========================================================="

document = gl.SFrame.read_csv(file_name, header=True,na_values=[' '],column_type_hints=str,error_bad_lines=True) # load csv file and convert it into SFrame type
# print document
"====================================================================="
"Calculate word counts(bag of words)based on the column: corpus"
"====================================================================="

document['word_count'] = gl.text_analytics.count_words(document['captions'])
# create a new column named word_count containing a dictionary of word counts

"====================================================================="
"Remove words that rarely appears, and clean stopwords"
"====================================================================="

docs = document['word_count'].dict_trim_by_values(3)
# remove all words do not occur at least three times in each document using
# docs = document['word_count']

document["cleaned_word_count"] = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

"====================================================================="
"TF-IDF"
"====================================================================="
document["cleaned_tfidf"] = gl.text_analytics.tf_idf(document['cleaned_word_count'])
print document["cleaned_tfidf"]
print document["unique_user"]




"====================================================================="
"Normalization 1"
"====================================================================="

lst = []
for i,s in zip(document['cleaned_tfidf'],document['unique_user']):
    temp = {}
    for key, value in i.iteritems():
        temp[key]= value/float(s)

    lst.append(temp)


document["cleaned_tfidf_norm1"]= lst
print document
# print document["cleaned_tfidf"]

# print document["cleaned_tfidf_norm1"]
"====================================================================="
"Create BOW for each region, fill 0 for non-exist, so can match the header of words"
"====================================================================="


"create a bag of word list with all word in Salt Lake County"
def createBagOfWords(arr):    # a function to create a list with all of the words in the corpus
    # t0 = time()
    BagOfWords = []
    for item in arr:          # run through all dictionary in the column(SArray)
        for key in item.keys(): # run through all keys in the dictionary
            if key not in BagOfWords:  # if the key is not in the created list, append the word
                BagOfWords.append(key)
    # print time() - t0
    return BagOfWords

"create bag of word"
BOW = createBagOfWords(document["cleaned_word_count"])  # get a list(BOW) with all of words in entire corpus
BOW.sort()     # sort the list



"a function to create a list with word counts. if the word is not in the document, the count is zero"
def dictToVectorList(dict,BOW):
    BOW_count_BG = []
    for word in BOW: # run through all words in BOW
        BOW_count_BG.append(dict.get(word,0))
        # if the word in BOW noes not appear in the dict, return zero.
        # if the word in BOW also appear in the dict, return the value(word frequency)
        # append all these result to list(BOW_count_track)
    return BOW_count_BG

document["word_freq_withZero"] = document["cleaned_tfidf_norm1"].apply(lambda x: dictToVectorList(x,BOW))
# print document["word_freq_withZero"]
"================================================================================================="
"remove all zeros' block group"
"================================================================================================="

SArray_all_zero_bg = document[document["word_freq_withZero"].apply(lambda x: all(value == 0 for value in x))]
# print document[document["word_freq_withZero"].apply(lambda x: all(value == 0 for value in x.values()))]

list_BG_all =  list(document['block_group_ID'])
list_BG_zero = list(SArray_all_zero_bg['block_group_ID'])
Non_zero_bg_ID =  [item for item in list_BG_all if item not in list_BG_zero]


##### transform SArray into Numpy array
myNPArr = document["word_freq_withZero"].to_numpy()

#####  remove all-zero row
myNPArr = myNPArr[~(myNPArr==0).all(1)]

"================================================================"
"create a cosine distance matrix"
"================================================================"
res = squareform(pdist(myNPArr,'cosine'))  # cosine dissimilarity matrix

res = 1 -res                          # 1-res top make it as cosine similarity

"==========================================================="
"run hierarchical clustering"
"==========================================================="

titles = document['block_group_ID']
linkage_matrix = ward(res) #define the linkage_matrix using ward clustering pre-computed distances
print linkage_matrix

group_result = fcluster(linkage_matrix, k, criterion='maxclust').tolist()

"==========================================================="
"write results to CSV"
"==========================================================="



header = ["BG_ID","Group_ID"]
with open(out_file, 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(header)

    for BG_ID, Group_ID in zip(Non_zero_bg_ID ,group_result):
        writer.writerow([BG_ID, Group_ID])




