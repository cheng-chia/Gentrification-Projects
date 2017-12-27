from image_classification_tf import *


import pymongo
from time import time
from datetime import datetime
import csv






class Classification:
    ''' setup '''
    # years = [2013, 2014, 2015]
    # sugarHouse = ("sugarHouse", [49035104400, 49035104800, 49035104900, 49035114100])
    # tractIDs = [49035104400, 49035104800, 49035104900, 49035114100]

    def __init__(self):
        pass



    ''' Connect to MongoDB '''

    def connect_MongoDB_SLC(self):
        client = pymongo.MongoClient()  # making a connection with MongoClient(connect on the default host and port)
        db = client.SLC  # connect to a certain database, SLC
        return db


    def QueryUrl_to_file(self,db,year_lnt,tractIDs):

        for year in year_lnt:

            filename = "image_link"+str(year)+".csv"
            with open (filename,"w") as f:   # create a writable csv file
                writer = csv.writer(f)
                header = ["ID", "URL"]
                writer.writerow(header)      # write header
                start = datetime(year, 1, 1, 0, 0, 0)  # set started time
                end = datetime(year+1, 1, 1, 0, 0, 0)  # set ended time



                for tractID in tractIDs:
                    # lst = []
                    theTract = db.census2010_tract_geo.find_one({"_id": str(tractID)})   # _id in census2010_tract_geo is string type!!!
                    geojson = theTract["geometry"]["rings"]
                    results = db.Instagram.aggregate([
                                                     {'$match' :{'$and':[
                                                     {"coordinates_geojson": {
                                                       "$geoWithin": {
                                                                        "$geometry": {
                                                                                     "type": "Polygon",
                                                                                        "coordinates": geojson
                                                                                     }
                                                                    }
                                                                            }
                                                                }
                                                              ,{'created_datetime_local': {'$gte': start, '$lt': end}},
                                                              ]}}
                                                    ], allowDiskUse=True)

                    ''' write the url into a text file'''
                    for document in results:
                        lst = []
                        if document["images"]:
                            lst.append(document["_id"])
                            lst.append(document['images']['standard_resolution']["url"])
                            writer.writerow(lst)
                            # f.write(document['_id']+" "+document['images']['standard_resolution']["url"] + "\n")






    def readImageFromURL(self,myURL):
        from PIL import Image
        from StringIO import StringIO
        import urllib
        import os

        my_Image = Image.open(StringIO(urllib.urlopen(myURL).read()))
        # urllib.urlopen().read(): to Open a network object denoted by a URL for reading. It will return a string
        # StringIO() is used to convert string into a file object.
        # Image.open() can open a file object
        save_path = os.path.join("./temp_images", "TEMP_IMAGE.jpg")
        # assign "./temp_images/TEMP_IMAGE.jpg" to save_path
        my_Image.save(save_path)
        # save my_Image under save_path
        save_path = os.path.abspath(save_path)
        # use os.path.abspath() to create a absolute path of save_path, and assign the absolute path to save_path

        return save_path


    def write_classification_file(self,year_list):
        maybe_download_and_extract()  # download and extract all data from trained model

        # year_list = ["2013","2014","2015"]

        year_index = 0
        for links in ["image_link2013.csv","image_link2014.csv","image_link2015.csv"]:  # run through the three text files with URL
            newfile = open(year_list[year_index]+"description.txt","w")  # create a new file

            print ("working on year: ", year_list[year_index] )

            count = 1           # for counting
            f = open(links)  # open the CSV file with ID and URL
            reader = csv.DictReader(f) # this kind of reader can read line into a dictionary, and set the header as key

            for line in reader:   # run through all line in text file. Now line is a dictionary
                print (count)

                # print line["ID"]
                # print line["URL"]
                url = line["URL"]   # URL is the value of line["URL"]
                ID = line["ID"]     # The image's ID is value of line["ID"]
                try:
                    img = self.readImageFromURL(url)   # read image from the URL, save it and return the saved path
                    # print ('ID:',ID)
                    with tf.Graph().as_default():
                    # Graph.as_default(): override the current default graph for the lifetime of the context

                        des_lst = run_inference_on_image(img)
                        # return a list with five lists that contain top 5 possible topics of the image and their possibility
                        print (des_lst)
                        # TopOneWord = des_lst[0][0]
                        # Probability = des_lst[0][-1]
                        # newfile.write(ID + ":")
                        # newfile.write('%s (posibility = %.5f)' % (TopOneWord, Probability) + "\n")

                        for topic in des_lst:
                        # run through five topic(which is a list with possible topic and its possibility)
                            newfile.write(ID + ":")  # write the image's ID
                            newfile.write('%s (score = %.5f)' % (topic[0], topic[1]) +"\n")  # write the possible topic and its score
                            print ('%s (score = %.5f)' % (topic[0], topic[1]))
                except Exception as e:  # catch the error
                    print (str(e.message))

                count += 1

            f.close()
            newfile.close()

            year_index += 1



    '''a function to get the lowest common synset'''

    def Get_LowestCommonSynset(self, word1, word2):
        from nltk.corpus import wordnet as wn
        synset1= wn.synsets(word1)[0]
        synset2 = wn.synsets(word2)[0]
        return synset1.lowest_common_hypernyms(synset2)


    def WriteTopOne_file(self,year_list):
        from nltk.corpus import wordnet as wn
        for year in year_list:

            with open(year + "description.txt","r") as f:     # open the description file for reading
                with open (year + "_FirstWord.txt","w") as nf:  # open a writable text file
                    lines = f.readlines()                  # get all lines
                    i = 0
                    for looping in range(len(lines)/5):    # in order to keep running 1/5 the length of total lines
                        lines[i].rstrip()           # for removing the new line(\n)
                        print (lines[i])
                        lst = lines[i].split(":")   # in order to separate the topics from id
                        nlst = lst[1].split(",")    # lst[1]: the topics without ids. lst[1].split(): separate the topic by "," to get possible topic without score
                        nnlst = nlst[0].split(" ")  # nlst[0]: the possible topic without scores. nlst[0],split(" "): to separate the compound word
                        first_word = nnlst[0]       # separate compound words and get the first word

                        coffee = wn.synsets("coffee")[0] #wn.synsets() will return a list of synset class object, and I only assign the first item (synset class) to coffee
                        alcohol = wn.synsets("alcohol")[0] #wn.synsets() will return a list of synset class object, and I only assign the first item (synset class) to alcohol
                        sunglass = wn.synsets("sunglass")[0] #wn.synsets() will return a list of synset class object, and I only assign the first item (synset class) to sunglass
                        try:
                            CommonSynset = self.Get_LowestCommonSynset(first_word,"coffee")[0]
                            # Get_LowestCommonSynset() returns a list that contains synset object, and I want the item in the list
                            if CommonSynset == coffee:
                                first_word = "coffee"
                        except:
                            pass

                        try:
                            CommonSynset = self.Get_LowestCommonSynset(first_word,"alcohol")[0]
                            # Get_LowestCommonSynset() returns a list that contains synset object, and I want the item in the list
                            if CommonSynset == alcohol:
                                first_word = "alcohol"
                        except:
                            pass
                        try:
                            CommonSynset = self.Get_LowestCommonSynset(first_word, "sunglass")[0]
                            # Get_LowestCommonSynset() returns a list that contains synset object, and I want the item in the list
                            if CommonSynset== sunglass:
                                first_word = "sunglasses"
                        except:
                            pass

                        nf.write(first_word+" ")
                        i += 5     # skip other potential topics of the images. for only taking the top 1 potential topic


    def Wordcloud(self,year_list):
        from wordcloud import WordCloud
        import matplotlib.pyplot as pl
        # years_list = ["2013","2014","2015"]
        # inputfile = ["2013_FirstWord.txt","2014_FirstWord.txt","2015_FirstWord.txt"]
        for year in year_list:

            text = open(year + "_FirstWord.txt").read()
            word_cloud = WordCloud(font_path='/home/karie/Desktop/tensorflow/cheri.regular.ttf', # the path I store the font style
                                   width=1600, height=800,  # the resolution of the word cloud
                                   scale=1,   # how different the font size is
                                   prefer_horizontal=1  # all horizontal
                                   # max_font_size= 128
                                   ).generate(text)

            pl.figure(figsize=(20, 10), facecolor='k') # create a figure that can contain the word cloud.
            pl.imshow(word_cloud)  # put the word cloud into the figure
            pl.axis("off")         # hide the axis

            # pl.show()

            pl.savefig('SugarHouse_image_topic_hiRes'+ year +'.png')


                       # facecolor='k', bbox_inches='tight')