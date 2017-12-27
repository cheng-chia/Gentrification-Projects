
import pymongo

from time import time
from datetime import datetime


class SentimentAnalysis():
    def __init__(self):
        pass

    ''' Connect to MongoDB '''
    def connect_MongoDB_SLC(self):
        client = pymongo.MongoClient()  # making a connection with MongoClient(connect on the default host and port)
        db = client.SLC  # connect to a certain database, SLC
        return db

    '''query captions from MongoDB, and write the IDs and captions into text file'''
    def query_caption_write_file(self,db, year_list, TractID_list):
        # create a function with four arguments
        # db represents my database; year_list: a list of years; TractID_list: a list with tract id

        # TractID_list(SugarHouse) = [49035104400, 49035104800, 49035104900, 49035114100]
        # year_list = [2013, 2014, 2015]



        for year in year_list:  # run through the years in year_list

            filename = "caption_text_sugarhouse_withID" + str(year) + ".txt"  # create a file name
            with open(filename, "w") as f:  # create a writable file

                start = datetime(year, 1, 1, 0, 0, 0)  # set started year
                end = datetime(year + 1, 1, 1, 0, 0, 0)  # set ended year

                t0 = time()  # for checking time

                for tractID in TractID_list:  # run through all tract id in TractID_list
                    theTract = db.census2010_tract_geo.find_one(
                        {"_id": str(tractID)})  # (_id in census2010_tract_geo is string type!!!)
                    # find the document with a certain tract id from collection "census2010_tract_geo", and assign it to theTract
                    # theTract is a cursor
                    geojson = theTract["geometry"]["rings"]
                    # find the geojson of the tract id, and assign it to geojson.
                    # (geojson represents the boundary of the tract)
                    results = db.Instagram.aggregate([
                        {'$match': {'$and': [
                            {"coordinates_geojson": {  # the first condition
                                "$geoWithin": {
                                    "$geometry": {
                                        "type": "Polygon",
                                        "coordinates": geojson
                                    }
                                }
                            }
                            }
                            , {'created_datetime_local': {'$gte': start, '$lt': end}},  # the second condition
                        ]}}
                    ], allowDiskUse=True)

                    ### write the id and url into a text file'''

                    for document in results:
                        if document["caption"]:  # if the "caption" exists in the document
                            f.write(document["_id"] + " ")
                            f.write(document["caption"]["text"].replace('\n', ' ').replace('\r', ' ').encode(
                                "utf-8") + "\n")
                            # write text, replace break line, and transform text into human readable form, and write text

                    print ("tract: " + str(tractID) + " is done")  # in order to check if a tract is finished
            print (str(year) + " is done" ) # in order to check if a year is finished



    '''based on textblob's function, create a sentiment analysis function
    (use NaiveBayes analyzer, and Twitter movie reviews for training dataset)'''

    def sentiment_BY_analysis(self,sentence_string):
        from textblob import TextBlob
        from textblob.sentiments import NaiveBayesAnalyzer
        blob = TextBlob(sentence_string,
                        analyzer=NaiveBayesAnalyzer())  # assign a textblob object to blob. set the analyzer is native bayes analyzer
        return blob.sentiment


    '''based on textblob's function, create a sentiment analysis function
        (use Pattern analyzer, and Twitter movie reviews for training dataset)'''

    def sentiment_PT_analysis(self, sentence_string):
        from textblob import TextBlob
        from textblob.sentiments import NaiveBayesAnalyzer
        blob = TextBlob(sentence_string)  # assign a textblob object to blob, the default analyzer is pattern analyzer
        return blob.sentiment

    '''start sentiment analysis, and write the result into CSV file'''
    def analyze_to_file(self):
        ''' analyzing sentiment and write it to CSV '''
        import csv


        years = [2013, 2014, 2015]


        for year in years:  # run through three years
            filename = "caption_text_sugarhouse_withID" + str(year) + ".txt"  # assign a name to filename
            with open(filename, "r") as f:  # read the file with IDs and captions
                with open("sentiment_analysis_sugarhouse_" + str(year) + ".csv",
                          "wb") as SF:  # open a writable file to record the result of sentiment analysis
                    writer = csv.writer(SF)  # create a CSV writer
                    header = ["UID", "Sentiment", "Pos_p", "Neg_p"]  # assign a list with four items to header
                    writer.writerow(header)  # write the header for the CSV file
                    # count_list = []

                    counter = 0  # set a counter in order to count the sum
                    counter_positive = 0  # set a counter in order to count the number of positive words
                    counter_negative = 0  # set a counter in order to count the number of negative words

                    for line in f:  # run through all lines in f
                        line = line.rstrip('\n')  # remove the \n of every line
                        # (because the text file we read with invisible new line at the end of every line)
                        str2list = line.split(" ", 1)  # split line by the first " ", and make it a list

                        id = str2list[0]  # the first item in list str2list is id
                        text = str2list[1]  # the second item in list str2list is text

                        # print str2list

                        lst = []  # create an empty list for appending the result we want
                        counter += 1  # add one to counter
                        try:  # for catching errors
                            sentiment_result = self.sentiment_BY_analysis(text)  # assign the result of sentiment analysis to sentiment_result

                            # counter += 1  # add one to counter
                            print (counter)  # for us to check which line is being analyzed

                            lst.append(id)  # append userID into lst
                            lst.append(sentiment_result[0])  # append pos/neg
                            lst.append(sentiment_result[1])  # append the probability of being positive
                            lst.append(sentiment_result[2])  # append the probability of being negative

                            # print lst

                            writer.writerow(lst)  # write the list of results to CSV file

                            if sentiment_result[0] == "pos":  # if the result is positive, add one to counter_positive
                                counter_positive += 1
                            else:  # in else situation, add one to counter_negative
                                counter_negative += 1

                        except Exception as e:  # for catching errors
                            print (counter)
                            print (e)

                    print ("The percentage of positive sentences is: ", (float(counter_positive)/ counter))
                    print ("The percentage of negative sentences is: ", (float(counter_negative)/ counter))
                    print (("{} year is done.").format(str(year)))



    '''insert a new sub-field(the result of sentiment analysis into MongoDB)'''
    def insert_sentiment_MongoDB(self, db, ID, description):

        db.Instagram.find_one_and_update({"_id": ID},  # the filter
                               {'$set': {"caption.sentiment": {"sentiment": description}}})
        # create a new sub field and insert new information
        # if the "_id" is ID, create a dictionary--Sentiment. In Sentiment, key is "sentiment" and value is description



    '''pass new training data set to Naive Bayes Classifier, and use the new classifier to
    do sentiment analysis'''
    def train_and_sentimentAnalysis(self,training_data_path,text):
        # This function is based on textblob's function
        # training_data_path: the training data set's path
        # text: the text that is going to be analyzed
        '''This function will return a list:["pos"/"neg",prob_pos,prob_neg]'''
        from textblob.classifiers import NaiveBayesClassifier
        result_lst = []
        with open (training_data_path,"r") as f:
            NewClassifier = NaiveBayesClassifier(f,format = "csv")
            result = NewClassifier.classify(text)
            result_lst.append(result)
            result_lst.append(round(result.prob("pos"),2)) # round the probability to 2 decimal place
            result_lst.append(round(result.prob("neg"),2)) # round the probability to 2 decimal place
            return result_lst


