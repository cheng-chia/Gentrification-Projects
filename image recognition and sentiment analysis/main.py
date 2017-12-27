
from libs.sentiment import*
from libs.classification import*
import csv


def main():

    '''Doing sentiment analysis part'''

    '''set up'''
    sugarhouse = [49035104400, 49035104800, 49035104900, 49035114100]
    years = ["2013", "2014", "2015"]
    year_lnt = [2013,2014,2015]


    '''sentiment analysis'''

    st = SentimentAnalysis()  # create a SentimentAnalysis object so that I can use its function

    db = st.connect_MongoDB_SLC()  # connect to SLC database


    st.query_caption_write_file(db,[2013, 2014, 2015],sugarhouse)
    # query captions from collections in SLC database,
    # and write the IDs and captions into text files:
    # caption_text_sugarhouse_withID2013.txt", "caption_text_sugarhouse_withID2014.txt",and "caption_text_sugarhouse_withID2015.txt"
    print ("complete the task that queries captions from the Instagram collection in database(SLC), and writes them to text files")
    print ("=======================================================================================")

    st.analyze_to_file()
    # start sentiment analysis, and write the result into CSV file:
    # sentiment_analysis_sugarhouse_2013.csv, sentiment_analysis_sugarhouse_2014.csv, and sentiment_analysis_sugarhouse_2015.csv
    print ("complete the task that analyzes the sentiment and writes them to CSV files")
    print ("=======================================================================================")


    for year in years:
        filename = "sentiment_analysis_sugarhouse_" + year + ".csv"
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            thelist = list(reader)     # read the csv file to list

            for line in thelist[1:]:   # in order to skip the header
                st.insert_sentiment_MongoDB(db,line[0],line[1])
                # db: connect to SLC, line[0]:ID, line[1]:the result of sentiment analysis
    print ("complete the task that writes the results of sentiment analysis to test collection in database(SLC)")







    '''image classification'''
    cls = Classification()   # create a Classification object so that I can use its function
    db = cls.connect_MongoDB_SLC()
    cls.QueryUrl_to_file(db,year_lnt,sugarhouse)
    # use spatial query to get URLs from the Instagram collection in SLC database
    # and write the IDs and URLs into three text files:
    # image_link2013.txt, image_link2014.txt and image_link2015.txt
    print ("the URLs of the images that tag in sugarhouse have been queried.")
    print ("The URL and its ID have been stored in CSV file")

    cls.write_classification_file(years)

    print ("the results of classification have been stored in text file")
    print ("===============================================================")

    cls.WriteTopOne_file(years)
    # reorganize the topics, and write top one possible topic into three text files

    print ("The top 1 possible topic of every image has been stored in text file")
    print ("================================================================")

    cls.Wordcloud(years)
    # create word clouds, and write them into three png files

    print ("Three word clouds have been created.")
    print ("The word clouds represent the main topic of the Instagram image in Sugarhose")



if __name__ == "__main__":
    main()