

# This code is to query and aggregate data for having a better understanding of the temporal pattern of Instagram data,
# the code also create a space time cube for one-month data in my study area, the space time cube tell us what
# the Instagram dataset look like every day at the same location

# Import system modules
import arcpy

import pymongo, datetime, csv, json
from pymongo import MongoClient, GEOSPHERE
from time import time
from time import strftime
from datetime import datetime
import csv
import shapefile
"==================================================================================="
''' Connect to MongoDB '''
"==================================================================================="
def connect_DB():
    client = MongoClient()
    db = client.SLC
    return db


"==================================================================================="
"create a list with extreme users' ID. Extreme users are above the 90th percentile"
"==================================================================================="
def extreme_user(db,extreme_user_file):

    with open(extreme_user_file, 'rU') as f:
    # with open('data_cleaning/outliers_id_90.csv', 'rU') as f:
        reader = csv.reader(f)
        myList = list(reader)
        filter_IDs = myList[1][1:]

    extreme_user_lst = [ str(x) for x in filter_IDs ]  ### 90 percentile, 576.6 posts/per user
    return  extreme_user_lst
    # print extreme_user_lst
    # print type(extreme_user_lst[0])


"==================================================================================="
"query hourly posts in each year and write to csv file"
"==================================================================================="

def hour_pattern_year(db,extreme_user_lst,year_lst):



    '''  Define time-range for time query '''
    for year in year_lst:

        final_results =[]
        start = datetime(year, 1, 1, 0, 0, 0)
        end = datetime(year+1, 1, 1, 0, 0, 0)


        ''' Aggregation '''
        t0 = time()


        total = db.Instagram.aggregate([
            {'$match':
                {'$and':[
                    # {"coordinates_geojson":
                    #     {"$geoWithin":
                    #         {"$geometry":
                    #             {"type": "Polygon",
                    #              "coordinates": geojson}
                    #          }
                    #      }
                    #  },
                    {'created_datetime_local': {'$gte': start, '$lt': end}},
                    {"user.id":{"$nin":extreme_user_lst}}
                ]}
            },
            {'$project':
                {'_id':0,
                 "dayHour": {"$hour" : "$created_datetime_local"}
                 }

            },
            {'$group':
                {
                    '_id': "$dayHour",
                    'count': {'$sum': 1 }
                }
             }

            ],allowDiskUse=True)


        print "start to write file"



        for document in total:
            print year, document["_id"],document["count"]

        # append sum of posts to final list
            final_results.append([year,document["_id"],document["count"]])


        """ write word-count per year to CSV"""

        filename = "total_hourly_pattern_{}".format(year) + ".csv"
        with open(filename, "wb") as f:         # create and open an empty CSV. f is a handle
            writer = csv.writer(f)              # create a writer
            header = ["year", "hour","counts"]   # create a header
            writer.writerow(header)             # write header into CSV file
            writer.writerows(final_results)
            print "one year finished"

"==================================================================================="
"query weekly posts in each year and write to csv file"
"==================================================================================="

def week_pattern_year(db,extreme_user_lst,year_lst):



    '''  Define time-range for time query '''
    for year in year_lst:

        final_results =[]
        start = datetime(year, 1, 1, 0, 0, 0)
        end = datetime(year+1, 1, 1, 0, 0, 0)


        ''' Aggregation '''
        t0 = time()


        total = db.Instagram.aggregate([
            {'$match':
                {'$and':[
                    # {"coordinates_geojson":
                    #     {"$geoWithin":
                    #         {"$geometry":
                    #             {"type": "Polygon",
                    #              "coordinates": geojson}
                    #          }
                    #      }
                    #  },
                    {'created_datetime_local': {'$gte': start, '$lt': end}},
                    {"user.id":{"$nin":extreme_user_lst}}
                ]}
            },
            {'$project':
                {'_id':0,
                 "dayWeek": {"$dayOfWeek" : "$created_datetime_local"}
                 }

            },

            {'$group': {
                        '_id': "$dayWeek",
                        'count': {'$sum': 1 }
                        }
             }
            ],allowDiskUse=True)


        print "start to write file"



        for document in total:
            print year, document["_id"],document["count"]

        # append sum of posts to final list
            final_results.append([year,document["_id"],document["count"]])


        """ write word-count per year to CSV"""

        filename = "total_weekly_pattern_{}".format(year) + ".csv"
        with open(filename, "wb") as f:         # create and open an empty CSV. f is a handle
            writer = csv.writer(f)              # create a writer
            header = ["year", "weekdate","counts"]   # create a header
            writer.writerow(header)             # write header into CSV file
            writer.writerows(final_results)
            print "one year finished"

"==================================================================================="
"query total posts in a defined time and write to csv file"
"==================================================================================="

def total_post_points(db,extreme_user_lst,year_start, month_start, date_start,year_end,month_end,date_end):



    '''  Define time-range for time query '''

    final_results =[]
    start = datetime(year_start, month_start, date_start, 0, 0, 0)
    end = datetime(year_end, month_end, date_end, 0, 0, 0)


    ''' Aggregation '''

    total = db.Instagram.aggregate([
        {'$match':
            {'$and':[

                {'created_datetime_local': {'$gte': start, '$lt': end}},
                {"user.id":{"$nin":extreme_user_lst}}
            ]}
        }


        ],allowDiskUse=True)
    print "start to write file"

    ''' run through all documents in the return results'''
    for document in total:

        final_results.append([document["_id"],
                              document['coordinates']['coordinates']['coordinates'][0],
                              document['coordinates']['coordinates']['coordinates'][1],
                              document["created_datetime_local"]])


        """ write word-count per year to CSV"""

        filename = "all_points.csv"
        with open(filename, "wb") as f:         # create and open an empty CSV. f is a handle
            writer = csv.writer(f)              # create a writer
            header = ["id", "x","y","local_time"]   # create a header
            writer.writerow(header)             # write header into CSV file
            writer.writerows(final_results)

"==================================================================================="
" write csv file to shapefile 1"
"==================================================================================="

def csv_point_shapefile(existing_shapefile,existing_csv,object_type):
    cursor = arcpy.InsertCursor(existing_shapefile)
    with open(existing_csv, 'rb') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Create the feature
            feature = cursor.newRow()

            # Add the point geometry to the feature
            vertex = arcpy.CreateObject(object_type)
            vertex.X = row['x']
            vertex.Y = row['y']
            feature.shape = vertex

            # Add attributes
            # feature.ID = row[id]
            # feature.LOCALTIME = row[time]

            # write to shapefile
            cursor.insertRow(feature)

    # clean up
    del cursor

"==================================================================================="
" write csv file to shapefile 2"
"==================================================================================="

''' funtion to generate a .prj file'''
def getWKT_PRJ (epsg_code):
    import urllib
    wkt = urllib.urlopen("http://spatialreference.org/ref/epsg/{0}/prettywkt/".format(epsg_code))
    remove_spaces = wkt.read().replace(" ","")
    output = remove_spaces.replace("\n", "")
    return output

def csv_shapefile(existing_csv,out_shp_file):

    shp = shapefile.Writer(shapefile.POINT) # create a shapefile writer
    shp.autoBalance = 1  # for every record there must be a corresponding geometry.

    shp.field("ID", "C") # create the field names and data type
    shp.field("X", "C")
    shp.field("Y", "C")
    shp.field("LOCALTIME", "C")
    with open(existing_csv, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        # skip the header
        next(reader, None)
        counter = 0
        for row in reader:
            id= row[0]
            x = row[1]
            y= row[2]
            time = row[3]
            # create the point geometry
            shp.point(float(x), float(y))
            # add attribute data
            shp.record(id, x, y, time)
            counter+=1
            print "add"+str(counter)+"point"
        # save the Shapefile
        shp.save(out_shp_file)
        # create a projection file
        prj = open(out_shp_file, "w")
        epsg = getWKT_PRJ("4326")  # give spatial reference: 4326 is WGS84
        prj.write(epsg)
        prj.close()



def projection(in_file, out_file, coordinate_sys):

    out_coordinate_system = arcpy.SpatialReference(coordinate_sys)
    # run the tool
    arcpy.Project_management(in_file, out_file, out_coordinate_system)




def main():
    # --- set up ---
    db = connect_DB()
    extreme_user_file = r'C:\Users\apple\Documents\thesis_20171005\method\data_cleaning\outliers_id_90.csv'
    extreme_user_lst = extreme_user(db,extreme_user_file)

    # query data and write to csv files
    year_lst = [2013,2014,2015] # setup years
    hour_pattern_year(db,extreme_user_lst,year_lst) # write hourly pattern per year to csv file
    week_pattern_year(db,extreme_user_lst,year_lst) # write weekly pattern per year to csv file
    total_post_points(db,extreme_user_lst,2013,7,1,2013,7,31) #write posts to csv file



    # ---- this is another way to write csv to shapefile ----
    # ShapfileName = "AllPost_2014.shp" # define the shapefile name
    # arcpy.CreateFeatureclass_management(r'C:\Users\apple\Documents\codes_karie\space_time',ShapfileName , "POINT") #create new shapefile
    # csvfile = "all_points_2014.csv"
    # csv_point_shapefile(ShapfileName,csvfile, "Point") # write the csv attributes to the empty shapefile

    # --- write csv file with 2014 points to shapefile ----
    ShapfileName = "AllPost_2014.shp" # define the shapefile name
    csvfile = "all_points_2014.csv"
    csv_shapefile(csvfile,ShapfileName)

    # --- give spatial reference---
    input_features = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\point_2014.shp"
    output_feature_class = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\point_2014_4302.shp"
    spatial_reference = "NAD 1983 StatePlane Utah Central FIPS 4302 Feet (US Feet)"
    # projection: WGS84 to tatePlane Utah Central FIPS 4302 feet
    projection(input_features, output_feature_class, spatial_reference)

    # --- clip points with study area ---
    study_area = r"C:\Users\apple\Documents\thesis_20171005\method\clustering_SLCity\typology_city\shapefile\kw_norm_pop.shp"
    arcpy.Clip_analysis(output_feature_class, study_area, r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\point_study.shp")


    # --- spatial join: let point data get census block group ID ---
    input_point = study_area
    join_features = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\point_study.shp" # the points to be spatial joined
    output_joined = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\points_area_geoid.shp"
    arcpy.SpatialJoin_analysis(input_point, join_features, output_joined)


    # ---create a space time cube ---

    output_cube = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\points.nc"
    arcpy.CreateSpaceTimeCube_stpm(input_point, output_cube, "local_time", "#", "1 Days",
                                    "End time", "#", "3 Miles", "#", "HEXAGON_GRID")

    # --- visualize cube in 3d---
    cube_shp = r"C:\Users\apple\Documents\codes_karie\space_time\point_shp\cube.shp"
    arcpy.VisualizeSpaceTimeCube3D_stpm(output_cube, "COUNT", "VALUE",cube_shp)






















