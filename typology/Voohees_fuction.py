import psycopg2
import csv

"=== set up ==="

dbname='*******' # enter your database name
user='********' # enter your user name
password='***'  # enter your password
host='***,***,***,***' # enter the host
port='****' # enter the poet

t1_table = 'decennial_2000_census_in_SLCounty' # the name of table: need to establish the table as the ER diagram shown in the slides
t2_table = 'acs_2014_5_year_estimate_in_slcounty' # the name of table: need to establish the table as the ER diagram shown in the slides

get_2014_SC_status = "yes"
#  yes, you will get the social-economic status level in t2
# if no, you don't need to give file path for output_file_1
output_file_1 = 'voohees_typology.csv'

get_2000_SC_status = "yes"
#  yes, you will get the social-economic status level in t1
# if no, you don't need to give file path for output_file_2
output_file_2 = 'C:/Users/geoguser/Desktop/database_class/final project/voohees_2000_v3.csv'

output_file_3 = 'C:/Users/geoguser/Desktop/database_class/final project/voohees_typology_v3.csv'
# output_file_3 is the file path for gentrification typology


def connect_DB():
    try:
        conn= psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
        print ('connecting to '+dbname+' succeeded')
        return conn
    except Exception, e:
        print (e)
        print ('connecting to '+dbname+' failed')
        # print (errorcodes.lookup(e.pgcode))



def Voorhees(conn,t1_table,t2_table,get_2014_SC_status,output_file_1,get_2000_SC_status,output_file_2,output_file_3):

    cursor = conn.cursor()

    cursor.execute('select geoid from '+ t2_table+';')
    # get the all geoids in the table
    geoid = cursor.fetchall()

    total_geoid = []
    for (id,) in geoid:
        total_geoid.append(id)

    "================================================================="
    "generate socioeconomic status in 2014"
    "================================================================="


    cursor.execute('with t1 as (select (sum(B03002003)::numeric/sum(B01003001)::numeric) as total_avg_white from '+ t2_table+
                   ' ) select geoid from '+ t2_table+ '  where (B03002003::numeric/NULLIF(B01003001,0)::numeric)- '
                  '( select total_avg_white from t1) > 0;')
    # condition 1: whether the percentage of white people (non-Hispanic) of the tract above city average
    # query the geoids that match with condition 1
    factor1 = cursor.fetchall()

    factor1_lst = []
    for (id,) in factor1:
        factor1_lst.append(id)

    cursor.execute('with t1 as (select (sum(B02001003)::numeric/sum(B01003001)::numeric) as total_avg_black from '+ t2_table +
                   ' ) select geoid from acs_2014_5_year_estimate_in_slcounty where '
                   '(B02001003::numeric/NULLIF(B01003001,0)::numeric)-(select total_avg_black from t1) >0;')
    # condition 2: If the percentage of black people of the tract above city average
    # query the geoids that match with condition 2
    factor2 = cursor.fetchall()
    factor2_lst = []
    for (id,) in factor2:
        factor2_lst.append(id)

    cursor.execute('with t1 as (select (sum(B03002012)::numeric/sum(B01003001)::numeric) as total_avg_latino from ' +t2_table +
                   ' ) select geoid from '+t2_table + ' where (B03002012::numeric/NULLIF(B01003001,0)::numeric)- (select total_avg_latino from t1) >0;')
    # condtion 3: If the percentage of Latino of the tract above city average
    # query the geoids that match with condition 3
    factor3 = cursor.fetchall()
    factor3_lst = []
    for (id,) in factor3:
        factor3_lst.append(id)

    cursor.execute('with t1 as (select (sum(B01001020+B01001021+B01001022+B01001023+B01001024+B01001025+B01001044+'
                   'B01001045+B01001046+B01001047+B01001048+B01001049)::numeric/sum(B01003001)::numeric) as total_avg_elder from '+t2_table+
                   ' ) select geoid from '+ t2_table+ ' where ((B01001020+B01001021+B01001022+B01001023+B01001024+'
                   'B01001025+B01001044+B01001045+B01001046+B01001047+B01001048+B01001049)/NULLIF(B01003001,0)::numeric)-(select total_avg_elder from t1) >0;')
    # condition 4: If the percentage of Elderly (Age 65+) of the tract above city average
    # query the geoids that match with condition 4
    factor4= cursor.fetchall()
    factor4_lst = []
    for (id,) in factor4:
        factor4_lst.append(id)

    cursor.execute('with t1 as (select (sum(B01001004+B01001005+B01001006+B01001007+B01001028+B01001029+B01001030+'
                   'B01001031)::numeric/sum(B01003001)::numeric) as total_avg_child from '+ t2_table +
                   ' ) select geoid from '+ t2_table+'  where ((B01001004+B01001005+B01001006+B01001007+B01001028+'
                    'B01001029+B01001030+B01001031)/NULLIF(B01003001,0)::numeric)- ( select total_avg_child from t1) >0;')
    # condition 5: If the percentage of children (Age 5-19) of the tract above city average
    # query the geoids that match with condition 5
    factor5= cursor.fetchall()
    factor5_lst = []
    for (id,) in factor5:
        factor5_lst.append(id)



    cursor.execute('with t1 as (select (sum(B15003022+B15003023+B15003024+B15003025)::numeric/sum(B01003001)::numeric) '
                   'as total_avg_college from '+ t2_table+' ) select geoid from '+t2_table+' where '
                 '((B15003022+B15003023+B15003024+B15003025)/NULLIF(B01003001,0)::numeric)- (select total_avg_college from t1) >0;')
    #condition 6:  If the percentage of college education (Bachelor degree or higher) of the tract above city average
    # query the geoids that match with condition 6
    factor6= cursor.fetchall()
    factor6_lst= []
    for (id,) in factor6:
        factor6_lst.append(id)

    cursor.execute('with t1 as ( select COALESCE(avg(B19013001)) as total_avg_med_income from ' +t2_table +
                   ' ) select geoid from '+ t2_table+ ' where B19013001 - (select total_avg_med_income from t1) >0;')
    # condition 7: If the median household income of the tract above city average
    # query the geoids that match with condition 7
    factor7= cursor.fetchall()
    factor7_lst = []
    for (id,) in factor7:
        factor7_lst.append(id)

    cursor.execute('with t1 as (select (sum(B25008002)::numeric/sum(B25008001)::numeric) as total_pct_owner from '+t2_table+
                   ' ) select geoid from '+ t2_table+' where (B25008002::numeric/NULLIF(B25008001,0)::numeric) - (select total_pct_owner from t1)>0;')
    # condition 8: If the percentage of owner occupied of the tract above city average
    # query the geoids that match with condition 8
    factor8= cursor.fetchall()

    factor8_lst = []
    for (id,) in factor8:
        factor8_lst.append(id)

    cursor.execute('with t1 as (select COALESCE(avg(B25077001)) as total_avg_med_house_price from '+ t2_table+
                   ' ) select geoid from '+ t2_table+' where B25077001 -(select total_avg_med_house_price from t1) >0; ')
    # condtions 9: If median house value of the tract above city average
    # query the geoids that match with condition 9
    factor9= cursor.fetchall()
    factor9_lst =[]
    for (id,) in factor9:
        factor9_lst.append(id)

    cursor.execute('with t1 as (select (sum(B17010002)::numeric/sum(B17010001)::numeric) as total_avg_pct_poverty from '+
                   t2_table+' ) select geoid from '+ t2_table+' where (B17010002::numeric/NULLIF(B17010001,0)::numeric)-(select total_avg_pct_poverty from t1)>0;')
    #condition 10: If the percentage of family below poverty of the tract above city average
    # query the geoids that match with condition 10
    factor10= cursor.fetchall()
    factor10_lst = []
    for (id,) in factor10:
        factor10_lst.append(id)

    cursor.execute('with t1 as (select (sum(C24020003+C24020039)::numeric/sum(C24020001)::numeric) as total_avg_pct_manager from '+
                   t2_table+' ) select geoid from '+ t2_table+' where (C24020003+C24020039)::numeric/NULLIF(C24020001,0)::numeric - (select total_avg_pct_manager from t1) >0; ')
    #condition 11: If the percentage of manager occupation of the tract above city average
    # query the geoids that match with condition 11
    factor11= cursor.fetchall()
    factor11_lst =[]
    for (id,) in factor11:
        factor11_lst.append(id)


    cursor.execute('with t1 as (select (sum(B11003003+B11003010+B11003016)::numeric/sum(B17010001)::numeric) as total_avg_pct_with_children from ' +
                   t2_table +' ) select geoid from '+ t2_table+ '  where ((B11003003+B11003010+B11003016)::numeric/NULLIF(B17010001,0)::numeric) - '
                   '( select total_avg_pct_with_children from t1)>0;')
    # condition 12: If the percentage of family with children of the tract above city average
    # query the geoids that match with condition 12
    factor12= cursor.fetchall()
    factor12_lst = []
    for (id,) in factor12:
        factor12_lst.append(id)


    cursor.execute('with t1 as (select (sum(B14002006+B14002009+B14002012+B14002015+B14002018+B14002030+B14002033+B14002036+'
                   'B14002039+B14002042)::numeric/sum(B14002003+B14002027)::numeric) as total_avg_pct_private from '+
                   t2_table+' ) select geoid from '+t2_table+'  where ((B14002006+B14002009+B14002012+B14002015+B14002018+'
                  'B14002030+B14002033+B14002036+B14002039+B14002042)::numeric/NULLIF(B14002003+B14002027,0)::numeric) '
                   '- ( select total_avg_pct_private from t1)>0;  ')
    #condition 13: If the percentage of private school attendance (pre-K through 12) of the tract above city average
    # query the geoids that match with condition 12
    factor13= cursor.fetchall()
    factor13_lst = []
    for (id,) in factor13:
        factor13_lst.append(id)


    "==========================================================="
    "calculate the socioeconomic status scores in 2014 (t2)"
    "=========================================================="


    score_dict = {}  # the dictionary to store the scores in 2014(t2)
    for geoid in total_geoid:
        score_dict[geoid] = 0
        if geoid in factor1_lst:
            score_dict[geoid] += 1  #If the percentage of white people (non-Hispanic) of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1


        if geoid in factor2_lst:
            score_dict[geoid] -= 1 #If the percentage of black people of the tract above city average, the tract get score -1
        else:
            score_dict[geoid] += 1


        if geoid in factor3_lst:
            score_dict[geoid] -= 1 #If the percentage of Latino of the tract above city average, the tract get score -1
        else:
            score_dict[geoid] += 1


        if geoid in factor4_lst:  #If the percentage of Elderly (Age 65+) of the tract above city average, the tract get score -1
            score_dict[geoid] -= 1
        else:
            score_dict[geoid] += 1


        if geoid in factor5_lst:
            score_dict[geoid] -= 1 #If the percentage of children (Age 5-19) of the tract above city average, the tract get score -1
        else:
            score_dict[geoid] +=1


        if geoid in factor6_lst:
            score_dict[geoid] += 1 #If the percentage of college education (Bachelor degree or higher) of the tract above city average, the tract get score +1

        else:
            score_dict[geoid] -= 1


        if geoid in factor7_lst:
            score_dict[geoid] += 1 #If the median family income of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1


        if geoid in factor8_lst:
            score_dict[geoid] += 1  #If the percentage of owner occupied of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1


        if geoid in factor9_lst:
            score_dict[geoid] += 1  #If median house value of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1



        if geoid in factor10_lst:
            score_dict[geoid] -= 1  #If the percentage of family below poverty of the tract above city average, the tract get score -1
        else:
            score_dict[geoid] += 1



        if geoid in factor11_lst:
            score_dict[geoid] += 1  #If the percentage of manager occupation of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1



        if geoid in factor12_lst:
            score_dict[geoid] -= 1  #If the percentage of family with children of the tract above city average, the tract get score -1
        else:
            score_dict[geoid] += 1



        if geoid in factor13_lst:
            score_dict[geoid] += 1  #If the percentage of private school attendance (pre-K through 12) of the tract above city average, the tract get score +1
        else:
            score_dict[geoid] -= 1


    # score_dict : {'geoid': score, 'geoid': score, 'geoid': score,...}

    "=========================================="
    "get the 2014 socioeconomic status"
    "=========================================="
    if get_2014_SC_status == "yes":

        very_low =[]
        low = []
        middle =[]
        high = []


        for id in total_geoid:
            if score_dict[id] < -7:
                very_low.append(id)

            if -7<=score_dict[id]<=-1:
                low.append(id)

            if  1<= score_dict[id] <=7:
                middle.append(id)

            if score_dict[id] > 7:
                high.append(id)




        # the result
        final_results=[]
        for id in total_geoid:
            inner_lst=[]
            inner_lst.append(id)

            if id in very_low:
                inner_lst.append('very low')

            if id in low:
                inner_lst.append('low')

            if id in middle:
                inner_lst.append('middle')

            if id in high:
                inner_lst.append('high')


            final_results.append(inner_lst)

        "============================================="
        " write the 2014 social-economic result to CSV file"
        "============================================="


        with open(output_file_1, "wb") as f:         # create and open an empty CSV. f is a handle
            writer = csv.writer(f)              # create a writer
            header = ["geoid_bg", "level"]   # create a header
            writer.writerow(header)             # write header into CSV file
            writer.writerows(final_results)





    "=============================================="
    " generate socioeconomic status for 2000 (t1)"
    "=============================================="

    cursor.execute('with t1 as (select (sum(P012I001)::numeric/sum(P001001)::numeric) as total_avg_white from '+ t1_table+
                   ' ) select geoid from '+t1_table+' where (P012I001::numeric/NULLIF(P001001,0)::numeric)- ( select total_avg_white from t1) > 0;')

    Factor1 = cursor.fetchall()

    # whether the percentage of white people (non-Hispanic) of the tract above city average
    Factor1_lst = []
    for (id,) in Factor1:
        Factor1_lst.append(id)

    cursor.execute('with t1 as (select (sum(P007003)::numeric/sum(P001001)::numeric) as total_avg_black from '+ t1_table+
                   ' ) select geoid from '+ t1_table+' where (P007003::numeric/NULLIF(P001001,0)::numeric)-(select total_avg_black from t1) >0;')
    Factor2 = cursor.fetchall()
    # If the percentage of black people of the tract above city average
    Factor2_lst = []
    for (id,) in Factor2:
        Factor2_lst.append(id)

    cursor.execute('with t1 as (select (sum(P011001)::numeric/sum(P001001)::numeric) as total_avg_latino from '+t1_table+
                   ' ) select geoid from '+ t1_table+' where (P011001::numeric/NULLIF(P001001,0)::numeric)- (select total_avg_latino from t1) >0;')
    Factor3 = cursor.fetchall()
    # If the percentage of Latino of the tract above city average
    Factor3_lst = []
    for (id,) in Factor3:
        Factor3_lst.append(id)

    cursor.execute('with t1 as (select (sum(P012020+P012021+P012022+P012023+P012024+P012025+P012044+P012045+P012046+P012047+'
                   'P012048+P012049)::numeric/sum(P001001)::numeric) as total_avg_elder from '+ t1_table+
                   ' ) select geoid from '+ t1_table+' where ((P012020+P012021+P012022+P012023+P012024+P012025+P012044+'
                   'P012045+P012046+P012047+P012048+P012049)/NULLIF(P001001,0)::numeric)-(select total_avg_elder from t1) >0;')
    Factor4= cursor.fetchall()
    #If the percentage of Elderly (Age 65+) of the tract above city average
    Factor4_lst = []
    for (id,) in Factor4:
        Factor4_lst.append(id)

    cursor.execute('with t1 as (select (sum(P012004+P012005+P012006+P012007+P012028+P012029+P012030+P012031)::numeric/sum(P001001)::numeric) as '
                   'total_avg_child from '+ t1_table+' ) select geoid from '+t1_table+
                   ' where ((P012004+P012005+P012006+P012007+P012028+P012029+P012030+P012031)/NULLIF(P001001,0)::numeric)- ( select total_avg_child from t1) >0;')
    Factor5= cursor.fetchall()
    #If the percentage of children (Age 5-19) of the tract above city average
    Factor5_lst = []
    for (id,) in Factor5:
        Factor5_lst.append(id)



    cursor.execute('with t1 as (select (sum(P037015+P037016+P037017+P037018+P037032+P037033+P037034+P037035)::numeric/sum(P001001)::numeric) as '
                   'total_avg_college from '+ t1_table+' ) select geoid from '+ t1_table+
                   ' where ((P037015+P037016+P037017+P037018+P037032+P037033+P037034+P037035)/NULLIF(P001001,0)::numeric)- (select total_avg_college from t1) >0;')
    Factor6= cursor.fetchall()
    # If the percentage of college education (Bachelor degree or higher) of the tract above city average
    Factor6_lst= []
    for (id,) in Factor6:
        Factor6_lst.append(id)

    cursor.execute('with t1 as ( select COALESCE(avg(P053001)) as total_avg_med_income from '+ t1_table+
                   ' ) select geoid from '+t1_table+'  where P053001 - (select total_avg_med_income from t1) >0;')
    Factor7= cursor.fetchall()
    #If the median household income of the tract above city average
    Factor7_lst = []
    for (id,) in Factor7:
        Factor7_lst.append(id)

    cursor.execute('with t1 as (select (sum(H011002)::numeric/sum(H011001)::numeric) as total_pct_owner from '+
                   t1_table+' ) select geoid from '+t1_table+
                   '  where (H011002::numeric/NULLIF(H011001,0)::numeric) - (select total_pct_owner from t1)>0;')
    Factor8= cursor.fetchall()
    # If the percentage of owner occupied of the tract above city average
    Factor8_lst = []
    for (id,) in Factor8:
        Factor8_lst.append(id)

    cursor.execute('with t1 as (select COALESCE(avg(H085001)) as total_avg_med_house_price from '+
                   t1_table+' ) select geoid from '+t1_table+' where H085001 -(select total_avg_med_house_price from t1) >0; ')
    Factor9= cursor.fetchall()
    # If median house value of the tract above city average
    Factor9_lst =[]
    for (id,) in Factor9:
        Factor9_lst.append(id)

    cursor.execute('with t1 as (select (sum(P090001)::numeric/sum(P090002)::numeric) as total_avg_pct_poverty from '+
                   t1_table+' ) select geoid from '+ t1_table+' where (P090001::numeric/NULLIF(P090002,0)::numeric)-(select total_avg_pct_poverty from t1)>0;')
    Factor10= cursor.fetchall()
    #If the percentage of family below poverty of the tract above city average
    Factor10_lst = []
    for (id,) in Factor10:
        Factor10_lst.append(id)

    cursor.execute('with t1 as (select (sum(P050003+P050050)::numeric/sum(P050001)::numeric) as total_avg_pct_manager from '+
                   t1_table+' ) select geoid from '+t1_table+' where (P050003+P050050)::numeric/NULLIF(P050001,0)::numeric - (select total_avg_pct_manager from t1) >0; ')
    Factor11= cursor.fetchall()
    #If the percentage of manager occupation of the tract above city average
    Factor11_lst =[]
    for (id,) in Factor11:
        Factor11_lst.append(id)


    cursor.execute('with t1 as (select (sum(P020005+P020009+P020012)::numeric/sum(P014002)::numeric) as total_avg_pct_with_children from '+
                   t1_table+' ) select geoid from  '+t1_table+'  where ((P020005+P020009+P020012)::numeric/NULLIF(P014002,0)::numeric) -'
                   ' ( select total_avg_pct_with_children from t1)>0;')
    Factor12= cursor.fetchall()
    #If the percentage of family with children of the tract above city average
    Factor12_lst = []
    for (id,) in Factor12:
        Factor12_lst.append(id)


    cursor.execute('with t1 as (select (sum(P036005+P036008+P036011+P036014+P036017+P036028+P036031+P036034+P036037+P036040)::numeric/sum(P036001)::numeric) '
                   'as total_avg_pct_private from '+ t1_table+' ) select geoid from '+ t1_table+'  where ((P036005+P036008+P036011+P036014+P036017+P036028+'
                    'P036031+P036034+P036037+P036040)::numeric/NULLIF(P036001,0)::numeric) - ( select total_avg_pct_private from t1)>0;  ')
    Factor13= cursor.fetchall()
    #If the percentage of private school attendance (pre-K through 12) of the tract above city average
    Factor13_lst = []
    for (id,) in Factor13:
        Factor13_lst.append(id)

    "================================================="
    "calculate the social-economic status in 2000(t1)"
    "================================================="



    score_dict_2 = {} # the dictionary to store the scores in 2000(t1)
    for geoid in total_geoid:
        score_dict_2[geoid] = 0
        if geoid in Factor1_lst:
            score_dict_2[geoid] += 1  #If the percentage of white people (non-Hispanic) of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1


        if geoid in Factor2_lst:
            score_dict_2[geoid] -= 1 #If the percentage of black people of the tract above city average, the tract get score -1
        else:
            score_dict_2[geoid] += 1


        if geoid in Factor3_lst:
            score_dict_2[geoid] -= 1 #If the percentage of Latino of the tract above city average, the tract get score -1
        else:
            score_dict_2[geoid] += 1


        if geoid in Factor4_lst:  #If the percentage of Elderly (Age 65+) of the tract above city average, the tract get score -1
            score_dict_2[geoid] -= 1
        else:
            score_dict_2[geoid] += 1


        if geoid in Factor5_lst:
            score_dict_2[geoid] -= 1 #If the percentage of children (Age 5-19) of the tract above city average, the tract get score -1
        else:
            score_dict_2[geoid] +=1


        if geoid in Factor6_lst:
            score_dict_2[geoid] += 1 #If the percentage of college education (Bachelor degree or higher) of the tract above city average, the tract get score +1

        else:
            score_dict_2[geoid] -= 1


        if geoid in Factor7_lst:
            score_dict_2[geoid] += 1 #If the median family income of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1


        if geoid in Factor8_lst:
            score_dict_2[geoid] += 1  #If the percentage of owner occupied of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1


        if geoid in Factor9_lst:
            score_dict_2[geoid] += 1  #If median house value of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1



        if geoid in Factor10_lst:
            score_dict_2[geoid] -= 1  #If the percentage of family below poverty of the tract above city average, the tract get score -1
        else:
            score_dict_2[geoid] += 1



        if geoid in Factor11_lst:
            score_dict_2[geoid] += 1  #If the percentage of manager occupation of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1



        if geoid in Factor12_lst:
            score_dict_2[geoid] -= 1  #If the percentage of family with children of the tract above city average, the tract get score -1
        else:
            score_dict_2[geoid] += 1



        if geoid in Factor13_lst:
            score_dict_2[geoid] += 1  #If the percentage of private school attendance (pre-K through 12) of the tract above city average, the tract get score +1
        else:
            score_dict_2[geoid] -= 1





    "=========================================="
    "get the 2000 social-economic result"
    "=========================================="


    final_results_2=[]

    if get_2000_SC_status == "yes":

        very_low_2 =[]
        low_2 = []
        middle_2 =[]
        high_2 = []

        for id in total_geoid:
            if score_dict_2[id] < -7:
                very_low_2.append(id)

            if -7<=score_dict_2[id]<=-1:
                low_2.append(id)

            if  1<= score_dict_2[id] <=7:
                middle_2.append(id)

            if score_dict_2[id] > 7:
                high_2.append(id)

        inner_lst=[]
        for id in total_geoid:

            inner_lst.append(id)

            if id in very_low_2:
                inner_lst.append('very low')

            if id in low_2:
                inner_lst.append('low')

            if id in middle_2:
                inner_lst.append('middle')

            if id in high_2:
                inner_lst.append('high')


        final_results_2.append(inner_lst)

        "============================================="
        " write the 2014 social-economic result to CSV file"
        "============================================="


        with open(output_file_2, "wb") as f:         # create and open an empty CSV. f is a handle
            writer = csv.writer(f)              # create a writer
            header = ["geoid_bg", "level"]   # create a header
            writer.writerow(header)             # write header into CSV file
            writer.writerows(final_results_2)



    "============================================"
    "write the gentrification typology to csv file"
    "============================================"
    final_results_3 =[]


    for id in total_geoid:
        inner_lst3= []
        inner_lst3.append(id)
        if score_dict[id]-score_dict_2[id]>4:
            if (score_dict[id]+score_dict_2[id])/2 <=7:
                inner_lst3.append('increase, not gentrification')
            if (score_dict[id]+score_dict_2[id])/2 >7:
                inner_lst3.append(('increase, gentrification'))

        if -4<=score_dict[id]-score_dict_2[id]<=4:
            if (score_dict[id]+score_dict_2[id])/2 < -7:
                inner_lst3.append('no change, extreme poverty')
            if -7<=(score_dict[id]+score_dict_2[id])/2 <=-1:
                inner_lst3.append('no change, poverty')
            if 0<=(score_dict[id]+score_dict_2[id])/2 <=7:
                inner_lst3.append('no change, middle class')
            if (score_dict[id]+score_dict_2[id])/2 >7:
                inner_lst3.append('no change, upper class')
        if -7<=score_dict[id]-score_dict_2[id] <=-5:
            inner_lst3.append('decrease, mild')
        if -9<=score_dict[id]-score_dict_2[id] <=-8:
            inner_lst3.append('decrease, moderate')
        if score_dict[id]-score_dict_2[id] <=-10:
            inner_lst3.append('decrease, severe')
        final_results_3.append(inner_lst3)

    with open(output_file_3, "wb") as f:         # create and open an empty CSV. f is a handle
        writer = csv.writer(f)              # create a writer
        header = ["geoid_bg", "typology"]   # create a header
        writer.writerow(header)             # write header into CSV file
        writer.writerows(final_results_3)



    conn.commit()
    conn.close()



def main():
    conn = connect_DB()
    Voorhees(conn,t1_table,t2_table,get_2014_SC_status,output_file_1,get_2000_SC_status,output_file_2,output_file_3)



if __name__ == "__main__":
    main()

