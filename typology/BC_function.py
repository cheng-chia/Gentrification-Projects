
import psycopg2
import statistics
import scipy.stats as ss
import csv


dbname='*******' # enter your database name
user='********' # enter your user name
password='***'  # enter your password
host='***,***,***,***' # enter the host
port='****' # enter the poet

output_file = 'bostic_typology.csv'
t1_table = 'decennial_2000_census_in_SLCounty' # the name of table: need to establish the table as the ER diagram shown in the slides
t2_table = 'acs_2014_5_year_estimate_in_slcounty' # the name of table: need to establish the table as the ER diagram shown in the slides




def connect_DB():
    try:
        conn= psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
        print ('connecting to '+dbname+' succeeded')
        return conn
    except Exception, e:
        print (e)
        print ('connecting to '+dbname+' failed')
        # print (errorcodes.lookup(e.pgcode))

def Bostic_Martin(conn,t1_table,t2_table,output_file):

    cursor = conn.cursor()
    cursor.execute('select geoid, P053001 from ' + t1_table +';')
    income = cursor.fetchall()

    "differentiate gentrifiable and non-gentrifiable areas"

    income_lst =[]
    total_geoid_lst = []
    for (geoid,i) in income:
        income_lst.append(i)
        total_geoid_lst.append(geoid)
    med_income = statistics.median(income_lst)  #get the median of median income

    gentrifiable_geoid = []
    NonGentrifiable_geoid = []
    for (geoid,i) in income:
        if i < med_income:
            gentrifiable_geoid.append(geoid)
        else:
            NonGentrifiable_geoid.append(geoid)

    gentrifiable_geoid_tuple = tuple(gentrifiable_geoid)
    gentrifiable_geoid_text = str(gentrifiable_geoid_tuple)

    "get the variables we need for identifying gentrifying areas "

    cursor.execute('select geoid, (b15003022 + b15003023 + b15003024 + b15003025):: numeric/NULLIF(B01003001,0)::numeric as college from '+
                   t2_table + ' where geoid in ' + gentrifiable_geoid_text +
                   ' and ((b15003022 + b15003023 + b15003024 + b15003025):: numeric/NULLIF(B01003001,0)::numeric) NOTNULL;')
    factor1 = cursor.fetchall()
    #share of persons 25 or older with a college degree at time t2
    # out put : [(geoid,college),(geoid,college),(geoid,college)...]
    lst1=[]
    for (x,y) in factor1:
        lst1.append(y)


    cursor.execute('with t1 as (select COALESCE(avg(P053001)) as avg_med_income from ' +t1_table+ ' where geoid in '+ gentrifiable_geoid_text +
                   ')' + 'select geoid, B19013001/ (select avg_med_income from t1) as income_ratio from '+ t2_table +
                   ' where B19013001/ (select avg_med_income from t1) NOTNULL and geoid in '+gentrifiable_geoid_text +';')
    factor2 = cursor.fetchall()
    #ratio of median family income at time t2 and average median income at time t1
    lst2=[]
    for (x,y) in factor2:
        lst2=lst2+[y]


    cursor.execute('select geoid, (B25008002::numeric/NULLIF(B25008001,0)::numeric) as home_ownership from '+ t2_table+ ' where geoid in '+
                   gentrifiable_geoid_text +' and (B25008002::numeric/NULLIF(B25008001,0)::numeric) NOTNULL;')
    factor3 = cursor.fetchall()
    #the home-ownership rate at time t2
    lst3=[]
    for (x,y) in factor3:
        lst3=lst3+[y]

    cursor.execute('with t1 as (select geoid, (P012012+P012013+P012014+P012036+P012037+P012038)/NULLIF(p001001,0) as young_2000 from ' + t1_table +
                   ' ), t2 as (select geoid, (B01001012+B01001013+B01001014+B01001036+B01001037+B01001038)/NULLIF(B01003001,0) as young_2014 from '+
                   t2_table + ') select geoid, (young_2014-young_2000) as young_change from t1 join t2 using(geoid) where geoid in '+ gentrifiable_geoid_text
                   +' and (young_2014-young_2000) NOTNULL;')
    factor4 = cursor.fetchall()
    #the change in population share of the cohort that is aged between 30 and 44 from time t1 to t2
    lst4=[]
    for (x,y) in factor4:
        lst4=lst4+[y]

    cursor.execute('select geoid, (B17021002::numeric/NULLIF(B01003001,0)::numeric) as poverty_rate from '+ t2_table+ ' where geoid in ' +
                   gentrifiable_geoid_text+' and (B17021002::numeric/NULLIF(B01003001,0)::numeric) NOTNULL;')
    factor5 = cursor.fetchall()
    #the t2 poverty rate;
    lst5=[]
    for (x,y) in factor5:
        lst5=lst5+[y]


    cursor.execute('select geoid, (B11002A012::numeric/NULLIF(B01003001,0)::numeric) as white_non_family_rate from ' +t2_table+
                   ' where geoid in ' + gentrifiable_geoid_text+' and (B11002A012::numeric/NULLIF(B01003001,0)::numeric) NOTNULL;')
    factor6 = cursor.fetchall()
    #the t2 population share of White non-family households
    lst6=[]
    for (x,y) in factor6:
        lst6=lst6+[y]


    cursor.execute('select geoid, (B02001003::real/NULLIF(B01003001,0)::numeric) as black_rate from '+ t2_table +
                   ' where geoid in ' + gentrifiable_geoid_text+' and (B02001003::real/NULLIF(B01003001,0)::numeric) NOTNULL;')
    factor7 = cursor.fetchall()
    #the t2 Black population share
    lst7=[]
    for (x,y) in factor7:
        lst7=lst7+[y]

    cursor.execute('select geoid, ((C24020003+C24020039+C24020029+C24020065)::real/NULLIF(C24020001,0)::real) as managerial_administrative_rate from '+ t2_table +
                   ' where geoid in '+ gentrifiable_geoid_text+' and ((C24020003+C24020039+C24020029+C24020065)::real/NULLIF(C24020001,0)::real) NOTNULL;')
    factor8 = cursor.fetchall()
    #managerial and administrative workers as share of the total workforce at t2
    lst8=[]
    for (x,y) in factor8:
        lst8=lst8+[y]

    cursor.execute('select geoid, ((B15003019+B15003020+B15003021)::real/NULLIF(B01003001,0)::real) as some_college_rate from '+ t2_table +
                   ' where geoid in '+ gentrifiable_geoid_text+' and ((B15003019+B15003020+B15003021)::real/NULLIF(B01003001,0)::real) NOTNULL;')
    factor9 = cursor.fetchall()
    #the t2 population share of persons 25 and older with some college education
    lst9=[]
    for (x,y) in factor9:
        lst9=lst9+[y]

    #==========================================================
    #get the ranking
    #==========================================================

    # For factor1:
    print lst1
    result1 = ss.rankdata(lst1)
    print result1
    rank1 = len(result1)+1-result1           #the bigger the value, the higher  the ranking
    print rank1
    rank1 =rank1.tolist()

    dic1 = {}
    for (g,res), r in zip(factor1,rank1):     # g: geoid, res: the value, r: the rank
        dic1[g] = r                      # dic1={g:the rank,g:the rank,g:the rank,g:the rank...}


    # For factor2:
    result2 = ss.rankdata(lst2)
    rank2 = len(result2)+1-result2           #the bigger the value, the higher  the ranking
    rank2=rank2.tolist()

    dic2 = {}
    for (g,res), r in zip(factor2,rank2):
        dic2[g] = r


    # For factor3:
    result3 = ss.rankdata(lst3)           #the bigger the value, the higher the ranking
    rank3 =len(result3)+1-result3
    rank3=rank3.tolist()
    dic3 = {}
    for (g,res), r in zip(factor3,rank3):
            dic3[g] = r


    # For factor4:
    result4 = ss.rankdata(lst4)
    rank4 = len(result4)+1-result4           #the bigger the value, the higher  the ranking
    rank4=rank4.tolist()

    dic4 = {}
    for (g,res), r in zip(factor4,rank4):
        dic4[g] = r


    # For factor5:
    result5 = ss.rankdata(lst5)           #the smaller the value, the higher the ranking
    rank5=result5.tolist()
    dic5 = {}
    for (g,res), r in zip(factor5,rank5):
        dic5[g] = r


    # For factor6:
    result6 = ss.rankdata(lst6)
    rank6 = len(result6)+1-result6           #the bigger the value, the higher  the ranking
    rank6=rank6.tolist()

    dic6 = {}
    for (g,res), r in zip(factor6,rank6):
        dic6[g] = r


    # For factor7:
    result7 = ss.rankdata(lst7)           #the smaller the value, the higher the ranking
    rank7=result7.tolist()
    dic7 = {}
    for (g,res), r in zip(factor7,rank7):
        dic7[g] = r


    # For factor8:
    result8 = ss.rankdata(lst8)
    rank8 = len(result8)+1-result8           #the bigger the value, the higher  the ranking
    rank8=rank8.tolist()

    dic8 = {}
    for (g,res), r in zip(factor8,rank8):
        dic8[g] = r


    # For factor9:
    result9 = ss.rankdata(lst9)
    rank9 = len(result8)+1-result9           #the bigger the value, the higher  the ranking
    rank9=rank9.tolist()

    dic9 = {}
    for (g,res), r in zip(factor9,rank9):
        dic9[g] = r


    "=== calculate the sum of their ranking ==="

    # give the block group not in gentrifiable list a rank
    rank_lst =[]
    for id in gentrifiable_geoid:
        if dic1.has_key(id) == False:
            # print "here1"
            dic1[id] = len(gentrifiable_geoid)
        if dic2.has_key(id)==False:
            dic2[id] = len(gentrifiable_geoid)
            # print "here2"
            # print dic2[id]
            # print type(dic2[id])
        if dic3.has_key(id)==False:
            dic3[id] = len(gentrifiable_geoid)
            # print "here3"
        if dic4.has_key(id)==False:
            dic4[id] = len(gentrifiable_geoid)
            # print "here4"
        if dic5.has_key(id)==False:
            dic5[id] = len(gentrifiable_geoid)
            # print "here5"
        if dic6.has_key(id)==False:
            dic6[id] = len(gentrifiable_geoid)
            # print "here6"
        if dic7.has_key(id)==False:
            dic7[id] = len(gentrifiable_geoid)
            # print "here7"
        if dic8.has_key(id)==False:
            dic8[id] = len(gentrifiable_geoid)
            # print "here8"
        if dic9.has_key(id)==False:
            dic9[id] = len(gentrifiable_geoid)
            # print "here9"

        rank_sum= dic1[id]+dic2[id]+dic3[id]+dic4[id]+dic5[id]+dic6[id]+dic7[id]+dic8[id]
        # print id, rank_sum

        rank_lst.append((id,rank_sum))    # rank_lst:[(geoid,ranking),(geoid,ranking)...]



    sorted_by_ranking = sorted(rank_lst, key=lambda tup: tup[1])

    gentrfying_geoid=[sorted_by_ranking[0][0]]       # add the top one ranking geoid to the list
    for (id,r) in sorted_by_ranking:                 # in case there are more than one lowest average rank
        if id != sorted_by_ranking[0][0] and r ==sorted_by_ranking[0][1]:
            gentrfying_geoid.append(id)



    gentrifiable_set = set(gentrifiable_geoid)
    gentrifying_set = set(gentrfying_geoid)
    NonGentrifying_set = gentrifiable_set-gentrifying_set
    NonGentrifying_geoid = list(NonGentrifying_set)   # those gentrifiable bgs which are not gentrifying bgs are non-gentrifying bgs






    "============================================="
    "write the result to CSV file"
    "=============================================="
    final_results=[]


    for id in total_geoid_lst:
        inner_lst2=[]
        inner_lst2.append(id)
        if id in NonGentrifiable_geoid:
            inner_lst2.append(1)
        else:
            inner_lst2.append(0)

        if id in gentrifiable_geoid:
            inner_lst2.append(1)
        else:
            inner_lst2.append(0)

        if id in gentrfying_geoid:
            inner_lst2.append(1)
        else:
            inner_lst2.append(0)

        if id in NonGentrifying_geoid:
            inner_lst2.append(1)
        else:
            inner_lst2.append(0)

        final_results.append(inner_lst2)



    with open(output_file, "wb") as f:         # create and open an empty CSV. f is a handle
        writer = csv.writer(f)              # create a writer
        header = ["geoid_bg", "NonGentrifiable","gentrifiable","gentrifying","NonGentrifying"]   # create a header
        writer.writerow(header)             # write header into CSV file
        writer.writerows(final_results)

    conn.commit()
    conn.close()



def main():
    conn = connect_DB()
    Bostic_Martin(conn,t1_table,t2_table,output_file)



if __name__ == "__main__":
    main()

