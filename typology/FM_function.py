import psycopg2
import statistics
import csv
from operator import itemgetter

"======= set up ==========="

dbname='*******' # enter your database name
user='********' # enter your user name
password='***'  # enter your password
host='***,***,***,***' # enter the host
port='****' # enter the poet

output_file = 'freeman_typology.csv'
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


"========================================================================="
'find out gentrifiable and non-gentrifiable bg'
"========================================================================="

def Freeman(conn,t1_table,t2_table,output_file):

    cursor = conn.cursor()
    cursor.execute ('select geoid, P053001 from ' + t1_table +';')
    income = cursor.fetchall()

    income_lst =[]
    total_geoid_lst = []
    for (geoid,i) in income:
        income_lst.append(i)
        total_geoid_lst.append(geoid)
    med_income = statistics.median(income_lst)  #get the median of median income
    print "median income: ",med_income



    cursor.execute('with t1 as (select geoid,H034003,H034002 from ' + t1_table + ' ) select geoid,((B25034003+B25034002+H034003+H034002)::numeric/NULLIF(B25034001,0)::numeric) as built_prop from t1  join ' + t2_table +' using(geoid);')
    built = cursor.fetchall()
    built_lst = []
    # print built
    for (geoid,b) in built:
        built_lst.append(b)
    med_built = statistics.median(built_lst) # get the median of proportion of housing built
    print "median built: ", med_built

    #with a median income that is at or less than the median in their respective metropolitan areas.
    #have a proportion of housing built within the past 20 years lower than the proportion found at the median for the respective metropolitan area (MSA).



    gentrifiable_geoid = []
    NonGentrifiable_geoid = []
    income = sorted(income, key = itemgetter(0))
    built = sorted(built, key = itemgetter(0))  # if we don't sort geoid, the order of geoid in income is different from the order in built!!!!!!!!!!
    # print income


    for (geoid,i),(geoid2,b) in zip(income,built):
        # print geoid,geoid2
        if i <= med_income and b < med_built:
            gentrifiable_geoid.append(geoid)
        else:
            NonGentrifiable_geoid.append(geoid)



    "================================================================"
    'find out the gentrifying and non gentrifying areas'
    "================================================================"

    cursor.execute('with t1 as (select geoid,(P037015+P037016+P037017+P037018+P037032+P037033+P037034+P037035) as  college_2000 from '+ t1_table + ' ) select geoid, (B15003022+B15003023+B15003024+B15003025-college_2000)::numeric/NULLIF(college_2000,0)::numeric from t1 join '+ t2_table+ ' using(geoid) ;')
    # get the percentage increase in educational attainment
    education = cursor.fetchall()
    education_lst=[]
    for (geoid,e) in education:
        education_lst.append(e)
    med_education = statistics.median(education_lst) #get the median education increasing rate
    print "median education: ", med_education



    cursor.execute('with t1 as (select geoid, H085001 from '+ t1_table +' ) select geoid,(B25077001-H085001)from t1 join '+ t2_table+' using(geoid) ; ')
    # cursor.execute('with t1 as (select geoid, H085001 from '+ t1_table +' ) select geoid from t1 join '+ t2_table+' using(geoid) where (B25077001-H085001)>0 and geoid in '+total_geoid_lst_text +'; ')


    # get the median house value increase
    house_price = cursor.fetchall()
    house_price_incrs =[]
    for i, h in house_price:
        if h>0:
            house_price_incrs.append(i)




    gentrfying_geoid= []
    NonGentrifying_geoid=[]
    for (geoid,e) in education:
        if e > med_education and geoid in house_price_incrs and geoid in gentrifiable_geoid:  # have a percentage increase in educational attainment greater than the median increase in educational attainment for the respective area.
            gentrfying_geoid.append(geoid)                    # and  has an increase in real housing prices during t1-t2.

        else:
            NonGentrifying_geoid.append(geoid)

    print "gentrifying:",gentrfying_geoid

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
    Freeman(conn,t1_table,t2_table,output_file)



if __name__ == "__main__":
    main()



