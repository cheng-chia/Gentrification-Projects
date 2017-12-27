
import psycopg2
import statistics
import csv
from operator import itemgetter


dbname='*******' # enter your database name
user='********' # enter your user name
password='***'  # enter your password
host='***,***,***,***' # enter the host
port='****' # enter the poet

output_file = 'ding_typology.csv'
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



def Ding(conn,t1_table,t2_table,output_file):
    cursor = conn.cursor()
    cursor.execute('select geoid, P053001 from '+ t1_table +';')
    income = cursor.fetchall()
    income_lst =[]
    total_geoid_lst = []
    for (geoid,i) in income:
        income_lst.append(i)
        total_geoid_lst.append(geoid)

    med_income = statistics.median(income_lst)  # calculate the median income



    total_geoid_lst_tuple = tuple(total_geoid_lst)
    total_geoid_lst_text = str(total_geoid_lst_tuple)

    "========================================================"
    "Get the gentrifiable and non-gentrifiable block group id"
    "========================================================"


    gentrifiable_geoid = []
    NonGentrifiable_geoid = []
    for (geoid,i) in income:
        if i < med_income:
            # print i
            gentrifiable_geoid.append(geoid)
        else:
            NonGentrifiable_geoid.append(geoid)


    gentrifiable_geoid_tuple = tuple(gentrifiable_geoid)
    gentrifiable_geoid_text = str(gentrifiable_geoid_tuple)


    "======================================================="
    "Get the gentrifying block group id"
    "======================================================="


    cursor.execute('with t1 as (select geoid, H085001 from '+  t1_table +' ) select geoid, ((B25077001-H085001)::numeric/NULLIF(H085001,0)::numeric) from t1 join '
                   + t2_table+ ' using(geoid) where geoid in '+total_geoid_lst_text +';')

    # get  percentage increase of median home value per census block group
    house_price = cursor.fetchall()
    house_lst =[]
    for (geoid,h) in house_price:
        house_lst.append(h)

    med_house =  statistics.median(house_lst)  #get the median percentage increase in house price


    cursor.execute('with t1 as (select geoid, H063001 from '+ t1_table +' ) select geoid, ((B25064001-H063001)::numeric/NULLIF(H063001,0)::numeric) from t1 join '
                   + t2_table+ ' using(geoid) where geoid in '+total_geoid_lst_text +';')

    # get  percentage increase of gross rent per census block group
    gross_rent = cursor.fetchall()
    rent_lst=[]
    for (geoid,r) in gross_rent:
        rent_lst.append(r)
    med_gross_rent = statistics.median(rent_lst) # get the median percentage increase in gross rent



    cursor.execute('with t1 as (select geoid, (P037015+P037016+P037017+P037018+P037032+P037033+P037034+P037035) as college_2000 from '
                   + t1_table +' ) select geoid, (B15003022+B15003023+B15003024+B15003025)-college_2000 from t1 join ' +t2_table+
                   ' using(geoid) where geoid in '+total_geoid_lst_text +';')
    # get the increasing number of college-educated residents
    education = cursor.fetchall()
    education_lst = []
    for (geoid,e) in education:
        education_lst.append(e)
    med_education = statistics.median(education_lst)  # get the median increase of college-educated residents




    gentrfying_geoid= []
    NonGentrifying_geoid=[]
    house_price = sorted(house_price, key = itemgetter(0))
    gross_rent = sorted(gross_rent, key = itemgetter(0))
    education = sorted(education, key = itemgetter(0))
    for (geoid1,h),(geoid2,r),(geoid3,e) in zip(house_price,gross_rent,education):
    #the bg has experienced an above citywide median percentage increase in either its median gross rent or median home value. And the tract has experienced an above citywide median increase in its share of college-educated residents
        # print geoid1,geoid2,geoid3
        if (h>med_house or r>med_gross_rent) and e>med_education and geoid1 in gentrifiable_geoid:
            gentrfying_geoid.append(geoid1)
            # print geoid1
        elif  geoid1 in gentrifiable_geoid:
            NonGentrifying_geoid.append(geoid1)


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
        header = ["geoid_bg", "NonGentrifiable","gentrifiable","gentrifying","Nongentrifying"]   # create a header
        writer.writerow(header)             # write header into CSV file
        writer.writerows(final_results)




    conn.commit()
    conn.close()


def main():
    conn = connect_DB()
    Ding(conn,t1_table,t2_table,output_file)



if __name__ == "__main__":
    main()



