import psycopg2
import csv
import numpy as np

dbname='*******' # enter your database name
user='********' # enter your user name
password='***'  # enter your password
host='***,***,***,***' # enter the host
port='****' # enter the poet

output_file = 'McKinnish_typology.csv'
t1_table = 'new_decennial_2000_census_in_SLCounty' # the name of table: need to establish the table as the ER diagram shown in the slides
t2_table = 'new_acs_2014_5_year_estimate_in_slcounty' # the name of table: need to establish the table as the ER diagram shown in the slides




def connect_DB():
    try:
        conn= psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
        print ('connecting to '+dbname+' succeeded')
        return conn
    except Exception, e:
        print (e)
        print ('connecting to '+dbname+' failed')
        # print (errorcodes.lookup(e.pgcode))


def McKinnish(conn,t1_table,t2_table,output_file):

    cursor = conn.cursor()

    cursor.execute('select geoid, P078001::numeric/NULLIF(P014002,0)::numeric'+ t1_table +';')
    # get the average family income per bg
    income = cursor.fetchall()
    total_bg = []
    fam_income = []
    for (geoid,i) in income:
        try:
            fam_income.append(float(i))
            total_bg.append(geoid)
        except Exception as e:
            # print e
            fam_income.append(None) # if there is any thing wrong, append None value to the list

    quantile_fam_income = np.percentile(fam_income,25)
    # get the bottom quantile of average family income

    cursor.execute('with t1 as(select geoid,(P078001::numeric/NULLIF(P014002,0)::numeric) as avg_income_2000 from '+ t1_table +
                   ' ), t2 as (select geoid, (B19127001::numeric/NULLIF(B11001002,0)::numeric) as avg_income_2014 from '+ t2_table +
                   ' ) select geoid from t1 join t2 using(geoid) where avg_income_2014-avg_income_2000>10000;')
    # get the bg which has an increase in average family income by at least 1,0000 from t1 to t2
    income_increase = cursor.fetchall()
    above_lst =[]
    for (id,) in  income_increase:
        above_lst.append(id)

    gentrifying_geoid = []
    NonGentrifying_geoid = []

    for (geoid,i)in income:
        if i <= quantile_fam_income and i in above_lst:
            gentrifying_geoid.append(geoid)
        else:
            NonGentrifying_geoid.append(geoid)



    "============================================="
    "write the result to CSV file"
    "=============================================="
    final_results=[]



    for id in total_bg:
        inner_lst=[]
        inner_lst.append(id)
        if id in gentrifying_geoid:
            inner_lst.append('yes')

        if id in NonGentrifying_geoid:
            inner_lst.append('no')
        final_results.append(inner_lst)

     # write the result to CSV file

    with open(output_file, "wb") as f:         # create and open an empty CSV. f is a handle
        writer = csv.writer(f)              # create a writer
        header = ["geoid_bg", "gen_YN"]   # create a header
        writer.writerow(header)             # write header into CSV file
        writer.writerows(final_results)

    conn.commit()
    conn.close()



def main():
    conn = connect_DB()
    McKinnish(conn,t1_table,t2_table,output_file)


if __name__ == "__main__":
    main()




