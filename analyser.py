import time
import pandas as pd
from sqlalchemy import create_engine


disk_engine = create_engine(f'sqlite:///datagov.db')


def create_second_table(table_name):
    snd_query = pd.read_sql(f"""
        SELECT A.AIRLINE, A.DESTINATION,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'ECONOMY'
            ) AS ECONOMY,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'BUSINESS'
            ) AS BUSINESS,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'FIRST'
            ) AS FIRST,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION
            ) AS TOTAL
        FROM "{table_name}" AS A
        GROUP BY AIRLINE, DESTINATION;
    """, disk_engine)
    snd_query.to_sql(f"{table_name}_snd", disk_engine)


def list_second_table():
    dest_air_ec_bu_fi_all = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, ECONOMY, BUSINESS, FIRST, TOTAl
        FROM "BUDAIRTRAFFIC_2021_1_snd";
    """, disk_engine)
    print(dest_air_ec_bu_fi_all)


def without_pre_analyst(table_name):
    snd_query = pd.read_sql(f"""
        SELECT A.AIRLINE, A.DESTINATION,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'ECONOMY'
            ) AS ECONOMY,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'BUSINESS'
            ) AS BUSINESS,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'FIRST'
            ) AS FIRST,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION
            ) AS TOTAL
        FROM "{table_name}" AS A
        GROUP BY AIRLINE, DESTINATION;
    """, disk_engine)

    print(snd_query)


def list_item_with_pre_analys(airline):
    dest_air_ec_bu_fi_all = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, ECONOMY, BUSINESS, FIRST, TOTAl
        FROM "BUDAIRTRAFFIC_2021_1_snd"
        WHERE AIRLINE = '{airline}';
    """, disk_engine)
    print(dest_air_ec_bu_fi_all)


def list_item_without_pre_analys(table_name, airline):
    snd_query = pd.read_sql(f"""
        SELECT A.AIRLINE, A.DESTINATION,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'ECONOMY' and B.AIRLINE == '{airline}'
            ) AS ECONOMY,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'BUSINESS' and B.AIRLINE == '{airline}'
            ) AS BUSINESS,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.CLASS == 'FIRST' and B.AIRLINE == '{airline}'
            ) AS FIRST,
            (SELECT count(*)
                FROM "{table_name}" AS B
                WHERE A.AIRLINE == B.AIRLINE and A.DESTINATION == B.DESTINATION and B.AIRLINE == '{airline}'
            ) AS TOTAL
        FROM "{table_name}" AS A
        WHERE A.AIRLINE == '{airline}'
        GROUP BY AIRLINE, DESTINATION;
    """, disk_engine)

    print(snd_query)


def without_preanalys():
    economy_string = "ECONOMY"
    economy_per_des_air = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, count(*) AS COUNT_ECONOMY
        FROM "BUDAIRTRAFFIC_2022_1"
        WHERE CLASS LIKE '{economy_string}'
        GROUP BY AIRLINE, DESTINATION
    ;""", disk_engine)

    business_string = "BUSINESS"
    business_per_des_air = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, count(*) AS COUNT_BUSINESS
        FROM "BUDAIRTRAFFIC_2022_1"
        WHERE CLASS LIKE '{business_string}'
        GROUP BY AIRLINE, DESTINATION
    ;""", disk_engine)

    first_string = "FIRST"
    first_per_des_air = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, count(*) AS COUNT_FIRST
        FROM "BUDAIRTRAFFIC_2022_1"
        WHERE CLASS LIKE '{first_string}'
        GROUP BY AIRLINE, DESTINATION
    ;""", disk_engine)

    all_per_des_air = pd.read_sql(f"""
        SELECT AIRLINE, DESTINATION, count(*) AS SUM_ALL
        FROM "BUDAIRTRAFFIC_2022_1"
        GROUP BY AIRLINE, DESTINATION
    ;""", disk_engine)

    dest_air_ec_bu = pd.merge(economy_per_des_air, business_per_des_air, left_on=['AIRLINE', 'DESTINATION'],
                            right_on=['AIRLINE', 'DESTINATION'], how='inner')
    dest_air_ec_bu_fi = pd.merge(dest_air_ec_bu, first_per_des_air, left_on=['AIRLINE', 'DESTINATION'],
                            right_on=['AIRLINE', 'DESTINATION'], how='inner')
    dest_air_ec_bu_fi_all = pd.merge(dest_air_ec_bu_fi, all_per_des_air, left_on=['AIRLINE', 'DESTINATION'],
                            right_on=['AIRLINE', 'DESTINATION'], how='inner')
    print(dest_air_ec_bu_fi_all)


if __name__ == "__main__":
    start_time = time.time()

    # create_second_table('BUDAIRTRAFFIC_2021_1')

    # list_second_table()
    # without_pre_analyst('BUDAIRTRAFFIC_2021_1')

    # list_item_with_pre_analys('AEGEAN')
    # list_item_without_pre_analys('BUDAIRTRAFFIC_2021_1', 'AEGEAN')

    #list_item_with_pre_analys('WIZZ AIR')
    # list_item_without_pre_analys('BUDAIRTRAFFIC_2021_1', 'WIZZ AIR')

    end_time = time.time()
    print(f"The runningtime was {end_time-start_time} second")
    pass
