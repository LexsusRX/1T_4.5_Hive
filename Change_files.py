import pandas as pd
from datetime import datetime as dt

PATH_SOURCE = './csv_files/'
PATH_MODIFIED = './csv_files_added_column_range/'
CNT_PARTS = 10
FILE_customers = 'customers.csv'
FILE_organizations = 'organizations.csv'
FILE_people = 'people.csv'

def range_group(rg):
    rg['Group'] = [index // (int(len(rg)) // CNT_PARTS) + 1 for index in range(int(len(rg)))]
    return rg

def insert_year(iy):
    year_data = []
    for date in iy['Subscription Date']:
        year_data.append(dt.strptime(date, '%Y-%m-%d').year)
    iy['Subscription Year'] = year_data
    return iy


if __name__ == '__main__':
    cust = pd.read_csv(PATH_SOURCE + FILE_customers)
    insert_year(range_group(cust)).to_csv(PATH_MODIFIED + FILE_customers, index=False)

    org = pd.read_csv(PATH_SOURCE + FILE_organizations)
    range_group(org).to_csv(PATH_MODIFIED + FILE_organizations, index=False)

    ppl = pd.read_csv(PATH_SOURCE + FILE_people)
    range_group(ppl).to_csv(PATH_MODIFIED + FILE_people, index=False)


    # cust = pd.read_csv(PATH_SOURCE + FILE_customers)
    # insert_year(range_group(cust)).to_csv(PATH_MODIFIED + FILE_customers, sep=';', index=False)
    #
    # org = pd.read_csv(PATH_SOURCE + FILE_organizations)
    # range_group(org).to_csv(PATH_MODIFIED + FILE_organizations, sep=';', index=False)
    #
    # ppl = pd.read_csv(PATH_SOURCE + FILE_people)
    # range_group(ppl).to_csv(PATH_MODIFIED + FILE_people, sep=';', index=False)