# Adding relevant libraries into our program
import pandas as pd
import numpy as np
from pandasql import sqldf
from datetime import datetime

# reading the two CSV files
employers = pd.read_csv('employers.csv')
quotes = pd.read_csv('quotes.csv')

pysqldf = lambda q: sqldf(q, globals())

# 1. Combine Employers and Quotes data
# Syntax
"""
 SELECT 
      column_name(s)
 FROM 
     table1
 INNER JOIN 
     table2
 ON table1.column_name = table2.column_name;
 """

employers_quotes = pysqldf("""
                    SELECT 
                        employers.EmployerId,employers.Employees,employers.Industry,employers.NetworkStrength,employers.ZipCode,employers.ClientStartDate,quotes.Offered,quotes.Status,quotes.QuoteDate
                    FROM 
                        employers
                    INNER JOIN 
                        quotes
                    ON employers.EmployerId = quotes.EmployerId;
                    """)

# print(employers_quotes)

# 2. Create a new column named ClientTenure
ClientStartDateList = []
for ClientStartDate in employers_quotes["ClientStartDate"]:
    # formating ClientStartDate to match quote date
    startDate = ''
    ClientStartDate = ClientStartDate.split('/')
    ClientStartDate[2] = '20' + ClientStartDate[2]
    ClientStartDate.reverse()
    startDate = '-'.join(ClientStartDate)
    ClientStartDateList.append(startDate)

employers_quotes["ClientStartDate"] = ClientStartDateList


# print(employers_quotes)

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%d-%m")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


ClientTenure = []
for startDate, endDate in zip(employers_quotes["ClientStartDate"], employers_quotes["QuoteDate"]):
    ClientTenure.append(round(days_between(startDate, endDate)/365))

employers_quotes["ClientTenure"] = ClientTenure

# print(employers_quotes)


# 3. Create a column named SizeRange
ranges = ['0-9 EE', '10-49 EE', '50-99 EE', '100-499 EE', '500-999 EE', '1000-4999EE', '5000+ EE']

SizeRange = []

for count in employers_quotes["Employees"]:
    if 0 <= count <= 9:
        size_range = ranges[0]
    elif 10 <= count <= 49:
        size_range = ranges[1]
    elif 50 <= count <= 99:
        size_range = ranges[2]
    elif 100 <= count <= 499:
        size_range = ranges[3]
    elif 500 <= count <= 999:
        size_range = ranges[4]
    elif 1000 <= count <= 4999:
        size_range = ranges[5]
    elif count >= 5000:
        size_range = ranges[6]
    SizeRange.append(size_range)

employers_quotes["SizeRange"] = SizeRange

# print(employers_quotes)

# 4. Read product_premiums.txt
objects = []

with open('product_premiums.txt') as f:
    lines = f.read()
    lines = lines.splitlines()

for index in range(len(lines)):
    obj = []
    if lines[index].startswith("EmployerId"):
        EmployerId = lines[index].split(":")[1]

    index += 1
    try:
        if lines[index].startswith("Product"):
            product_name = lines[index].split(":")[0]
            product_premium = lines[index].split(":")[1]
            obj.append(EmployerId)
            obj.append(product_name)
            obj.append(product_premium)
            # print(obj)
            objects.append(obj)
            obj = []

    except:
        pass

employeeProductPremium = pd.DataFrame(objects, columns=["EmployerId","Product", "Premium"])

# print(employeeProductPremium)


# 5. Aggregate the premium data from step 4 to the EmployerId level
employers = {}
for index, row in employeeProductPremium.iterrows():
    if row["EmployerId"] not in employers:
        employers[row["EmployerId"]] = [row["EmployerId"], {row["Product"]}, 1, int(row["Premium"])]
        continue
    employers[row["EmployerId"]][1].add(row["Product"])

    employers[row["EmployerId"]][2] = len(employers[row["EmployerId"]][1])
    employers[row["EmployerId"]][3] += int(row["Premium"])

aggregate_data = list(employers)

df = pd.DataFrame.from_dict(employers.values())
aggregate_data = df.rename(columns={0: 'EmployerId', 1: 'ProductList', 2: 'NumUniqueProducts', 3: 'SumPremium'})

ProductList = []
for product in aggregate_data["ProductList"]:
    product = ",".join(product)
    ProductList.append(product)

aggregate_data["ProductList"] = ProductList

# print(aggregate_data)

# 6. Join aggregated premium data to the dataset created in steps 1-3 using pandasql

# syntax
"""
     SELECT 
         employers_quotes.EmployerId,employers_quotes.Employees,employers_quotes.SizeRange,employers_quotes.ClientTenure,employers_quotes.Industry,employers_quotes.ClientStartDate,employers_quotes.QuoteDate,employers_quotes.Offered
     FROM 
         employers_quotes
"""

"""
    SELECT 
         aggregate_data.ProductList as ExistingProducts,aggregate_data.NumUniqueProducts,aggregate_data.SumPremium
    FROM 
        aggregate_data
"""

JointAggregatedPremiumData = pysqldf("""
                    SELECT 
                        employers_quotes.EmployerId,
                        employers_quotes.Employees,
                        employers_quotes.SizeRange,
                        employers_quotes.ClientTenure,
                        employers_quotes.Industry,
                        employers_quotes.ClientStartDate,
                        employers_quotes.QuoteDate,
                        employers_quotes.Offered,
                        aggregate_data.ProductList as ExistingProducts,
                        aggregate_data.NumUniqueProducts,
                        aggregate_data.SumPremium
                    FROM 
                        employers_quotes,aggregate_data
                    Where
                        employers_quotes.EmployerId=aggregate_data.EmployerId
                    """)

# print(JointAggregatedPremiumData)

## 7. Write final DataFrame to file output specified by user
data_store = input("Specify file:")
JointAggregatedPremiumData.to_csv(data_store)
