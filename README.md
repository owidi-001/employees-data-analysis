#   Getting started

-   Create a python virtual environment using the commmand and activate < Not amust, packages can be installed globally>
-   Install necessary libraries, ie pandas,pandasql `pip install pandasql pandas` Or `pip install -r requirements.txt`
  


# Activities

## Prerequisites
```python
# Adding relevant libraries into our program
import pandas as pd
import numpy as np
from pandasql import sqldf
from datetime import datetime

# reads the two CSV files
employers = pd.read_csv('employers.csv')
quotes = pd.read_csv('quotes.csv')

# Instantiates the sqldf object
pysqldf = lambda q: sqldf(q, globals())
```


## 1. Combine Employers and Quotes data

Read the employers.csv and quotes.csv and merge them on EmployerId as an inner join. Ensure that
the result is unique on EmployerId. If there are duplicates, keep the row with the latest quote date.

```python
# 1. Combine Employers and Quotes data

# SQL Syntax
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

# Output the results of the join
print(type(employers_quotes))
```

## 1. Snap
![Output of the fist](/snaps/1.png)



## 2. Create a new column named ClientTenure

Based on the date the employer became a client and the date ProductX was quoted, determine the client
tenure (in years, assuming 365 days a year) at the time of quote. Round to nearest integer.

```python
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

# Outputs employers_quotes with the formated ClientStartDate
print(employers_quotes)


# Returns the difference between the dates in days
def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%d-%m")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


ClientTenure = []
for startDate, endDate in zip(employers_quotes["ClientStartDate"], employers_quotes["QuoteDate"]):
    ClientTenure.append(round(days_between(startDate, endDate)/365))

employers_quotes["ClientTenure"] = ClientTenure

# Outputs employers_quotes with the ClientTenure added in years
print(employers_quotes)
```

## 2. Snap
![Output of the fist](/snaps/2.png)

## 3. Create a column named SizeRange

This will be a categorical column based on the Employees column, which is a count of the number of
employees a client has. The resulting values in the new column will be:

```python
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

# Outputs employers_quotes with the SizeRange column added
print(employers_quotes)
```

## 3. Snap
![Output of the fist](/snaps/3.png)


## 4. Read product_premiums.txt

This file contains the products a client currently has, and the annual premium associated with each product.
The file structure is not natively readable by pandas, so you must write a parser to extract the information
into a dataframe. The file is structured to have a section start with EmployerId: 00000000 followed by a
series of product and premium pairs, such as Product1: 9233 . Each Id only occurs once. An employer
can have duplicate products (one per location) but premiums will be unique. The resulting dataframe should
look like this:a client has. The resulting values in the new column will be:

```python
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

# Outputs the read data formatted as specified
print(employeeProductPremium)
```

## 4. Snap
![Output of the fist](/snaps/4.png)


## 5. Aggregate the premium data from step 4 to the EmployerId level

Create the following columns:
-   ProductList : a comma separated string with the distinct products a client has. ex. 'Product4,Product1, Product2'
-   NumUniqueProducts : the number of unique products a client has. In the above example it would be 3.
-   SUmPremium: A sum of the premium from all products.

```python
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

# Outputs the aggregate data formatted as specified
print(aggregate_data)
```

## 5. Snap
![Output of the fist](/snaps/5.png)


## 6. Join aggregated premium data to the dataset created in steps 1-3 using pandasql

Write SQL to create an inner join on EmployerId

```python
# 6. Join aggregated premium data to the dataset created in steps 1-3 using pandasql

# SYNTAX FOR DATAFRAME employers_quotes
"""
     SELECT 
         employers_quotes.EmployerId,employers_quotes.Employees,employers_quotes.SizeRange,employers_quotes.ClientTenure,employers_quotes.Industry,employers_quotes.ClientStartDate,employers_quotes.QuoteDate,employers_quotes.Offered
     FROM 
         employers_quotes
"""
# SYNTAX FOR DATAFRAME aggregate_data
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

# Outputs the JointAggregatedPremiumData data formatted as specified
print(JointAggregatedPremiumData)                    
```

## 6. Snap
![Output of the fist](/snaps/6.png)


## 7. Write final DataFrame to file output specified by user

```python
## 7. Write final DataFrame to file output specified by user
data_store = input("Specify file:")
JointAggregatedPremiumData.to_csv(data_store)                  
```

## 7. Snap
![Output of the fist](/snaps/7.png)
![Output of the fist](/snaps/7.1.png)
