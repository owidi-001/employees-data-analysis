# Adding relevant libraries into our program
import pandas as pd
import numpy as np
from pandasql import sqldf
from datetime import date

objects=[]

# with open('product_premiums.txt') as f:
#     lines = f.readline()
with open('product_premiums.txt') as f:
    lines = f.read()

    lines=lines.splitlines()

for index in range(len(lines)):
    obj=[]
    if lines[index].startswith("EmployerId"):
        EmployerId=lines[index].split(":")[1]

    index+=1
    try:
        if lines[index].startswith("Product"):
            product_name=lines[index].split(":")[0]
            product_premium=lines[index].split(":")[1]
            obj.append(EmployerId)
            obj.append(product_name)
            obj.append(product_premium)   
            # print(obj)
            objects.append(obj)
            obj=[]          

    except:
        pass

employeeProductPremium=pd.DataFrame(objects,columns=["EmployerId","Product","Premium"])

# print(employeeProductPremium)


# 5. Aggregate the premium data from step 4 to the EmployerId level
employers={}
for index, row in employeeProductPremium.iterrows():
    if row["EmployerId"] not in employers:
        employers[row["EmployerId"]]=[row["EmployerId"],{row["Product"]},1,int(row["Premium"])]
        continue
    employers[row["EmployerId"]][1].add(row["Product"])

    employers[row["EmployerId"]][2]=len(employers[row["EmployerId"]][1])
    employers[row["EmployerId"]][3] += int(row["Premium"])


aggregate_data=list(employers)

df = pd.DataFrame.from_dict(employers.values())
aggregate_data = df.rename(columns={0: 'EmployerId',1: 'ProductList',2:'NumUniqueProducts',3:'SumPremium'})

ProductList=[]
for product in aggregate_data["ProductList"]:
    product=",".join(product)
    ProductList.append(product)

aggregate_data["ProductList"]=ProductList

print(aggregate_data)
