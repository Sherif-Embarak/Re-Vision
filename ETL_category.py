#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from collections import defaultdict

fileName = 'category_tree.csv'
colTypes = {"parentid":str,"categoryid":str}
CategoryDF  = pd.read_csv( fileName , dtype=colTypes)

main_roots = CategoryDF[CategoryDF.parentid.isna()].categoryid.tolist()
CategoryDF = CategoryDF[(CategoryDF.parentid.notna())]

parent_child_dict = defaultdict(list)
for index,row in CategoryDF.iterrows():
        parent_child_dict[row['parentid']].append(row['categoryid'])

outDict = defaultdict(list)
def DFStraverse (value , parent , dep):
    outDict[value].append(parent)
    for child in parent_child_dict[value]:
        DFStraverse(child , parent+[(value,dep)] , dep+1)

for i in main_roots:
    DFStraverse(i, [] , 1)

df = {}
i = 0
outColumns = ['categoryid', 'parentid', 'parent_depth']
for key,value in outDict.items():
    for j in value[0]:
        df[i] = {outColumns[0]: key, outColumns[1]: j[0], outColumns[2]: j[1]}
        i = i + 1
        
for root in main_roots:
        df[i] = {outColumns[0]: root, outColumns[1]: '', outColumns[2]: 0}
        i = i + 1
df = pd.DataFrame.from_dict(df, "index")
df.to_csv("outCategoryTree.csv" , index=False)




