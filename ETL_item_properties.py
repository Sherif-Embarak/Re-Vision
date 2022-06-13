#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import glob
import os

def group(df):
    return df.drop_duplicates(["itemid","property","value"] , keep='first')

def save(df):
    df.to_csv("out_item_properties.csv", index=False)
    del df
    
def group_and_stage(fileName):
    df = pd.read_csv(fileName)
    df = group(df)
    df.to_csv("stage_"+fileName, index=False)
    
def group_and_reshape(fileNames):
    li = []
    for fileName in fileNames:
        df = pd.read_csv(fileName, index_col=None, header=0)
        li.append(df)
    df = pd.concat(li, axis=0, ignore_index=True)
    df = group(df)
    # df = df.pivot(index=['itemid','timestamp'] , columns='property' , values='value').reset_index()
    return df


allCsvs = glob.glob("*.csv")
itemPropertiesStartStr = "item_properties_part"
itemPropertiesFiles = list(filter(lambda x: x.startswith(itemPropertiesStartStr), allCsvs))


for itemPropertiesFile in itemPropertiesFiles:
    group_and_stage(itemPropertiesFile)


allCsvs = glob.glob("*.csv")
stageFiles = list(filter(lambda x: x.startswith("stage"), allCsvs))
df = group_and_reshape(stageFiles)
save(df)


#delete staging files 
for fileName in stageFiles:
    os.remove(fileName)
    print("file deleted: ",fileName)

