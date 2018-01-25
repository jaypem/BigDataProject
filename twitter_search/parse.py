
# coding: utf-8

# In[9]:

import json
import pandas as pd
import os


# In[10]:

def checkForFile(path):
    new_path = path.split('.')[0]
    new_path = new_path+'.csv'
    
    rootdir = os.getcwd() + '/textfiles' 
    files = os.listdir(rootdir)
    
    if new_path in files:
        return True
    else: 
        return False    


# In[11]:

def convertJSON(path):
    data = pd.DataFrame(columns=['created_at', 'text'])
    
    with open(path) as file:
        for line in file:
            try:
                js = json.loads(line)
                d = {'created_at': js['created_at'], 'text': js['text']}
                vals = [[js['created_at'], js['text'].replace('\n','')]]

                temp = pd.DataFrame(vals, columns=['created_at', 'text'])

                data = data.append(temp, ignore_index=True) 
            except:
                print('error in file: ', file)
    
        print('convert successfull: ', file)
    return data


# In[12]:

def saveTextfile(path, data):
    new_path = path.split('.')[0]
    new_path = 'textfiles/'+new_path+'.csv'

    data.to_csv(new_path, sep=',', header=False, index=False)  
    print('saving successfull: ', new_path)


# In[13]:

directories = ['Bitcoin', 'bitcoin_s', '#Bitcoin', '#bitcoin_s', 'cryptocurrency', '#cryptocurrency']

rootdir = os.getcwd()

for subdir, dirs, files in os.walk(rootdir):
    path = subdir.split('\\')[-1]
    if path in directories:
        for file in files:
            filename = subdir+'\\'+file 

            # check if file already exist
            if checkForFile(file):
                print('file already exist: ', file)
                continue
                
            print('start convert: ', file)
            df = convertJSON(filename)
            print(df.head(3))
            saveTextfile(file, df)

