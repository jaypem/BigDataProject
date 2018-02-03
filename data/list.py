import os

# get all files
files = []
for file in os.listdir('.'):
    if file.endswith(".csv"):
       files.append("./projekt/data/" + file)

o = ','.join(files)

with open('files', 'w') as fp:
    fp.write(o)
