
import textblob as tb
from datetime import datetime

import options


def split_data_line(line):
    i = line.find(',')
    return (line[:i].replace('+0000 ', ''), line[i + 1:])

def convert_date(x):
    try:
        dt = datetime.strptime(x[0], '%a %b %d %H:%M:%S %Y')
        return (dt, x[1])
    except:
        return (datetime(1990, 1, 1), x[0])

def check_language(x):
    blob = tb.TextBlob(x[1])
    l = blob.detect_language()

    if l == "en":
        return x
    else:
        #try:
        #    print('*******************************************************************translated')
        #    return (x[0], str(blob.translate(from_lang=l, to="en")))
        #except:
        return (x[0], '')

def check_sentiment(x):
    try:
        return (x[0], tb.TextBlob(x[1]).sentiment.polarity)
    except:
        return (x[0], 666)

def cluster_value(v, c):
    if v < c[0][0]:
        return 0

    for i in range(len(c)):
        if c[i][0] <= v < c[i][1]:
            return i

    return len(c) - 1

def cluster_sentiment(x, levels):
    return (x[0], cluster_value(x[1], levels))

def cluster_date(x, levels):
    index = cluster_value(x[0].hour, levels)
    d = '{}-{}-{},{},{}'.format(x[0].year, x[0].month, x[0].day, levels[index][0], levels[index][1])
    return (d, x[1])

