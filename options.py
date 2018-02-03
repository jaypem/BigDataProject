
# global variables
sentiment_levels = []
date_levels = []

def set_sentiment_levels(l):
    global sentiment_levels
    sentiment_levels = l

def set_date_levels(l):
    global date_levels
    date_levels = l

def get_date_level(index):
    global date_levels
    if 0 <= index < len(date_levels):
        return date_levels[index]
    else:
        return 0

def generate_date_levels(steps):
    hours = 24 / steps
    clusters = []

    t = 0
    while t < 24:
        clusters.append([t, t + hours])
        t += hours

    return clusters
