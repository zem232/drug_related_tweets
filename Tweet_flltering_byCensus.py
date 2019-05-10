from pyspark import SparkContext

def createIndex(shapefile):
    '''
    Creates a an rtree index from a shapefile or geojson
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    Determines if a point coordinate is contained
    within a census tract, and returns the index of the census tract.
    '''
    try:
        if 'geometry' in zones:
            match = index.intersection((p.x, p.y, p.x, p.y))
            for idx in match:
                if zones.geometry[idx].contains(p):
                    return idx
                    continue   
    except:
        return None

def processTweets(pid, records):
    '''
    This function processes the tweets in the following steps:
    1. Create a list of drug terms from the drug_sched2 and drug_illegal
    text files.
    2. Iterate through each tweet to determine if a drug term is in the tweet.
    3. Determine if tweets with drug terms are contained within a census tract
    4. Update a dictionary of counts of drug tweets for each census tract
    '''
    import csv
    import pyproj
    import shapely.geometry as geom
    import re
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    CENSUS = '500cities_tracts.geojson'
    indexCT, zonesCT = createIndex(CENSUS)

    DRUGS_SLANG = 'drug_illegal.txt'
    DRUGS_SCHED = 'drug_sched2.txt'

    ## open the two drug files and concat into one list of drum terms
    with open(DRUGS_SLANG, 'r') as fi:
        drugs = [(line.strip()) for i,line in enumerate(fi)]

    with open(DRUGS_SCHED, 'r') as fii:
        drugs_ = [(line.strip()) for i,line in enumerate(fii)]

    drugs = drugs + drugs_

    reader = csv.reader(records)
    counts = {}

    ## Iterate through each tweet
    for fields in reader:
        foo = 0    ## re-setting the indicator of whether a drug term is in a tweet
        zoneCT = None   ## re-setting the spatial index to None
        tweet = None
        words = None
        ## determine if drug term is in the tweet
        
        try:  
            row = str(fields).split('|')
            if len(row) != 7:
                continue
            ## remove all apostrophes, commas and symbols from the tweet
            tweet = row[5].replace("'","").replace(',','')
            tweet = re.sub(r'[^\w]',' ',tweet).lower()  ## from row 5, which is the entire tweet and preserves order
            words = row[6].split(' ') ## create a list of all words in row 6, which contains all relevant words
        except:
            continue

        if tweet and words:
            for drugTerm in drugs:
                if drugTerm in tweet:  ## first determine if a drug term is in a tweet
                    for word in words:  ## iterate through each word from row 6 to determine if the full word is in the twe
                        if word in drugTerm:
                            foo = 1  ## if the drug terms match the word, then indicator is 1]
        if foo == 1:
            try:  ## get lat/long of tweet and convert to Point
                p = geom.Point(proj(float(row[2]), float(row[1])))
                ## determine if the point is within a census tract
                zoneCT = findZone(p, indexCT, zonesCT)

            except:
                continue

            if zoneCT:
                tractId = zonesCT['plctract10'][zoneCT]
                tractPop = zonesCT['plctrpop10'][zoneCT]
                tractKey = (tractId, tractPop) ## define a key of the census tract ID & population
                counts[tractKey] = counts.get(tractKey, 0) + 1 ## create dictionary of tract keys and # of tweets

    return counts.items()

def normalizeTracts(item):

    try:
        tractID = item[0][0]  ## extracting the key (tract name)
        tractPOP = int(item[0][1])  ## extracting the key (tract population)
        drugTweets = item[1]   ## the num of drug-related tweets
        if tractPOP > 0:
            normTweets = drugTweets/tractPOP
            return (tractID, normTweets)
    except:
        pass

def main(sc):
        import sys
        TWEETS = sys.argv[1]
        rdd = sc.textFile(TWEETS)
        counts = rdd.mapPartitionsWithIndex(processTweets).\
        map(normalizeTracts).filter(lambda x: x is not None).\
	sortByKey(True)
        print(counts.collect())

## Execute the main function
if __name__ == "__main__":
        sc = SparkContext()
        main(sc)
