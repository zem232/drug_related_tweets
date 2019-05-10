# Visualizing drug-related tweets with Pyspark
Using PySpark to find tweets containing drug terms and perform geospatial analysis.

### Data
* [500 Cities Census Tract Boundaries](https://chronicdata.cdc.gov/500-Cities/500-Cities-Census-Tract-Boundaries/x7zy-2xmx)
* [Illegal drug terms](drug_illegal.txt)
* [Schedule 2 drug names](drug_sched2.txt)
* 100 million geo-tagged tweets in the US
  * In CSV format, with | as delimeter
  * Source: collected through the Twitter Open API

### Filtering Tweets and Aggregating by Census Tract

[Tweet filtering python script](Tweet_flltering_byCensus.py)

### Sample Filtering Output

[('0107000-01073000800', 0.0005182689816014512),
 ('0177256-01125012000', 0.000244140625),
 ('0412000-04013812500', 0.00048661800486618007),
 ('0427400-04013422509', 0.00019557989438685703)]
 
 ### Geospatial Visualization
 
 
