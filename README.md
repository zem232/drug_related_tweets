# Visualizing drug-related tweets with Pyspark
Using PySpark to find tweets containing drug terms and perform geospatial analysis.

### Data
* [500 Cities Census Tract Boundaries](https://chronicdata.cdc.gov/500-Cities/500-Cities-Census-Tract-Boundaries/x7zy-2xmx)
* [Illegal drug terms](drug_illegal.txt)
* [Schedule 2 drug names](drug_sched2.txt)
* 100 million geo-tagged tweets in the US
 * In CSV format, with | as delimeter

100 million geo-tagged tweets in the US
Source: collected through the Twitter Open API
The data is in CSV format, however, the delimiter is the pipe character (“|”)

### Filtering Tweets and Aggregating by Census Tract

[Tweet filtering python script](Tweet_flltering_byCensus.py)

### Sample Output

[('0107000-01073000800', 0.0005182689816014512),
 ('0177256-01125012000', 0.000244140625),
 ('0412000-04013812500', 0.00048661800486618007),
 ('0427400-04013422509', 0.00019557989438685703),
 ('0427400-04013815100', 0.0004521817770743839),
 ('0446000-04013420211', 0.0002151462994836489),
 ('0446000-04013422203', 0.000224517287831163),
 ('0454050-04013610900', 0.0005320563979781857),
 ('0455000-04013110501', 0.0006898930665746809),
 ('0455000-04013611400', 0.00016046213093709883),
 ('0455000-04013615200', 0.0001856665428889714),
 ('0465000-04013217201', 0.0007230657989877079),
 ('0465000-04013217800', 0.00033008747318039283)]
