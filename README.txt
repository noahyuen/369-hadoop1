Noah Yuen - CSC 369 - Spring 2022 - Lab 3

1. Description:

At a high level, for each of the six lab3 queries, I wrote at least one map-reduce phase, and an additional map-reduce
phase for queries whose output required additional sorting. For the queries that required additional sorting, the
output of the first map-reduce job became the input to the sorting map-reduce job.

For query 1, I created two map-reduce phases. The map portion of the first phase extracted the url path from each line
of the access log input file and emitted a url path, one pair. The reduce portion of the first phase combined the value
of key/value pairs that contained the same url path and summed up all the occurences of that url path. In order to sort
the urlPath/count pairs, I created a second map-reduce job that emitted the count as the key and the url path as the
value.

For query 2, my implementation was the same as query 1, except the key emitted was the response code instead of a url
path. Since the response codes were sorted in the shuffle phase, this query did not require an additional map-reduce
phase to sort by response code.

For query 3, my map emitted the host name and byte count for each line in the access log that contained the given host
name. In the reduce phase, I simply sumed all the byte counts for the given hostname, as there should only be a single
key (the input host name) passed to the reduce phase.

For query 4, my map emitted the host name and 'one' for each line in the access log that contained the given URL. In
the reduce phase, I sumed all the values for each hostname. In order to sort the output by ascending count, I created
an additional map-reduce phase whose map emitted the count as the key and hostname as the value.

For query 5, my map split each line of the access log to obtain the month/year of each log and emitted the month/year as
the key with 'one' as the value. My reduce phase sumed all the counts for each month/year. In order to sort
chronologically, I had to create an additional map-reduce phase. The map of this second map-reduce phase converted
each month abbreviation to its numeric representation, allowing the 'shuffle' phase of the second map-reduce phase to
sort by the numeric representation of the month/year. Finally, my second reduce phase converted the numeric
representation of the month/year back into its english abbreviation as represented in the original access log contents.

For query 6, my map split each line of the access log to obtain the day/month/year of each log and emitted the
day/month/year as the key with the bytes for that log as the value. My reduce phase then summed all the bytes paired
with each day/month/year combination. To sort the output by byte count, I created an additional map-reduce job whose
map emitted the byte count as the key and day/month/year as the value. 



