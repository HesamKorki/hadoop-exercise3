-- Load the data
principals = LOAD '/home/users/hkorki/title.principals.tsv' USING PigStorage('\t') AS (tid:chararray, ordering:int, nid:chararray, category:chararray, job:chararray, characters:chararray);
names = LOAD '/home/users/hkorki/name.basics.tsv' USING PigStorage('\t') AS (nid:chararray, primaryName:chararray, bd: int, dd: int, profession: chararray, knownFor: chararray);

-- Keep only director records
directed = FILTER principals BY category == 'director';

-- Group and count the occurance of each director
group_directed = GROUP directed BY nid;
count_directed = FOREACH group_directed GENERATE group AS nid, COUNT(directed) AS count;
order_directed = ORDER count_directed BY count DESC;

-- Keep only top 25 then perform a less compute intensive join
top25_directed = LIMIT order_directed 25;
joined_result = JOIN top25_directed BY nid, names BY nid;
result = FOREACH joined_result GENERATE top25_directed::nid AS nid, names::primaryName AS name, top25_directed::count AS count;

-- Order the results
result_ordered = ORDER result BY count DESC;

-- Write the output
STORE result_ordered INTO './pig-top-directors';
