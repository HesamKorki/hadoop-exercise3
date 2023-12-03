-- Load the data
principals = LOAD '/home/users/hkorki/title.principals.tsv' USING PigStorage('\t') AS (tid:chararray, ordering:int, nid:chararray, category:chararray, job:chararray, characters:chararray);
names = LOAD '/home/users/hkorki/name.basics.tsv' USING PigStorage('\t') AS (nid:chararray, primaryName:chararray, bd: int, dd: int, profession: chararray, knownFor: chararray);
titles = LOAD '/home/users/hkorki/title.basics.tsv' USING PigStorage('\t') AS (tid:chararray, type:chararray, primaryTitle: chararray, original: chararray, adult: int, s: int, e: int, r: int, genre: chararray);

-- Keep only movies in the principals
movies = FILTER titles BY type == 'movie';
join_movies = JOIN principals BY tid, movies BY tid;
principal_movies = FOREACH join_movies GENERATE principals::tid AS tid, principals::category AS category, principals::nid AS nid;


-- Keep only actor and director records
acted1 = FILTER principal_movies BY (category == 'actor' OR category == 'actress' OR category == 'director');
acted2 = FILTER principal_movies BY (category == 'actor' OR category == 'actress' OR category == 'director');

-- Self join to get co-appearances
self_join = JOIN acted1 BY tid, acted2 BY tid;
co_actors = FILTER self_join BY acted1::nid < acted2::nid;

-- Count the number of co-appearances
grouped_data = GROUP co_actors BY (acted1::nid, acted2::nid);
count_data = FOREACH grouped_data GENERATE FLATTEN(group) AS (Actor1:chararray, Actor2:chararray), COUNT(co_actors) AS co_count;

-- Only keep the ones that have count >= 4
itemset = FILTER count_data BY co_count > 3;

-- Join with the names to get actor's names
join_name1 = JOIN itemset BY Actor1, names BY nid;
name1 = FOREACH join_name1 GENERATE names::primaryName AS Actor1 , itemset::Actor2 AS Actor2 , itemset::co_count AS co_count;
join_name2 = JOIN name1 BY Actor2, names BY nid;
result = FOREACH join_name2 GENERATE name1::Actor1 AS Actor1, names::primaryName AS Actor2, name1::co_count AS co_count;

-- Final order since the last join shuffles
ordered_result = ORDER result BY co_count DESC;

-- Write the output
STORE ordered_result INTO './pig-top-co-actors';
