For Hive part, we tried different queries to find out which one has the optimal time.
While grouping/filtering the records first and joining later is the best way to do(first query below), it is initializing 3 mapreduce tasks and is taking more time compared to joining and grouping(second query is taking 2 mapreduce tasks). So we took the readings with the second query.

select b.rank, a.title 
from imdb_bigdata11.movies as a
join
(
	select movieid, count(*) as rank 
	from imdb_bigdata11.reviews 
	group by movieid 
	order by rank
) as b
where a.movieid = b.movieid;

Select a.review_count, a.title from
(
select count(*) as review_count , m.title, r.movieid
from imdb_bigdata11.movies m
join imdb_bigdata11.reviews r
on r.movieid = m.movieid
group by r.movieid,m.title
order by review_count asc
) as a;