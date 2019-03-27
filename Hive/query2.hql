Select a.title, a.avg_rating, a.num_ratings from
(
select m.title, avg(r.rating) as avg_rating, count(*) as num_ratings , r.movieid
from imdb_bigdata11.movies m 
join imdb_bigdata11.reviews r 
on m.movieid = r.movieid 
group by r.movieid,m.title 
having (avg_rating > 4 and num_ratings > 10) 
order by avg_rating asc
) 
as a;
