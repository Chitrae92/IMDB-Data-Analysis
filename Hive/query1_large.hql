Select a.review_count, a.title from
(
select count(*) as review_count , m.title, r.movieid
from imdb_bigdata11.movies_large m
join imdb_bigdata11.reviews_large r
on r.movieid = m.movieid
group by r.movieid,m.title
order by review_count asc
) as a;
