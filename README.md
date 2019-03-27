## IMDB Data Analysis using MapReduce and Hive queries
Implemented code/queries to rank the movies based on popularity (number of reviews) and find the movies that are having an average rating > 4 stars and number of reviews > 10. 
A comparative study between Mapreduce and Hive was done based on the number of jobs and the time taken for the various datasets provided.

### Input dataset (Small dataset - 100k rows vs Large dataset - 26M rows)
- reviews.csv
** Schema : Reviews(userId, movieId, rating, timestamp) **

- movies.csv
** Schema : Movies(movieId, title, genres) **

### Hive
For inserting data into the movies table from the csv file we made use of CSV SerDe to delimit the fields correctly. SerDe takes care of commas inside the string and reads the file as expected. Since CSVSerDe creates only string fields, we loaded a temporary table first using CSVSerDe and inserted the data into the final table from the temporary table casting the columns as required. 

** Movies table ** 
- Temp table(my_table) for loading the values of movies CSV file 
> CREATE TABLE my_table(movieid int, title string,genres string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  STORED AS TEXTFILE tblproperties("skip.header.line.count"="1"); 

- Loading data to temp table 
> load data local inpath "/HADOOP/hdfs/user/bigdata11/dataset/movies/movies.csv" overwrite into table imdb_bigdata11.my_table; 

- Creating movies table 
> CREATE TABLE movies (movieid INT, title STRING,genres STRING); 

- Inserting data to movies table from the temp table 
> insert into movies select  CAST(movieid as INT), title, genres from my_table; 

- Dropping the temp table after creating movies table 
> drop table my_table; 
 
** Reviews table **
 
- Creating reviews table 
> CREATE TABLE reviews (userid INT,movieid INT, rating DOUBLE,timestamp BIGINT) row format delimited fields terminated BY ',' tblproperties("skip.header.line.count"="1");  

- Loading data to reviews table 
> load data local inpath "/HADOOP/hdfs/user/bigdata11/dataset/reviews/reviews.csv" overwrite into table imdb_bigdata11.reviews_large; 
 

### Mapreduce
For Mapreduce program, Mapside join has been used to join the content of Movies and Reviews files based on movieid. Mapside join is effective when one of the two files can fit entirely in the memory. DistributedCache method is used to add the smaller file (movies.csv) to the cache of the node where the mapper is being executed.

### Conclusion
Hive required less lines of code and less development effort compared to Mapreduce. But with the increase in the size of dataset, hive took more time compared to Mapreduce because of the internal conversion from hive query to Mapreduce jobs and higher level of abstraction.
