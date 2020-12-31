-- show global variables like '%secure_file_priv%';

set autocommit=0;

load data CONCURRENT
infile '/var/lib/mysql/movie.csv'
into table movie
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

commit;

load data CONCURRENT
infile '/var/lib/mysql/product.csv'
into table product
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

commit;

load data CONCURRENT
infile '/var/lib/mysql/product_actor.csv'
into table product_actor
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

commit;

load data CONCURRENT
infile '/var/lib/mysql/product_director.csv'
into table product_director
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

commit;

load data CONCURRENT
infile '/var/lib/mysql/product_genres.csv'
into table product_genres
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

commit;