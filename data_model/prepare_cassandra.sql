CREATE KEYSPACE movielens_small WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE movielens_small.ratings (user_id bigint, movie_id bigint,rating float,timestamp timestamp, PRIMARY KEY(user_id, movie_id));
CREATE TABLE movielens_small.tags (user_id bigint, movie_id bigint,tag text,timestamp timestamp, PRIMARY KEY(user_id, movie_id, tag));
CREATE TABLE movielens_small.movies (movie_id bigint, title text, genres set<text>, PRIMARY KEY(movie_id));
CREATE TABLE movielens_small.links (movie_id bigint, imdb_id bigint, tmdb_id bigint, PRIMARY KEY(movie_id));

