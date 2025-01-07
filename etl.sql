CREATE OR REPLACE DATABASE MovieLensDB;

CREATE OR REPLACE SCHEMA MovieLensDB.staging;

USE SCHEMA MovieLensDB.staging;

CREATE OR REPLACE TABLE age_group_staging(
    age_groupId INT PRIMARY KEY,
    name VARCHAR(10)
);

CREATE OR REPLACE TABLE genres_staging(
    genreId INT PRIMARY KEY,
    name VARCHAR(30)
);

CREATE OR REPLACE TABLE movies_staging(
    movieId INT PRIMARY KEY,
    title VARCHAR(100),
    release_year CHAR(4)
);


CREATE OR REPLACE TABLE genres_movies_staging(
    genres_moviesId INT PRIMARY KEY,
    movieId INT,
    genreId INT,
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId),
    FOREIGN KEY (genreId) REFERENCES genres_staging(genreId)
);

CREATE OR REPLACE TABLE occupations_staging(
    occupationId INT PRIMARY KEY,
    name VARCHAR(30)
);

CREATE OR REPLACE TABLE users_staging(
    userId INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupationId INT,
    zip_code VARCHAR(10),
    FOREIGN KEY (occupationId) REFERENCES occupations_staging(occupationId),
    FOREIGN KEY (age) REFERENCES age_group_staging(age_groupId)
);

CREATE OR REPLACE TABLE ratings_staging(
    ratingId INT PRIMARY KEY,
    userId INT,
    movieId INT,
    rating INT,
    rated_at TIMESTAMP,
    FOREIGN KEY (userId) REFERENCES users_staging(userId),
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId)
);

CREATE OR REPLACE TABLE tags_staging(
    tagId INT PRIMARY KEY,
    userId INT,
    movieId INT,
    tags VARCHAR(100),
    created_at TIMESTAMP,
    FOREIGN KEY (userId) REFERENCES users_staging(userId),
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId)
);


CREATE OR REPLACE STAGE project_stage;

CREATE OR REPLACE FILE FORMAT CSV
TYPE='CSV'
FIELD_DELIMITER=','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = NONE
SKIP_HEADER=1;

COPY INTO age_group_staging
FROM @project_stage/age_group.csv
FILE_FORMAT = CSV;

COPY INTO genres_movies_staging
FROM @project_stage/genres_movies.csv
FILE_FORMAT = CSV;

COPY INTO genres_staging
FROM @project_stage/genres.csv
FILE_FORMAT = CSV;

COPY INTO movies_staging
FROM @project_stage/movies.csv
FILE_FORMAT = CSV;

COPY INTO occupations_staging
FROM @project_stage/occupations.csv
FILE_FORMAT = CSV;

COPY INTO ratings_staging
FROM @project_stage/ratings.csv
FILE_FORMAT = CSV;

COPY INTO users_staging
FROM @project_stage/users.csv
FILE_FORMAT = CSV;

COPY INTO tags_staging
FROM @project_stage/tags.csv
FILE_FORMAT = CSV
ON_ERROR = "CONTINUE";

CREATE TABLE dim_movies AS
SELECT DISTINCT
movieId AS dim_movieId,
title,
release_year AS release_year
FROM movies_staging;


CREATE TABLE dim_genres AS
SELECT DISTINCT
genreId AS dim_genreId,
name
FROM genres_staging;

CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT 
us.userId AS dim_userId,
CASE 
        WHEN us.age < 18 THEN 'Under 18'
        WHEN us.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN us.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN us.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN us.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN us.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
oc.name AS occupation
FROM users_staging us
JOIN age_group_staging ag ON us.age = ag.age_groupId
JOIN occupations_staging oc ON us.occupationId = oc.occupationId;

CREATE TABLE dim_tags AS
SELECT DISTINCT
tagId AS dim_tagId,
tags,
created_at
FROM tags_staging;

CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateId,
    CAST(rated_at AS DATE) AS date,
    DATE_PART(year, rated_at) AS year,  
    DATE_PART(quarter, rated_at) AS quarter,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(week, rated_at) AS week,
    DATE_PART(day, rated_at) AS day
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at),  
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(quarter, rated_at);

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', rated_at)) AS dim_timeId,                   
    rated_at AS timestamp,
    EXTRACT(HOUR FROM rated_at) AS hours,
    EXTRACT(MINUTE FROM rated_at) AS minutes,
    EXTRACT(SECOND FROM rated_at) AS seconds
FROM ratings_staging
GROUP BY rated_at;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    ra.ratingId AS fact_ratingId,
    ra.rating AS rating,
    ra.rated_at AS rated_at,
    dd.dim_dateId AS dim_dateId,
    dt.dim_timeId AS dim_timeId,
    du.dim_userId AS dim_userId,
    dm.dim_movieId AS dim_movieId,
    ts.tagId AS dim_tagId, 
    dg.dim_genreId AS dim_genreId
FROM ratings_staging ra
JOIN dim_date dd ON CAST(ra.rated_at AS DATE) = dd.date
JOIN dim_time dt ON ra.rated_at = dt.timestamp
JOIN dim_movies dm ON ra.movieId = dm.dim_movieId
JOIN dim_users du ON ra.userId = du.dim_userId
JOIN tags_staging ts ON ra.movieId = ts.movieId
JOIN genres_movies_staging gms ON ra.movieId = gms.movieId
JOIN dim_genres dg ON gms.genreId = dg.dim_genreId;

DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;