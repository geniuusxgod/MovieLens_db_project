USE DATABASE MOVIELENSDB;

SELECT 
    dg.name AS genre_name,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_genres dg ON fr.dim_genreId = dg.dim_genreId
GROUP BY dg.name
ORDER BY avg_rating DESC
LIMIT 10;

SELECT 
    dm.title AS movie_title,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.dim_movieId = dm.dim_movieId
GROUP BY dm.title
ORDER BY total_ratings DESC
LIMIT 10;

SELECT 
    dd.year,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_date dd ON fr.dim_dateId = dd.dim_dateId
GROUP BY dd.year
ORDER BY dd.year;

SELECT 
    dt.hours AS hour_of_day,
    du.age_group,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.dim_timeId = dt.dim_timeId
JOIN dim_users du ON fr.dim_userId = du.dim_userId
GROUP BY dt.hours, du.age_group
ORDER BY dt.hours ASC, du.age_group;

SELECT 
    du.occupation,
    AVG(fr.rating) AS total_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userId = du.dim_userId
GROUP BY du.occupation;
