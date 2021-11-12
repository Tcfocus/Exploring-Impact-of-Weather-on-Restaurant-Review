-- Integrating Yelp and weather data -- create a view via query that holds Review, Weather, and Precipitation data

CREATE VIEW weather_yelp_integration AS
SELECT r.business_id, r.cool, r.date, r.funny, r.review_id, r.stars, r.text, r.useful, r.user_id, w.min_temp, w.max_temp, p.precipitation
FROM review as r
JOIN weather as w
ON to_date(r.date) = w.date
JOIN precipitation as p
on to_date(r.date) = p.date;