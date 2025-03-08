-- Revenue breakdown by booking duration
SELECT
    SUM(one_night_revenue) AS one_night_revenue,
    SUM(two_night_revenue) AS two_night_revenue,
    SUM(three_to_six_night_revenue) AS three_to_six_night_revenue,
    SUM(seven_plus_night_revenue) AS seven_plus_night_revenue,

    ROUND(SUM(one_night_revenue) * 100.0 / NULLIF(SUM(one_night_revenue + two_night_revenue + three_to_six_night_revenue + seven_plus_night_revenue), 0), 2) AS pct_one_night,
    ROUND(SUM(two_night_revenue) * 100.0 / NULLIF(SUM(one_night_revenue + two_night_revenue + three_to_six_night_revenue + seven_plus_night_revenue), 0), 2) AS pct_two_night,
    ROUND(SUM(three_to_six_night_revenue) * 100.0 / NULLIF(SUM(one_night_revenue + two_night_revenue + three_to_six_night_revenue + seven_plus_night_revenue), 0), 2) AS pct_three_to_six_night,
    ROUND(SUM(seven_plus_night_revenue) * 100.0 / NULLIF(SUM(one_night_revenue + two_night_revenue + three_to_six_night_revenue + seven_plus_night_revenue), 0), 2) AS pct_seven_plus_night
FROM {{ ref('fct_booking_economics') }}
