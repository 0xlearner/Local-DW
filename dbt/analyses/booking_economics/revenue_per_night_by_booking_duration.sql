SELECT
    ROUND(SUM(one_night_revenue) / NULLIF(SUM(one_night_bookings * 1), 0), 2) AS revenue_per_night_one_night,
    ROUND(SUM(two_night_revenue) / NULLIF(SUM(two_night_bookings * 2), 0), 2) AS revenue_per_night_two_night,
    ROUND(SUM(three_to_six_night_revenue) / NULLIF(SUM(three_to_six_night_bookings * 4.5), 0), 2) AS revenue_per_night_three_to_six,
    ROUND(SUM(seven_plus_night_revenue) / NULLIF(SUM(seven_plus_night_bookings * 10), 0), 2) AS revenue_per_night_seven_plus
FROM {{ ref('fct_booking_economics') }}
