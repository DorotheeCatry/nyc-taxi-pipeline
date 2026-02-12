-- Table de résumé quotidien :
-- Nom : FINAL.daily_summary
-- Métriques par jour : nombre de trajets, distance moyenne, revenus totaux
-- Groupement par date de pickup

with trips as (
    select * from {{ ref('int_trip_metrics') }}
),

daily_agg as (
    select
        date_trunc('day', pickup_datetime) as trip_date, -- Métriques par jour, date de pickup
        count(trip_id) as total_trips,                   -- Nombre de trajets
        avg(trip_distance) as avg_distance,              -- Distance moyenne
        sum(total_amount) as total_revenue               -- Revenus totaux
    from trips 
    group by 1)

select * from daily_agg order by trip_date desc
