-- Table des patterns horaires :
-- Nom : FINAL.hourly_patterns
-- Métriques par heure : demande, revenus, vitesse moyenne

with trips as (
    select * from {{ ref('int_trip_metrics') }}
),

hourly_agg as (
    select 
        hour(pickup_datetime) as pickup_hour,
        time_of_day,
        count(trip_id) as hourly_demand,                  -- Demande par heure
        avg(average_speed_mph) as avg_speed,              -- Vitesse moyenne
        avg(total_amount) as hourly_revenu,               -- Revenus moyens from source      
        avg(fare_amount) as avg_fare_per_trip
    from trips 
    group by 1, 2) 

select * from hourly_agg order by pickup_hour, time_of_day