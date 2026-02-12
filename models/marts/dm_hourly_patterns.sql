-- Table des patterns horaires :
-- Nom : FINAL.hourly_patterns
-- Métriques par heure : demande, revenus, vitesse moyenne

with source as (
    select * from {{ ref('stg_yellow_tripdata') }}),

hourly_agg as (
    select 
        date_trunc('hour', pickup_datetime) as trip_hour, -- Métriques par heure
        count(trip_id) as trips_volume,                   -- Demande
        avg(average_speed_mph) as avg_speed,              -- Vitesse moyenne
        avg(total_amount) as hourly_revenu,               -- Revenus moyens from source      
        avg(fare_amount) as avg_fare_per_trip
        from source
    group by 1 ) 

select * from hourly_agg 
order by trip_hour asc