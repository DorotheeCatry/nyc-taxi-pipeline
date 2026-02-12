with staging as (

    select * from {{ ref('stg_yellow_taxi_trips') }}

),

metrics as (

    select
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        
        -- Métriques numériques
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        
        -- Récupération des calculs faits en staging (ou recalcul ici si besoin)
        trip_duration_minutes,
        average_speed_mph,
        tip_percentage,

        -- 🟢 ENRICHISSEMENT MÉTIER (Demandé dans le brief)
        
        -- 1. Catégorisation des distances
        case 
            when trip_distance <= 1 then 'Court (< 1 mile)'
            when trip_distance <= 5 then 'Moyen (1-5 miles)'
            when trip_distance <= 10 then 'Long (5-10 miles)'
            else 'Très Long (> 10 miles)'
        end as distance_category,

        -- 2. Catégorisation des périodes horaires (Rush, Nuit...)
        case 
            when hour(pickup_datetime) between 6 and 9 then 'Rush Matinal'
            when hour(pickup_datetime) between 10 and 15 then 'Journée'
            when hour(pickup_datetime) between 16 and 19 then 'Rush Soir'
            when hour(pickup_datetime) between 20 and 23 then 'Soirée'
            else 'Nuit'
        end as time_of_day,

        -- 3. Type de jour (Semaine vs Weekend)
        -- dayofweekiso retourne 1 (Lundi) à 7 (Dimanche)
        case
            when dayofweekiso(pickup_datetime) >= 6 then 'Weekend'
            else 'Semaine'
        end as day_type

    from staging

)

select * from metrics