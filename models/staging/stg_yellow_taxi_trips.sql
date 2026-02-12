/*
Actions de nettoyage requises :
- Éliminer les montants négatifs (fare_amount, total_amount)
- Garder seulement les trajets avec pickup < dropoff
- Filtrer les distances entre 0.1 et 100 miles
- Exclure les zones NULL (PULocationID, DOLocationID)

Enrichissements à ajouter :
- Calcul de la durée du trajet (en minutes)
- Extraction des dimensions temporelles (heure, jour, mois)
- Calcul de la vitesse moyenne
- Calcul du pourcentage de pourboire par rapport au montant total

Tables à créer dans FINAL :
*Table de résumé quotidien :
- Nom : FINAL.daily_summary
- Métriques par jour : nombre de trajets, distance moyenne, revenus totaux
- Groupement par date de pickup

*Table d'analyse par zone :
- Nom : FINAL.zone_analysis
- Métriques par zone de départ : volume, revenus moyens, popularité

*Table des patterns horaires :
- Nom : FINAL.hourly_patterns
- Métriques par heure : demande, revenus, vitesse moyenne
*/

with source as (
    select * from {{ source('nyc_taxi_source', 'yellow_taxi_trips') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance 
            order by tpep_pickup_datetime
        ) as rn
    from source
),

cleaned as (
    select 
        -- Identifiant unique (Généré par dbt)
        {{ dbt_utils.generate_surrogate_key([
            'vendorid', 
            'tpep_pickup_datetime', 
            'pulocationid', 
            'dolocationid'
        ]) }} as trip_id,
        vendorid as vendor_id,

        -- Conversion des timestamps (Format Microsecondes -> Timestamp)
        to_timestamp(tpep_pickup_datetime / 1000000) as pickup_datetime,
        to_timestamp(tpep_dropoff_datetime / 1000000) as dropoff_datetime,

        -- Localisation
        cast(pulocationid as int) as pickup_location_id,
        cast(dolocationid as int) as dropoff_location_id,

        -- Métriques brutes
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as float) as trip_distance,

        -- Tarification
        cast(fare_amount as float) as fare_amount,
        cast(tip_amount as float) as tip_amount,
        cast(total_amount as float) as total_amount,
        cast(payment_type as int) as payment_type

    from deduplicated
    where rn = 1 -- Garder seulement la première occurrence pour chaque groupe de doublons
),

enriched as (
    select
    *,
    -- Calcul de la durée du trajet en minutes
    datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,

    -- Extraction des dimensions temporelles
    hour(pickup_datetime) as pickup_hour,
    day(pickup_datetime) as pickup_day,
    month(pickup_datetime) as pickup_month,
    year(pickup_datetime) as pickup_year,

    -- Calcul du pourcentage de pourboire par rapport au montant total
    case 
        when (total_amount - tip_amount) > 0 
        then (tip_amount / (total_amount - tip_amount)) * 100 
        else 0 
    end as tip_percentage
    from cleaned

),


final as (
    select *,

        -- Calcul de la vitesse moyenne (miles par heure)
        case 
            when trip_duration_minutes > 0 
            then (trip_distance / (trip_duration_minutes / 60)) 
            else 0 
        end as average_speed_mph,

    from enriched 
    where
        -- Filtrer les montants négatifs
        fare_amount >= 0
        and tip_amount >= 0
        and total_amount >= 0

        -- Exclure les trajets avec dates incohérentes
        and pickup_datetime < dropoff_datetime
        and trip_duration_minutes < 600

        -- Gérer les valeurs manquantes (Exclure les enregistrements avec des champs critiques manquants)
        and vendor_id is not null
        and pickup_datetime is not null and dropoff_datetime is not null
        and pickup_location_id is not null and dropoff_location_id is not null
        and passenger_count is not null
        and trip_distance is not null

        -- Supprimer les outliers extrêmes (< 0.1 miles ou > 100 miles)
        and trip_distance between 0.1 and 100
)


select * from final
