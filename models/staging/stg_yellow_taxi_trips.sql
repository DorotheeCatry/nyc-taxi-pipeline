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

pre_computed as (
    select 
        -- Génération de la clé unique
        {{ dbt_utils.generate_surrogate_key([
            'vendorid', 
            'tpep_pickup_datetime', 
            'pulocationid', 
            'dolocationid',
            'trip_distance' 
        ]) }} as trip_id, -- J'ai ajouté trip_distance pour rendre la clé encore plus unique
        
        vendorid as vendor_id,
        to_timestamp(tpep_pickup_datetime / 1000000) as pickup_datetime,
        to_timestamp(tpep_dropoff_datetime / 1000000) as dropoff_datetime,
        cast(pulocationid as int) as pickup_location_id,
        cast(dolocationid as int) as dropoff_location_id,
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as float) as trip_distance,
        cast(fare_amount as float) as fare_amount,
        cast(tip_amount as float) as tip_amount,
        cast(total_amount as float) as total_amount,
        cast(payment_type as int) as payment_type
    from source
),

deduplicated as (
    select *,
        row_number() over (
            partition by trip_id 
            order by pickup_datetime
        ) as rn
    from pre_computed
),

final as (
    select 
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,
        datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes
    from deduplicated
    where rn = 1 -- On ne garde que l'original, suppression stricte des doublons
    
    -- Filtres de qualité
    and fare_amount >= 0
    and tip_amount >= 0
    and total_amount >= 0
    and pickup_datetime < dropoff_datetime
    and trip_duration_minutes < 600
    and vendor_id is not null
    and pickup_location_id is not null
    and dropoff_location_id is not null
    and trip_distance between 0.1 and 100
)

select * from final