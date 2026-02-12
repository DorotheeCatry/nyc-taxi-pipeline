-- Table d'analyse par zone :
-- Nom : FINAL.zone_analysis
-- Métriques par zone de départ : volume, revenus moyens, popularité

with trips as (
    select * from {{ ref('int_trip_metrics') }} 
),

zone_agg as (
    select
        pickup_location_id as pickup_zone,          -- Volume de trajets par zone de départ 
        count(trip_id) as total_departures,         -- Revenus moyens par zone de départ 
        avg(total_amount) as avg_ticket_price,      -- Popularité de la zone (pourcentage de pourboire moyen)
        avg(tip_percentage) as avg_tip_ptc,
        avg(trip_distance) as avg_trip_distance 
    from trips 
    group by 1) 
    
select * from zone_agg order by total_departures desc