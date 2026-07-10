{#
    Great-circle distance in kilometres between two lat/lon pairs.

    ClickHouse ships `geoDistance`, which is faster and more accurate: it uses
    a WGS-84 ellipsoidal approximation rather than a sphere. It is used here in
    preference, with the haversine kept as the portable definition of what the
    function is computing.

    The distinction matters at the airport-matching radius: over 8 km the two
    agree to well under a hundred metres, so either would do, but geoDistance
    returns metres and the models work in kilometres.
#}

{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
    (geoDistance(
        toFloat64({{ lon1 }}),
        toFloat64({{ lat1 }}),
        toFloat64({{ lon2 }}),
        toFloat64({{ lat2 }})
    ) / 1000.0)
{%- endmacro %}
