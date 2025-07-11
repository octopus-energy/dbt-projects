SELECT
  id AS workpreference_isochrone_id
  , workpreference_id AS work_preference_id
  , isochrone_id
FROM {{ source('src_coconut', 'scheduling_workpreference_isochrones') }}
