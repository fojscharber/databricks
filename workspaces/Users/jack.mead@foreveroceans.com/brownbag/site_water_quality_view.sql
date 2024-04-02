CREATE
OR REPLACE VIEW site_water_quality AS
WITH mapping AS (
  SELECT
    reading,
    type
  FROM
    (
      VALUES
        ('tchainTemp1', 'temperature'),
        ('exo2Temperature', 'temperature'),
        ('exo2Ph', 'ph'),
        ('exo2Turbidity', 'turbidity'),
        ('exo2Salinity', 'salinity'),
        ('exo2BgaPe', 'bga'),
        ('tchainDo1', 'dissolved_oxygen'),
        ('exo2DoSat', 'dissolved_oxygen'),
        ('exo2Do', 'dissolved_oxygen'),
        ('tchainTemp2', 'temperature'),
        (
          'exo2SpecificConductivity',
          'specific_conductivity'
        ),
        ('exo2Chlorophyll', 'chlorophyll'),
        ('tchainDo2', 'dissolved_oxygen'),
        ('esondeDoSat', 'dissolved_oxygen'),
        ('esondePh', 'ph'),
        ('esondeDoMgl', 'dissolved_oxygen'),
        ('esondeTemp', 'temperature'),
        ('esondeTurb', 'turbidity'),
        ('esondeCond', 'specific_conductivity')
    ) as t(reading, type)
)
SELECT
  CAST(wq.site_id AS INT) as site_id,
  wq.sensor,
  wq.reading,
  wq.metrics.avg as value,
  wq.window.start as timestamp,
  CAST(wq.window.start as DATE) as date,
  mapping.type
FROM
  hive_metastore.default.water_quality_aggregations wq
  JOIN mapping ON wq.reading = mapping.reading
WHERE CAST(wq.window.start as DATE) >= date_sub(current_date(), 90)
