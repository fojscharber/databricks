-- return all instances where we detected a lenght and compute a biomass
create view view_instances_by_date_and_camera as
select
  `date`,
  cameraName as camera,
  poseCheck as poseDetected,
  depthCheck as depthDetected,
  length3d as lenght,
  biomass,
  minDepth,
  maxDepth
from
  biomassresults
where
  biomassresults.length3d > 0
order by
  cameraName,
  `date`
