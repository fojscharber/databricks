SELECT
  site.designation as region,
  sitegroup.designation as site,
  cage.designation as unit,
  lot.designation as batch,
  date_format(cagelot.populationdate, "MM-dd-yy") as PopulationStart,
  species.designation,
  lot.lotfish as initialStockingCount
from
  site,
  sitegroup,
  cage,
  cagelot,
  lot,
  species
where
  site.sitegroupid = sitegroup.sitegroupid
  and cage.siteid = site.siteid
  and cagelot.cageid = cage.cageid
  and species.speciesid = lot.speciesid
  and lot.designation = "SR-C06B22MxEc"
