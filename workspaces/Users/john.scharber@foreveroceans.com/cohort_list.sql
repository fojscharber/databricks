-- enddate defaults to 01-01-00, these are active cohorts
-- virtual cages have enddats so selecting 01-01-00 in the endadate filters
-- leaves only active cohorts
select
  lot.designation as batch,
  cage.designation as cage,
  hatchery.designation as hatchery,
  species.designation,
  date_format(lot.purchasedate, "MM-dd-yy") as purchased,
  date_format(cagelot.populationdate, "MM-dd-yy") as cohortStart,
  date_format(cagelot.enddate, "MM-dd-yy") as enddate,
  lot.lotfish as initialStocking,
  cagelot.initialbiomassafter
from
  cage,
  cagelot,
  lot,
  hatchery,
  species
where
  cagelot.cageid = cage.cageid
  and cagelot.lotid = lot.lotid
  and lot.hatcheryid = hatchery.hatcheryid
  and lot.speciesid = species.speciesid
order by
  cagelot.populationdate
