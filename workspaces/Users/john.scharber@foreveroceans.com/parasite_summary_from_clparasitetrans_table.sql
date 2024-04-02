select
  transdate as date,
  concat(lot.designation, "-", cage.designation) as cohort,
  totalparasiteno as parasites,
  totalparasitesperfish as perfish,
  totalfishno as totalfish,
  clparasitetrans.mab as mab
from
  clparasitetrans,
  cagelot,
  cage,
  lot
where
  clparasitetrans.cagelotid = cagelot.cagelotid
  and lot.lotid = cagelot.lotid
  and cage.cageid = cagelot.cageid
order by
  cohort,
  date
LIMIT
  all
