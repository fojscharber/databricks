select
  transdate as date,
  concat(lot.designation, "-", cage.designation) as cohort,
  totalparasiteno as parasites,
  totalparasitesperfish as perfish,
  totalfishno as totalfish,
  clparasitetrans.mab as mab
from
  clparasitetrans,
  clparasitesamples,
  clparasitesamplesdetails,
  parasitetypes,
  cagelot,
  cage,
  lot
where
  clparasitesamplesdetails.parasitetypeid = parasitetypes.parasitetypesid
  and clparasitesamples.clparasitesamplesid = clparasitesamplesdetails.clparasitesamplesid
  and clparasitetrans.clparasitetransid = clparasitesamples.clparasitetransid
  and clparasitetrans.cagelotid = cagelot.cagelotid
  and lot.lotid = cagelot.lotid
  and cage.cageid = cagelot.cageid
order by
  cohort,
  date
LIMIT all
