select
  sampledate,
  concat(lot.designation, "-", cage.designation) as batch,
  clparasitesamples.fishno as totalfish,
  clparasitesamples.positive as positivetest,
  clparasitesamplesdetails.parasiteno,
  parasitetypes.designation as parasitetype
from
  clparasitesamplesdetails,
  clparasitesamples,
  clparasitetrans,
  cagelot,
  cage,
  lot,
  parasitetypes
where
  clparasitesamples.clparasitesamplesid = clparasitesamplesdetails.clparasitesamplesid
  and clparasitetrans.clparasitetransid = clparasitesamples.clparasitetransid
  and cagelot.cagelotid = clparasitetrans.cagelotid
  and cage.cageid = cagelot.cageid
  and lot.lotid = cagelot.lotid
  and parasitetypes.parasitetypesid = clparasitesamplesdetails.parasitetypeid
  and sampledate > "{{date_range.start}}" and sampledate < "{{date_range.end}}"
order by
  sampledate desc
