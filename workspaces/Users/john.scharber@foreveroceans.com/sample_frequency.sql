select
  concat(lot.designation, cage.designation) as cohort,
  cltrans.transdate as sampledate,
  cagelot.populationdate as stockingdate,
  cltransdetails.sampledfishno as fishsampled,
  datediff(cltrans.transdate, cagelot.populationdate) as lifecycleday
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot
where
  cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cltrans.transkindid = 4
  and cage.designation != "Virtual Cage"
  and datediff(cltrans.transdate, cagelot.populationdate) <= {{ make lifecycle days }}
  
order by
  lifecycleday
