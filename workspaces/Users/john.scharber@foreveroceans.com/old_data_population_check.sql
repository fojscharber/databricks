select
  lot.designation as batch,
  cage.designation as cage,
  to_timestamp(cltrans.transdate, "mm-dd-yy") as date,
  transkind.designation as type,
  cause.designation,
  cltransdetails.fishintransaction,
  cltransdetails.totaltransactioncost
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot,
  transkind
  left join cause on cltransdetails.causeid = cause.causeid
where
  cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.transkindid = transkind.transkindid
  and cltrans.transkindid in (1, 3, 5, 6, 7, 8, 12)
  and lot.designation like "SR%"
  and cage.designation != "Virtual Cage"
  and cltransdetails.`_fivetran_deleted` = false

order by
  lot.designation,
  cage.designation,
  cltrans.transdate,
  cltrans.transkindid
