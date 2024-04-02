select
  concat(lot.designation,"-", cage.designation) as cohort,
  date_format(cltrans.transdate, "MM-dd-yy") as date,
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
  and to_date(cltrans.transdate, 'yyyy-MM-dd HH:mm:ss') BETWEEN "{{date_range.start}}" and "{{date_range.end}}"
 

order by
  lot.designation,
  cage.designation,
  cltrans.transdate,
  cltrans.transkindid
