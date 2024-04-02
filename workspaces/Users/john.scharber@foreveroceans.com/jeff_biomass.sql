select
  lot.designation as batch,
  cage.designation as cage,
  date_format(cltrans.transdate, "MM-dd-yy") as date,
  transkind.designation as type,
  cltranssampledetails.samplefishno,
  cltranssampledetails.sampleweight,
  cltransdetails.cltranremarks
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot,
  transkind,
  cltranssampledetails
where
  cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.transkindid = transkind.transkindid
  and cltrans.transkindid = 4
  and cltranssampledetails.cltransdetailsid = cltransdetails.cltransdetailsid  
  AND to_date(cltrans.transdate, 'MM-d-yy') >= "{{daterange.start}}"
  and to_date(cltrans.transdate, 'MM-d-yy') <= "{{daterange.end}}"
order by
  lot.designation,
  cage.designation,
  cltrans.transdate,
  cltrans.transkindid
