-- trans in/outs of fish in pen along with the associated costs
select
  lot.designation as batch,
  sum(cltransdetails.fishintransaction) as totalfish
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot,
  transkind
where
  cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.transkindid = transkind.transkindid
  and lot.designation in ({{ batch }})
  and cltrans.transkindid in ({{ types }})
group by
  lot.designation
order by
  sum(fishintransaction) desc,
  lot.designation
