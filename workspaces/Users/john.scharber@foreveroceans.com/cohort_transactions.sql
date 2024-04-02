-- trans in/outs of fish in pen along with the associated costs
select
  lot.designation as batch,
  cage.designation as cage,
  date_format(cltrans.transdate, "MM-dd-yy") as date,
  transkind.designation as type,
  cltransdetails.fishintransaction,
  cltransdetails.totaltransactioncost
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
  and lot.designation = "{{ batch }}"
  and cltrans.transkindid in ({{ types }})
order by cltrans.transdate, cltrans.transkindid
