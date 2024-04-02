-- trans in/outs of fish in pen along with the associated costs
-- trans types
-- 1 = batch input
-- 3 = mortal
-- 5 = transfer
-- 6 = fill
-- 7 = adjustment
-- 8 = harvest
-- 12 = culling
create view view_fish_population_transaction as
select
  lot.designation as batch,
  cage.designation as cage,
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
order by
  lot.designation,
  cage.designation,
  cltrans.transdate,
  cltrans.transkindid
