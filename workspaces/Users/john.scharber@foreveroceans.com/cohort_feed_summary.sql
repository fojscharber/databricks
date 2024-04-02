-- feed history view
select
  lot.designation as batch,
  cage.designation as cage,
  date_format(cltrans.transdate, "MM-dd-yy") as date,
  count(cltransdetails.feedingnumber),
  sum(cltransdetails.foodquantity),
  sum(cltransdetails.totaltransactioncost),
  sum(cltransdetails.transactioncostperfish),
  sum(cltransdetails.transactioncostperkg)
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot,
  transkind,
  store,
  food
where
  cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.transkindid = transkind.transkindid
  and lot.designation in ({{ batch }})
  and cltrans.transkindid in (2)
  and store.storeid = cltransdetails.storeid
  and food.foodid = cltransdetails.foodid
  and cltransdetails.foodquantity >= {{ minfeed }}
  and cltransdetails.foodquantity <= {{ maxfeed }}
group by
  cltrans.transdate,
  lot.designation,
  cage.designation
order by
  date asc
