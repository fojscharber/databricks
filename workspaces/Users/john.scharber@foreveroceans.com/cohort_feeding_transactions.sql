-- feed history view
select
  lot.designation as batch,
  cage.designation as cage,
  date_format(cltrans.transdate, "MM-dd-yy") as date,
  transkind.designation as type,
  cltransdetails.feedingnumber,
  cltransdetails.foodquantity,
  date_format(cltransdetails.starttime, "MM-dd-yy HH:mm") as startTime,
  date_format(cltransdetails.endtime, "MM-dd-yy HH:mm") as endTime,
  store.designation as store,
  cltransdetails.foodbatchcode,
  food.designation as foodType,
  cltransdetails.barcodetransactionid,
  cltransdetails.feedingdifferencejustificationid,
  cltransdetails.totaltransactioncost,
  cltransdetails.transactioncostperfish,
  cltransdetails.transactioncostperkg,
  cltransdetails.cltranremarks
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
  and lot.designation = "{{ batch }}"
  and cltrans.transkindid in ({{ types }})
  and store.storeid = cltransdetails.storeid
  and food.foodid = cltransdetails.foodid
order by
  cltrans.transdate,
  cltrans.transkindid
