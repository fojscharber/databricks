-- feed history view
create or replace view view_feeding_transactions as
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
  and cltrans.transkindid = 2
  and store.storeid = cltransdetails.storeid
  and food.foodid = cltransdetails.foodid
  and cltransdetails.`_fivetran_deleted` = false
order by
  batch,
  cage,
  cltrans.transdate,
  cltrans.transkindid
