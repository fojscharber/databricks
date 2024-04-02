select
  cltransdetails.cagelotid,
  transdate,
  designation,
  remarks,
  totaltransactioncost,
  cagelotid,
  storeid,
  starttime,
  endtime,
  foodbatchcode,
  transactioncostperfish,
  transactioncostperkg,
  foodid,
  causeid,
  feedingnumber,
  foodquantity
from
  aquamanager_growout_dbo.cltrans,
  aquamanager_growout_dbo.transkind,
  aquamanager_growout_dbo.cltransdetails
where
  cltrans.transkindid = cltrans.transkindid
  and cltrans.cltransid = cltransdetails.cltransid
  and cltrans.transkindid in ({{transactionKind }})
