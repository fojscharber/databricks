select
  tltrans.transdate as date,
  tltrans.transno as number,
  transtype.designation as type,
  lot.designation as batch,
  lot.stockdate,
  lot.ancestororigin,
  lot.origin1,
  lot.straincode,
  lot.cost,
  lot.speciesid,

  tanklot.startdate,
  tanklot.enddate,
  tanklot.hatcheryitemtypeid,
  tanklot.innoculationnumber,
  
  
  tltrans.transtotalvalue as totalvalue,
  tltrans.remarks,
  tltransdetails.weightno1 as weight1,
  tltransdetails.weightno2 as weight2,
  tltransdetails.weightno3 as weight3,
  tltransdetails.weightno4 as weight4,
  tltransdetails.broodstockorigin,
  tltransdetails.transactionmab,
  tltransdetails.ancestororigin,
  tltransdetails.transposition,
  tltransdetails.livefoodrejecttypeid,
  tltransdetails.swimbladdertypeid,
  tltransdetails.foodid,
  tltransdetails.transno1,
  tltransdetails.transno2,
  tltransdetails.transno3,
  tltransdetails.transno4,
  tltransdetails.femalesinseminatedno,
  tltransdetails.eggnoscrap,
  tltransdetails.eggno,
  tltransdetails.eggml,
  tltransdetails.foodbatchcode,
  tltransdetails.storeid,
  tltrans.translasteditedbyuser,
  tltrans.transcreatedbyuser
FROM
  tltrans,
  tltransdetails,
  transtype,
  site,
  tanklot,
  lot
where
  tltrans.tltransid = tltransdetails.tltransid
  and tltrans.transkindid = transtype.transtypeid
  and tltrans.tltransid = tltransdetails.tltransid
  and tltransdetails.tanklotid = tanklot.tanklotid
  and tanklot.lotid = lot.lotid
  and tltrans.transkindid in ({{ kind }})
order by
  transno
