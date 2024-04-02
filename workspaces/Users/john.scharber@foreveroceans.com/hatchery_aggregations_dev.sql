SELECT
  lot.designation as batch,
  tank.designation as tank,
  transkind.designation as type,
  lot.straincode as strain,
  species.designation as species,
  tltransdetails.broodstockorigin,
  larvaeproductionstage.designation as laraproductionstage,
  larvaeproductionstage.orderno as orderno,
  larvaeproductionstage.isnursery as isnursery,
  hatcheryitem.designation as hatcheryitemtype,
  tank.tankvolume as tankvolume,
  tank.cz as cz,
  tltransdetails.broodstocktanks as broodstocktanks,
  tanklot.spawningdate as spawned,
  tanklot.startdate as tanklotstartdate,
  tanklot.enddate as tanklotenddate,
  tanklot.innoculationnumber as innoculations,
  tanklot.startage as age,
  store.designation as store,
  tltransdetails.foodid as wipneedsjoinperfeedtype,
  tltransdetails.foodbatchcode as foodbatch,
  livefoodrejecttype.designation as livefoodrejectedf,
  correctiontype.designation as correctiontype,
  tltransdetails.fishlength,
  cause.designation as cause,
  tltransdetails.previousage,
  tltransdetails.gradingcode,
  tltransdetails.weightno1 as weight1,
  tltransdetails.weightno2 as weight2,
  tltransdetails.weightno3 as weight3,
  tltransdetails.weightno4 as weight4,
  tltransdetails.transactionmab,
  tltransdetails.ancestororigin,
  tltransdetails.transposition,
  tltransdetails.swimbladdertypeid,
  tltransdetails.transno1,
  tltransdetails.transno2,
  tltransdetails.transno3,
  tltransdetails.transno4,
  tltransdetails.femalesinseminatedno,
  tltransdetails.eggnoscrap,
  tltransdetails.eggno,
  tltransdetails.eggml,
  tltrans.translasteditedbyuser,
  tltrans.transcreatedbyuser
FROM
  htransanalysis,
  tltransdetails,
  tltrans,
  transkind,
  tanklot,
  tank,
  lot,
  species,
  larvaeproductionstage,
  hatcheryitem
  LEFT JOIN store on tltransdetails.storeid = store.storeid
  LEFT JOIN livefoodrejecttype on tltransdetails.livefoodrejecttypeid = livefoodrejecttype.livefoodrejecttypeid
  LEFT JOIN correctiontype on tltransdetails.correctiontypeid = correctiontype.correctiontypeid
  LEFT JOIN cause on tltransdetails.causeid = cause.causeid
where
  htransanalysis.tltransdetailsid = tltransdetails.tltransdetailsid
  and tltrans.tltransid = tltransdetails.tltransid
  and transkind.transkindid = tltrans.transkindid
  and tanklot.tanklotid = tltransdetails.tanklotid
  and tanklot.tankid = tank.tankid
  and tanklot.lotid = lot.lotid
  and tltrans.transkindid in ({{ kind }})
  and tank.isvirtual = false
  and lot.speciesid = species.speciesid
  and tank.larvaeproductionstageid = larvaeproductionstage.larvaeproductionstageid
  and tank.hatcheryitemid = hatcheryitem.hatcheryitemid
order by
  transkind.designation
