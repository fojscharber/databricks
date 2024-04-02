create
or replace view view_growout_adjustments as
select
  transdate,
  correctiontype.designation as correction_type,
  lot.designation as batch,
  cage.designation as cage,
  remarks as comments,
  fishintransaction,
  fishnoafter,
  transactioncostperfish,
  totaltransactioncost
from
  cltrans,
  cltransdetails,
  correctiontype,
  cagelot,
  cage,
  lot
where
  transkindid = 7
  and cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.correctiontypeid = correctiontype.correctiontypeid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cagelot.cageid = cage.cageid
  and isvirtual = false
  and cltrans.`_fivetran_deleted` = false
  and cltransdetails.`_fivetran_deleted` = false
  and correctiontype.`_fivetran_deleted` = false
  and cage.`_fivetran_deleted` = false
  and cagelot.`_fivetran_deleted` = false
  and lot.`_fivetran_deleted` = false
