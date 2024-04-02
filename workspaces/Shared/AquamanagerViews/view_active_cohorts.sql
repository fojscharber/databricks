 -- build a list of active cohorts
 -- active cohorts date defaults to "01-01-00"
CREATE view view_active_cohorts as
select
  lot.designation as value,
  lot.designation as name
from
  cage,
  cagelot,
  lot
where
  cagelot.cageid = cage.cageid
  and cagelot.lotid = lot.lotid
  and date_format(cagelot.enddate, "MM-dd-yy") = "01-01-00"
