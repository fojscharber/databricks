-- sample summary by date
CREATE or replace view view_sample_summary_by_date as
select
  lot.designation as batch,
  cage.designation as cage,
  date_format(cltrans.transdate, "'yyyy-MM-dd'") as date,
  sum(cltranssampledetails.samplefishno) as fishSampled,
  format_number(avg(cltranssampledetails.sampleweight), "##,###.##") as avgWeight
from
  cltrans,
  cltransdetails,
  cage,
  cagelot,
  lot,
  transkind,
  cltranssampledetails
where
  cltrans.cltransid = cltransdetails.cltransid
  and cltransdetails.cagelotid = cagelot.cagelotid
  and cagelot.lotid = lot.lotid
  and cage.cageid = cagelot.cageid
  and cltrans.transkindid = transkind.transkindid
  and cltrans.transkindid = 4
  and cltranssampledetails.cltransdetailsid = cltransdetails.cltransdetailsid
group by
  lot.designation,
  cage.designation,
  cltrans.transdate
order by
  lot.designation,
  cage.designation,
  cltrans.transdate
