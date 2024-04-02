SELECT
  *
FROM
  sfrfactor,
  sfrtemplate
where
  sfrtemplate.sfrtemplateid = sfrfactor.sfrtemplateid
order by 
  sfrtemplate.templatename
