SELECT date_, batch_name, cage_name,  SFR_Pred, SFR_Real, (SFR_Pred - SFR_Real) * -1 as sfr_diff FROM view_daily_unit_report
