-- multiply weight by 1.1 to allow for 10% blood loss
-- multiply yield by 0.453592 to convert it to kg
create or replace view view_summary_of_processed_fish as
SELECT
  date_format(date, 'yyyy-MM-dd') as date,
  cohort,
  (count2To4 + count4To6 + count6Plus) as totalFish,
  (weight2To4 + weight4To6 + weight6Plus) * 1.1 as weightkg,
  (
    finalWeightLbs2To4 + finalWeightLbs4To6 + finalWeightLbs6Plus
  ) * 0.453592 as yieldWeightkg,
  (yieldWeightkg / weightkg) as yieldPercentage
FROM
  harvetprocessingdata
ORDER BY
  date
