select
  *
from
  view_weight_wise_mortality
where
  cage_age_in_days >= {{ start_age }}
  and cage_age_in_days <= {{ end_age }}
  and cage_name != "Virtual Cage"
  and mortality_no <= {{ max_morts}}
  
