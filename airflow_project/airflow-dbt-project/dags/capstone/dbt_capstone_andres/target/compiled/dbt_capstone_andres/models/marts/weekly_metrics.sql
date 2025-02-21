

SELECT *
from  DATAEXPERT_STUDENT.andres.audit_weekly_metrics 



where week_start_date >= (select coalesce(max(week_start_date)-7,'1900-01-01') from DATAEXPERT_STUDENT.andres.weekly_metrics )

