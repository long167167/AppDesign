WITH current_sprint AS (
    SELECT
        TOP 1 sprint_id AS current_sprint_id,
        start_date AS current_sprint_start_date,
        end_date AS current_sprint_end_date
    FROM epa_bimoe_prod.o_sprint_planning_dimension_table
    WHERE current_date BETWEEN TO_DATE(LEFT(start_date, 10), 'YYYY-MM-DD') AND DATEADD(day, 2, TO_DATE(LEFT(end_date, 10), 'YYYY-MM-DD'))
    /*
       start_date is the start date of the estimated completion sprint (usually is Monday)
       end_date is the end date of the estimated completion sprint (usually is Friday)
    */
)

select distinct trim(replace(replace(assigneeidentity,'kerberos:',''),'@ANT.AMAZON.COM', '')) AS assignee
                , concat(trim(replace(replace(assigneeidentity,'kerberos:',''),'@ANT.AMAZON.COM', '')),'@amazon.com') as assignee_email
from (select issueid,
             title,
             issueurl,
             assignedfolderid,
             assigneeidentity,
             resolveddate,
             createdate,
             status,
             label_customer,
             estimatedcompletiondate,
             nvl(regexp_count(labelids, ',')+1, 0)
                 - (case when label_customer is not null then 1 else 0 end
                 + case when label_execution is not null then 1 else 0 end
                 + case when label_planning is not null then 1 else 0 end
                 + case when label_product is not null then 1 else 0 end)         as sprint_label_count,
             dateDiff('Day', TO_DATE(resolveddate, 'YYYY-MM-DD'), getdate())      as number_days_resolved,
             CASE WHEN (POSITION((SELECT current_sprint_id FROM current_sprint) IN labelids) <> 0)
                      THEN 'TRUE'
                  ELSE 'FALSE'
             END                                                                   as is_current_sprint_project,
             (SELECT current_sprint_start_date FROM current_sprint)                as current_sprint_start_date,
             (SELECT current_sprint_end_date FROM current_sprint)                  as current_sprint_end_date
      from epa_bimoe_prod.o_sprint_planning_sim_data
      where (label_customer is null or (coalesce(estimatedcompletiondate, resolveddate) is null and sprint_label_count > 0
                                        and (number_days_resolved is null or number_days_resolved < 15)
                                       )  -- est completion is missing. logic is from QS dashboard
            )
      and parenttaskids is null  -- and issueurl = 'https://issues.amazon.com/issues/EP&A_TECH-PROJECT-54'
      and is_current_sprint_project = 'TRUE'
    ) AS sim
LEFT JOIN epa_bimoe_prod.o_sprint_planning_sim_folders AS folder
    ON sim.assignedfolderid = folder.assignedfolderid
LEFT JOIN epa_bimoe_prod.o_sprint_planning_team_structure AS team
    ON trim(replace(replace(sim.assigneeidentity, 'kerberos:',''), '@ANT.AMAZON.COM', '')) = team.alias
where (folder_roll_up in ('BUG', 'Access Request', 'Feature Request') or folder_roll_up is null)
and assigneeidentity is not null



