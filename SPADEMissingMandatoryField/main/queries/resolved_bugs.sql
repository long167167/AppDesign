SELECT t.* FROM
    (SELECT DISTINCT
                    SIM.ISSUEURL,
                    SIM.TITLE,
                    --SIM.FOLDER_NAME,
                    TRIM(REPLACE(REPLACE(ASSIGNEEIDENTITY, 'kerberos:', ''), '@ANT.AMAZON.COM', ''))   AS ASSIGNEE,
                    SIM.MANAGER_EMAIL,
                    CASE
                        WHEN SIM.LABEL_CUSTOMER IS NULL AND SIM.LABEL_PRODUCT IS NULL AND
                            SIM.SPRINT_LABEL_COUNT = 0
                            THEN 'Customer, Product, Sprint Tag'
                        WHEN SIM.LABEL_CUSTOMER IS NOT NULL AND SIM.LABEL_PRODUCT IS NULL AND
                            SIM.SPRINT_LABEL_COUNT = 0
                            THEN 'Product, Sprint Tag'
                        WHEN SIM.LABEL_CUSTOMER IS NULL AND SIM.LABEL_PRODUCT IS NOT NULL AND
                            SIM.SPRINT_LABEL_COUNT = 0
                            THEN 'Customer, Sprint Tag'
                        WHEN SIM.LABEL_CUSTOMER IS NULL AND SIM.LABEL_PRODUCT IS NULL AND
                            SIM.SPRINT_LABEL_COUNT > 0
                            THEN 'Customer, Product'
                        WHEN SIM.LABEL_CUSTOMER IS NOT NULL AND SIM.LABEL_PRODUCT IS NOT NULL AND
                            SIM.SPRINT_LABEL_COUNT = 0
                            THEN 'Sprint Tag'
                        WHEN SIM.LABEL_CUSTOMER IS NULL AND SIM.LABEL_PRODUCT IS NOT NULL AND
                            SIM.SPRINT_LABEL_COUNT > 0
                            THEN 'Customer'
                        WHEN SIM.LABEL_CUSTOMER IS NOT NULL AND SIM.LABEL_PRODUCT IS NULL AND
                            SIM.SPRINT_LABEL_COUNT > 0
                            THEN 'Product'
                        ELSE 'No'
                        END                                                                           AS MISSING_LABELS,
                    CASE
                        WHEN SIM.ESTIMATEDCOMPLETIONDATE IS NULL AND SIM.ACTUALSTARTDATE IS NULL
                            THEN 'Estimated Completion Date, Actual Start Date'
                        WHEN SIM.ESTIMATEDCOMPLETIONDATE IS NULL AND SIM.ACTUALSTARTDATE IS NOT NULL
                            THEN 'Estimated Completion Date'
                        WHEN SIM.ACTUALSTARTDATE IS NULL AND SIM.ESTIMATEDCOMPLETIONDATE IS NOT NULL
                            THEN 'Actual Start Date'
                        ELSE 'No'
                        END                                                                           AS MISSING_DATES,
                    CASE WHEN SIM.TOTALEFFORTSPENT = 0 THEN 'Yes' ELSE 'No' END                    AS MISSING_EFFORT,
                    CASE
                        WHEN SIM.ROOTCAUSES IS NULL THEN 'Yes'
                        --WHEN SIM.ROOTCAUSES IS NOT NULL THEN 'No'
                        ELSE 'No'
                        END                                                                           AS MISSING_ROOT_CAUSE,

                    CASE WHEN SIM.CONVERSATIONCOUNT IS NULL THEN 'Yes'
                        ELSE 'No'
                    END AS MISSING_COMMENTS,
                    DATE_DIFF('DAY'::TEXT, TO_DATE(RESOLVEDDATE, 'YYYY-MM-DD'), GETDATE())
                        - DATE_DIFF('WEEK'::TEXT, TO_DATE(RESOLVEDDATE, 'YYYY-MM-DD'), GETDATE()) *  2  AS BUSINESS_DAYS_SINCE_RESOLVE
    FROM (SELECT S.ISSUEID,
                ---UPPER(FOLDER.FOLDER_NAME)                                    AS FOLDER_NAME,
                S.TITLE,
                S.ISSUEURL,
                S.ASSIGNEEIDENTITY,
                S.RESOLVEDDATE,
                S.LABEL_CUSTOMER,
                S.LABEL_PRODUCT,
                S.TOTALEFFORTSPENT,
                S.ESTIMATEDCOMPLETIONDATE,
                S.ACTUALSTARTDATE,
                S.ACTUALCOMPLETIONDATE,
                S.CONVERSATIONCOUNT,
                NVL(REGEXP_COUNT(LABELIDS, ',') + 1, 0)
                    - (case when LABEL_CUSTOMER is not null then 1 else 0 end
                    + case when LABEL_EXECUTION is not null then 1 else 0 end
                    + case when LABEL_PLANNING is not null then 1 else 0 end
                    + case when LABEL_PRODUCT is not null then 1 else 0 end) as SPRINT_LABEL_COUNT,
                S.ROOTCAUSES,
                CASE
                    WHEN TEAM.MANAGER = ALIAS THEN CONCAT(TEAM.SR_MANAGER,'@AMAZON.COM')
                    ELSE CONCAT(TEAM.MANAGER,'@AMAZON.COM')
                END AS MANAGER_EMAIL
        FROM EPA_BIMOE_PROD.O_SPRINT_PLANNING_SIM_DATA AS S

            LEFT JOIN EPA_BIMOE_PROD.O_SPRINT_PLANNING_TEAM_STRUCTURE AS TEAM
                ON TRIM(REPLACE(REPLACE(S.ASSIGNEEIDENTITY, 'kerberos:',''), '@ANT.AMAZON.COM', '')) = TEAM.ALIAS
            /*JOIN EPA_BIMOE_PROD.O_SPRINT_PLANNING_SIM_FOLDERS AS FOLDER
                        ON S.ASSIGNEDFOLDERID = FOLDER.ASSIGNEDFOLDERID
            AND UPPER(FOLDER.FOLDER_NAME) = 'BUG' --we only care about BUGS */
        WHERE S.ASSIGNEDFOLDERID = '76bbf190-f510-4a69-90db-4dc192b225fe'  -- BUGS folder ID
            AND S.STATUS = 'Resolved'
            AND S.LAST_COMMENT_AUTHOR <> 'fluxo:flx-shepherd' --not autoresolved by flx-shepherd
            AND TO_DATE(S.RESOLVEDDATE, 'YYYY-MM-DD') > '2022-10-01') AS SIM) t --start with tickets on this updated Sprint
            
WHERE (t.MISSING_LABELS <> 'No' OR t.MISSING_DATES <> 'No' OR t.MISSING_COMMENTS <> 'No'
   OR t.MISSING_EFFORT = 'Yes' OR t.MISSING_ROOT_CAUSE = 'Yes')
ORDER BY t.ASSIGNEE, t.BUSINESS_DAYS_SINCE_RESOLVE DESC
