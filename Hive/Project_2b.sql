use project;
select worksite,count(case_status) as total,year from h1b_final where year = ${hiveconf:var}and case_status='CERTIFIED' group by worksite,year order by total desc limit 5;
