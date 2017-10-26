use project;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/output/hive/question_5a' row format delimited fields terminated by ','
select job_title,year,count(*) as temp from h1b_final where year= ${hiveconf:var}  group by job_title,year  order by temp desc limit 10;

 
