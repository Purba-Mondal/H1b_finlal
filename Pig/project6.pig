register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
h1b_data = LOAD 'hdfs://localhost:54310/user/hive/warehouse/project.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);

year_group= group h1b_data by $7;

total1= foreach year_group generate group,COUNT(h1b_data.$1);

--dump total1;

year_case_group= group h1b_data by ($7,$1);
total2= foreach year_case_group generate group,group.$0,COUNT($1);
--dump total2;

joined= join total2 by $1,total1 by $0;
ans= foreach joined generate FLATTEN($0),(float)($2*100)/$4,$2; --percent generation
--dump ans;
--STORE DATA INTO TEXT FILE
store finaloutput into '/home/hduser/output/pig/question6' using PigStorage('\t');
