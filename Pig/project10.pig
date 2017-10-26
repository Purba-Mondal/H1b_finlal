register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
h1b_data = LOAD 'hdfs://localhost:54310/user/hive/warehouse/project.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);


--Count Total Applications

temp= group h1b_data by $4;
total= foreach temp generate group,COUNT(h1b_data.$1);

--Count Total Applications who are 'CERTIFIED'

certified= filter h1b_data by $1 == 'CERTIFIED';
temp1= group certified by $4;
total_certified= foreach temp1 generate group,COUNT(certified.$1);

--Count Total Applications who are 'CERTIFIED-WITHDRAWN'

certified_withdrawn= filter h1b_data by $1 == 'CERTIFIED-WITHDRAWN';
temp2= group certified_withdrawn by $4;
total_certified_withdrawn= foreach temp2 generate group,COUNT(certified_withdrawn.$1); 

--SUCCESS_RATE=(CERTIFIED+CERTIFIED-WITHDRAWN)/TOTAL X 100

joined= join total_certified by $0,total_certified_withdrawn by $0,total by $0;
joined= foreach joined generate $0,$1,$3,$5;
intermediateoutput= foreach joined generate $0,(float)($1+$2)*100/($3),$3;
intermediateoutput2= filter intermediateoutput by $1>70 and $2>1000;	--Filter by success-rate greater than 70% and petition count above 1000
finaloutput= order intermediateoutput2 by $1 DESC;
--dump finaloutput
--STORE DATA INTO TEXT FILE
store finaloutput into '/home/hduser/output/pig/question10' using PigStorage('\t');
