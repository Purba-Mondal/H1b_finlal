register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
h1b_data = LOAD 'hdfs://localhost:54310/user/hive/warehouse/project.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);

cleansed= filter h1b_data by $7=='2011';
a= group cleansed by $4;
step_a= foreach a generate group,COUNT($1);
describe step_a;
cleansed1= filter h1b_data  by $7=='2012';
b= group cleansed1 by $4;
step_b= foreach b generate group,COUNT($1);
describe step_b;
cleansed2= filter h1b_data  by $7=='2013';
c= group cleansed2 by $4;
step_c= foreach c generate group,COUNT($1);
describe step_c;
cleansed3= filter h1b_data  by $7=='2014';
d= group cleansed3 by $4;
step_d= foreach d generate group,COUNT($1);
describe step_d;
cleansed4= filter h1b_data  by $7=='2015';
e= group cleansed4 by $4;
step_e= foreach e generate group,COUNT($1);
describe step_e;
f= group cleansed5 by $4;
step_f= foreach f generate group,COUNT($1);
describe step_f;
joined= join step_a by $0,step_b by $0,step_c by $0,step_d by $0,step_e by $0,step_f by $0;
describe joined;
yearwiseapplications= foreach joined generate $0,$1,$3,$5,$7,$9,$11;
progressivegrowth= foreach yearwiseapplications  generate $0,ROUND_TO((long)($6-$5)*100/$5,2),ROUND_TO((long)($5-$4)*100/$4,2),ROUND_TO((long)($4-$3)*100/$3,2),ROUND_TO((long)($3-$2)*100/$2,2),ROUND_TO((long)($2-$1)*100/$1,2);
avgprogressivegrowth= foreach progressivegrowth generate $0,($1+$2+$3+$4+$5)/5;
orderedavggrowth= order avgprogressivegrowth by $1 desc;
answer = limit orderedavggrowth  5;
dump answer;
--STORE DATA INTO TEXT FILE
store finaloutput into '/home/hduser/output/pig/question_1b' using PigStorage('\t');
