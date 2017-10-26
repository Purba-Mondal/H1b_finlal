import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Project3 {
	//mapper........
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {	
	      public void map(LongWritable key, Text value, Context context)
	      {	  LongWritable one=new LongWritable(1);
	    	  try{
		            String[] str = value.toString().split("\t");
		            if (str[4].contains("DATA SCIENTIST")) {
		                Text answer = new Text(str[3]);
		                context.write(answer, one);
		            	}
	    	     }
	    	  catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
	      }
	   }
	//reducer part.........
	public static class ReducerClass extends Reducer < Text, LongWritable, NullWritable, Text > {
    private TreeMap < LongWritable,Text > DataScientistJobs = new TreeMap < LongWritable,Text > ();

    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
    InterruptedException {

        long sum = 0;
        for (LongWritable val: values)
            sum += val.get();

        DataScientistJobs.put(new LongWritable(sum), new Text(key.toString().replaceAll("\"", "") + "," + sum));
        if (DataScientistJobs.size() > 5)
            DataScientistJobs.remove(DataScientistJobs.firstKey());
    }

    protected void cleanup(Context context) throws IOException,
    InterruptedException {
        for (Text t: DataScientistJobs.descendingMap().values())
            context.write(NullWritable.get(), t);

    }

}
	public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Scientist jobs");
        
        job.setJarByClass(Project3.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }

}
