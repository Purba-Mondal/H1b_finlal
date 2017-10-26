import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Project_1a {
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {	
	      public void map(LongWritable key, Text value, Context context)
	      {	  LongWritable one=new LongWritable(1);
	    	  try{
		            String[] str = value.toString().split("\t");
		            if(str[4]!=null && str[4].contains("DATA ENGINEER"))
		    		{	Text answer= new Text(str[7]);
		    	  		context.write(answer,one);
		    		}
	    	  }
	    	  catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
	      }
}
	public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		private LongWritable result = new LongWritable();
	    long total_sum=0;
	    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	      long sum = 0;
	  
	         for (LongWritable val : values)
	         {       	
	        	sum += val.get();
	         }
	         
	      result.set(sum);
	      if((sum-total_sum)>0)
	      {
	      context.write(new Text(key+" "+"increasing"), result);
	      }
	      else
	      {
	    	  context.write(new Text(key+" "+"decreasing"), result);  
	      }
	      total_sum=sum;
	   }
}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = Job.getInstance(conf, "total Count");
	    job.setJarByClass(Project_1a.class);
	    job.setMapperClass(MapClass.class);
	    job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}