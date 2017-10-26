import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Project_2a {
	//mapper........
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {	
	      public void map(LongWritable key, Text value, Context context)
	      {	  LongWritable one=new LongWritable(1);
	    	  try{
		            String[] str = value.toString().split("\t");
		            if(str[4]!=null && str[4].contains("DATA ENGINEER"))
		    		{	Text answer= new Text(str[8]+"\t"+str[7]);
		    	  		context.write(answer,one);
		    		}
	    	  }
	    	  catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
	      }
}
	
	public static class CodePartition extends
	Partitioner < Text, LongWritable > {
	    @Override
	    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
	        String[] str = key.toString().split("\t");
	        if (str[1].equals("2011"))
	            return 0;
	        if (str[1].equals("2012"))
	            return 1;
	        if (str[1].equals("2013"))
	            return 2;
	        if (str[1].equals("2014"))
	            return 3;
	        if (str[1].equals("2015"))
	            return 4;
	        if (str[1].equals("2016"))
	            return 5;
	        else
	            return 6;
	    }
	}

//reducer............
	public static class ReducerClass extends Reducer<Text,LongWritable,NullWritable,Text>
	{
		private TreeMap<LongWritable, Text> Top5DataEngineer = new TreeMap<LongWritable, Text>();
		long sum=0;
		public void reduce(Text key,Iterable <LongWritable> values,Context context) throws IOException, InterruptedException
		{
			sum=0;
			for(LongWritable val:values)
			{
			sum+=val.get();
			}
			Top5DataEngineer.put(new LongWritable(sum),new Text(key+","+sum));
			if (Top5DataEngineer.size()>1)
				Top5DataEngineer.remove(Top5DataEngineer.firstKey());
		}
		protected void cleanup(Context context)throws IOException, InterruptedException
		{
			for (Text t : Top5DataEngineer.descendingMap().values()) 
				context.write(NullWritable.get(), t);
		}				
	} 
    //main class...........
    public static void main(String args[])  throws IOException, InterruptedException, ClassNotFoundException
	{
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "Top  5 Data Engineer in a worksite");

		  job.setJarByClass(Project_2a.class);
		  job.setMapperClass(MapClass.class);
		  job.setPartitionerClass(CodePartition.class);
		  job.setReducerClass(ReducerClass.class);
		  	  
		  job.setNumReduceTasks(7);

		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(LongWritable.class);
		    
		  job.setOutputKeyClass(NullWritable.class);
		  job.setOutputValueClass(Text.class);
		  
		  //job.setInputFormatClass(TextInputFormat.class);	
	      //job.setOutputFormatClass(TextOutputFormat.class);
	      
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

	  }

}
