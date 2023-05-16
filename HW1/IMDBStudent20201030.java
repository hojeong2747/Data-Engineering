import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20201030 
{

	public static class IMDBStudent20201030Mapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable result = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split("::");
			int len = tokens.length;

			StringTokenizer itr = new StringTokenizer(tokens[len-1], "|");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, result);
			}

		}

	}

	public static class IMDBStudent20201030Reducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2)
		{
			System.err.println("Usage: IMDBBStudent20201030");
			System.exit(2);
		}


		Job job = new Job(conf, "IMDBStudent20201030");
		job.setJarByClass(IMDBStudent20201030.class);
		job.setMapperClass(IMDBStudent20201030Mapper.class);
		job.setReducerClass(IMDBStudent20201030Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
