import java.io.IOException;
import java.util.Calendar;
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

public class UBERStudent20201030 
{

	public static class UBERStudent20201030Mapper extends Mapper<Object, Text, Text, Text>
	{
		private Text key_word = new Text();
		private Text value_word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			if (itr.countTokens() >= 4) {
				String baseNumber = itr.nextToken().trim();
				String date = itr.nextToken().trim();
				String activeVehicles = itr.nextToken().trim();
				String trips = itr.nextToken().trim();

				String dayOfWeek = getDayOfWeek(date);
				String keyString = baseNumber + "," + dayOfWeek;
				String valueString = trips + "," + activeVehicles;

				key_word.set(keyString);
				value_word.set(valueString);
				context.write(key_word, value_word);
			}
		}

		private String getDayOfWeek(String date) {
			String[] dateParts = date.split("/");
			int month = Integer.parseInt(dateParts[0]);
			int day = Integer.parseInt(dateParts[1]);
			int year = Integer.parseInt(dateParts[2]);

			Calendar calendar = Calendar.getInstance();
			calendar.set(year, month-1, day);

			int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

			switch(dayOfWeek) {
				case Calendar.SUNDAY:
					return "SUN";
				case Calendar.MONDAY:
					return "MON";
				case Calendar.TUESDAY:
					return "TUE";
				case Calendar.WEDNESDAY:
					return "WED";
				case Calendar.THURSDAY:
					return "THU";
				case Calendar.FRIDAY:
					return "FRI";
				case Calendar.SATURDAY:
					return "SAT";
				default:
					return "";
			}
		}
	}

	public static class UBERStudent20201030Reducer extends Reducer<Text, Text, Text, Text> 
	{

		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException 
		{
			context.write(key, value);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2)
		{
			System.err.println("Usage: UBERStudent20201030");
			System.exit(2);
		}


		Job job = new Job(conf, "UBER20201030");
		job.setJarByClass(UBERStudent20201030.class);
		job.setMapperClass(UBERStudent20201030Mapper.class);
		job.setReducerClass(UBERStudent20201030Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
