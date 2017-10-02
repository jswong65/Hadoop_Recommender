import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outKey = new IntWritable();
		private Text outVal = new Text();
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input user,movie,rating
			//divide data by user
			String[] tmp = value.toString().trim().split(",");
			String userID = tmp[0];
			String movie = tmp[1];
			String rating = tmp[2];

			outKey.set(Integer.parseInt(userID));
			outVal.set(movie + "=" + rating);
			context.write(outKey, outVal);


		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		private Text outVal = new Text();
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//merge data for one user
			StringBuilder sb = new StringBuilder();
			for(Text value: values){
				sb.append(value.toString() + ",");
			}
			String str = sb.toString();
			// remove the last comma
			str = str.substring(0, str.length() - 1);
			outVal.set(str);

			context.write(key, outVal);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
