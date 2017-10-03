import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		Text outKey = new Text();
		Text outVal = new Text();
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation

			//pass data to reducer
			String [] movieB_relation = value.toString().trim().split("\t");
			outKey.set(movieB_relation[0]);
			outVal.set(movieB_relation[1]);
			context.write(outKey, outVal);
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		Text outKey = new Text();
		Text outVal = new Text();
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			//pass data to reducer

			String [] user_movie_rating = value.toString().trim().split(",");
			String user = user_movie_rating[0];
			String movie = user_movie_rating[1];
			String rating = user_movie_rating[2];

			outKey.set(movie);
			outVal.set(user + ":" + rating);
			context.write(outKey, outVal);
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Text outKey = new Text();
			DoubleWritable outVal = new DoubleWritable();

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication

			Map<String, Double> coocurrenceMap = new HashMap<String, Double>();
			Map<String, Double> ratingMap = new HashMap<String, Double>();

			for (Text val: values){
				if(val.toString().contains("=")){
					String [] movie_relation = val.toString().trim().split("=");
					coocurrenceMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}else{
					String [] user_rating = val.toString().trim().split(":");
					ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}
			String movieId, userId;
			Double relation, rating;
			for (Map.Entry<String, Double> entry: coocurrenceMap.entrySet()){

				movieId = entry.getKey();
				relation = entry.getValue();
				for (Map.Entry<String, Double> element: ratingMap.entrySet()) {

					userId = element.getKey();
					rating = element.getValue();

					outKey.set(userId + ":" + movieId);
					outVal.set(rating * relation);

					context.write(outKey, outVal);
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
