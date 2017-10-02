import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outVal = new Text();
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //movieA:movieB \t count
            //collect the count list for movieA
            String[] tmp = value.toString().trim().split("\t");
            String count = tmp[1];
            String movieA = tmp[0].split(":")[0];
            String movieB = tmp[0].split(":")[1];

            outKey.set(movieA);
            outVal.set(movieB + "=" + count);
            context.write(outKey, outVal);
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outVal = new Text();
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA, value=<movieB:count, movieC:count...>
            //normalize each unit of co-occurrence matrix
            //outKey = movieB
            //outVal = movieA=normal_score
            Map<String, Integer> movie_count_map = new HashMap<String, Integer>();
            String movie, count;
            double normal_score;
            int totalCount = 0;
            for(Text val: values){
                movie = val.toString().split("=")[0];
                count = val.toString().split("=")[1];
                totalCount += Integer.parseInt(count);
                movie_count_map.put(movie, Integer.parseInt(count));
            }

            Iterator it = movie_count_map.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
                normal_score = (double)pair.getValue() / totalCount;
                outKey.set(pair.getKey());
                outVal.set(key.toString() + "=" + normal_score);
                context.write(outKey, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
