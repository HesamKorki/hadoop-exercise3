import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BetterInvertedIndex {

	public static class IdfMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
        private Text count = new Text();
		private Map<String, Integer> tokenCount = new HashMap<>();
		private final String pattern = "<doc id=\"([^\"]+)\"[^>]+>([^<]+)</doc>";
		private final Pattern regex = Pattern.compile(pattern);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			Matcher matcher = regex.matcher(line);

			if (!matcher.find()) {
				return;
			}
			String docId = matcher.group(1);
			String content = matcher.group(2);
			
			String[] tokens = content.split("[^\\w']+");
			for (String token : tokens) {
				if (tokenCount.containsKey(token)) {
					tokenCount.put(token, tokenCount.get(token) + 1);
				} else {
					tokenCount.put(token, 1);
				}
			}
			for (String token : tokenCount.keySet()) {
				word.set(token + "," + docId);
                count.set(String.valueOf(tokenCount.get(token)));
				context.write(word, count);
			}
		}
	}

	public static class TermPartitioner extends Partitioner<Text, IntWritable> {


		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			
			String term = key.toString().split("\\$")[0]; 

			return Math.abs(term.hashCode()) % numReduceTasks;
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();
        private String prevToken;
        private List<String> postingsList;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            prevToken = "";
            postingsList = new ArrayList<>();
        }
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
            String[] key_content = key.toString().split(",");
            String token = key_content[0];
            String docId = key_content[1];

            // If the token is different from the previous token, then emit the previous token's postings list
            // and reset the postings list
            if (!token.equals(prevToken)) {
                if (!prevToken.equals("")) {
                    result.set(String.join(", ", postingsList));
                    context.write(new Text(prevToken), result);
                    postingsList.clear();
                }
            }
			
			for (Text val : values) {
				postingsList.add(docId + ":" + val.toString());
			}
            prevToken = token;
		}

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the last token's postings list
            result.set(String.join(", ", postingsList));
            context.write(new Text(prevToken), result);

	    }
    }

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BetterInvertedIndex");
		job.setJar("BetterInvertedIndex.jar");

		job.setMapperClass(IdfMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setPartitionerClass(TermPartitioner.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    
}