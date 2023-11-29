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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class IdfMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text posting = new Text();
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
				word.set(token);
				posting.set(docId + ":" + tokenCount.get(token));

				context.write(word, posting);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				
			List<String> postingsList = new ArrayList<>();
			for (Text posting : values) {
				postingsList.add(posting.toString());
			}
			// Custom comparator for sorting based on ID
			Comparator<String> idComparator = new Comparator<String>() {
    			@Override
    			public int compare(String s1, String s2) {
        			// Extract IDs from strings (assuming "id:count" format)
					String id1 = s1.split(":")[0];
					String id2 = s2.split(":")[0];
					// Compare IDs as strings
					return id1.compareTo(id2);
    			}
			};
			// Sort postings list
			Collections.sort(postingsList, idComparator);
			result.set(String.join(", ", postingsList));
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJar("InvertedIndex.jar");

		job.setMapperClass(IdfMapper.class);
		job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}