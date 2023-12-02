import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class SearchEngine {

	public static class SearchMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
			String[] keywords = conf.get("search.keywords", "").split(",");

            // Parse the inverted index files
            String line = value.toString();
            String[] line_content = line.split("\\t");
            String token = line_content[0];
            String postings = line_content[1];
            String[] posting_list = postings.split(", ");

            // Check if the token is in the search keywords
            boolean isTokenSearched = false;
            for (String keyword : keywords) {
                if (token.equals(keyword)) {
                    isTokenSearched = true;
                    break;
                }
            }

            // If the token is in the search keywords, then emit each posting
            if (isTokenSearched) {
                for (String posting : posting_list) {
                    String[] posting_content = posting.split(":");
                    String docId = posting_content[0];
                    String count = posting_content[1];
                    context.write(new Text(docId), new IntWritable(Integer.parseInt(count)));
                }
            }
		}
	
    }

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SearchEngine");
		job.setJar("SearchEngine.jar");

		job.setMapperClass(SearchMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        // The 3rd argument is the search keyword flag followed by as many keywords as you want
        if ("-search.keywords".equals(args[2])) {
            StringBuilder keywords = new StringBuilder();
            for (int i = 3; i < args.length; i++) {
                keywords.append(args[i]);
                if (i != args.length - 1) {
                    keywords.append(",");
                }
            }
            System.out.println("Search keywords: " + keywords.toString());
            job.getConfiguration().set("search.keywords", keywords.toString());
        }


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}