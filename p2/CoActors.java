import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;

public class CoActors {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text actorId = new Text();
        private Text titleId = new Text();
        private Map<String, String> titleTypeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try (BufferedReader br = new BufferedReader(new FileReader(context.getConfiguration().get("titleBasicsPath")))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        String tconst = parts[0].trim();
                        String titleType = parts[1].trim();
                        // Mapping each title to its type
                        titleTypeMap.put(tconst, titleType);
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                String category = parts[3].trim();

                if (category.equals("actor") || category.equals("actress")) {
                    String actorIdStr = parts[2].trim();
                    String titleIdStr = parts[0].trim();

                    if (titleTypeMap.containsKey(titleIdStr) && titleTypeMap.get(titleIdStr).equals("movie")) {
                        actorId.set(actorIdStr);
                        titleId.set(titleIdStr);
                        context.write(titleId, actorId);
                    }
                }
            }
        }
    }

    public static class MakePairsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> actors = new ArrayList<>();
    
            for (Text value : values) {
                actors.add(value.toString());
            }
    
            for (int i = 0; i < actors.size(); i++) {
                for (int j = i + 1; j < actors.size(); j++) {
                    String actor1 = actors.get(i);
                    String actor2 = actors.get(j);
    
                    String sortedPair = actor1.compareTo(actor2) < 0 ? actor1 + "\t" + actor2 : actor1 + "\t" + actor2;
                    context.write(new Text(sortedPair), new Text(""));
                }
            }
        }
    }

    public static class PairCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text actorPair = new Text();
    
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] actors = value.toString().trim().split("\t");
    
            if (actors.length == 2) {
                String actor1 = actors[0];
                String actor2 = actors[1];
                
                actorPair.set(actor1 + "\t" + actor2);

                context.write(actorPair, one);
            }
        }
    }

    public static class PairCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // sum up the values
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
    
            // set the sum as the result
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CoActors <inputPath> <titleBasicsPath> <outputPath>");
            System.exit(1);
        }

        // Job 1 - Create pairs of actors that participated together in a movie
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "CoActors-Job1");
        job1.getConfiguration().set("titleBasicsPath", args[1]);

        job1.setJarByClass(CoActors.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(MovieMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(MakePairsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path("/intermediate_output"));

        // Run the first job
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed");
            System.exit(1);
        }

        // Job 2 - Count the pairs
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "CoActors-Job2");

        job2.setJarByClass(CoActors.class);
        FileInputFormat.addInputPath(job2, new Path("/intermediate_output"));
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(PairCountMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setReducerClass(PairCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Run the second job
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}