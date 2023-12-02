import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActorFrequency {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            // nConst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
            // 0    1           2          3          4                  5


            // Emit actor ID and count
            word.set(tokens[2]);
            context.write(word, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
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

    public static class ActorNameMapper extends Mapper<Object, Text, Text, Text> {

        private Text actorID = new Text();
        private Text actorName = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");

            // Emit actor ID and actor name
            actorID.set(tokens[0]);
            actorName.set(tokens[1]);
            context.write(actorID, actorName);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String actorName = null;
            int frequencySum = 0;

            for (Text val : values) {
                String value = val.toString();
                if (value.matches("\\d+")) {
                    // It's a frequency value
                    frequencySum += Integer.parseInt(value);
                } else {
                    // It's an actor name
                    actorName = value;
                }
            }

            if (actorName != null) {
                // Emit actor ID, frequency, and actor name
                result.set(frequencySum + "\t" + actorName);
                context.write(key, result);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "ActorFrequency");
        job1.setJar("ActorFrequency.jar");

        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp-output"));

        job1.waitForCompletion(true);

        // Second Job for Join
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "ActorJoin");
        job2.setJar("ActorFrequency.jar");

        job2.setMapperClass(ActorNameMapper.class);
        job2.setReducerClass(JoinReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input is name.basics.tsv
        FileInputFormat.addInputPath(job2, new Path("temp-output")); // Input is the output of the first job
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Output path for the final result

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
