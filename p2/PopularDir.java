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

public class PopularDir {

    public static class DirCounter extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text directorId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                String category = parts[3].trim();
                if (category.equals("director")) {
                    String dirId = parts[2].trim();
                    directorId.set(dirId);
                    context.write(directorId, one);
                }
            }
        }
    }

    public static class DirIdReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class DirectorJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text directorId = new Text();
        private Text directorName = new Text();
        private Text directorCount = new Text();
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            // Check if it's the name.basics.tsv file
            if (parts.length != 2) {
                String id = parts[0].trim();
                String name = parts[1].trim();
                directorId.set(id);
                // Add '$' prefix to separate count from director name
                directorName.set("$" + name);   
                context.write(directorId, directorName);
            }
            // If it's the output of the first reducer we take the count
            else {
                String id = parts[0].trim();
                String count = parts[1].trim();
                directorId.set(id);
                // No '$' prefix to separate count from director name 
                directorCount.set(count);
                context.write(directorId, directorCount);
            }
        }
    }

    public static class DirectorJoinReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
    
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String directorName = null;
            int count = 0;
    
            for (Text val : values) {
                String valueStr = val.toString();
                if (valueStr.startsWith("$")) {
                    // Remove the '$' prefix
                    directorName = valueStr.substring(1); 
                } 
                else {
                     count += Integer.parseInt(valueStr);
                }
            }
    
            if (directorName != null) {
                result.set(directorName + "\t" + count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "PopularDir-Job1");
        job1.setJarByClass(PopularDir.class);
        job1.setMapperClass(DirCounter.class);
        job1.setCombinerClass(DirIdReducer.class);
        job1.setReducerClass(DirIdReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2])); 

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "PopularDir-Job2");
        job2.setJarByClass(PopularDir.class);
        job2.setMapperClass(DirectorJoinMapper.class);
        job2.setReducerClass(DirectorJoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job2, args[1] + "," + args[2]);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}