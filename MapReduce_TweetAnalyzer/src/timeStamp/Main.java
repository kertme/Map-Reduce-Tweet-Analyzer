package timeStamp;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void runJob(String[] input, String output) throws Exception {


       /* if (args.length != 2) {
            System.err.println("Usage: <inputFilePath> <outputDir>");
            System.err.println(args.length);
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("csv.path", args[0]);
        conf.set("csv.delimiter", ",");
        //conf.set("top.athletesOnly", "true");
        //conf.set("top.number", "30");

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);
        job.setJarByClass(Main.class);
        job.setCombinerClass(TweetReducer.class);

        ChainMapper.addMapper(job, TweetMapper.class, Object.class, Text.class,
                Text.class, IntWritable.class, new Configuration(false));

        ChainReducer.setReducer(job, TweetReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, new Configuration(false));

        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.waitForCompletion(true);*/

        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);
        job.waitForCompletion(true);
        job.setNumReduceTasks(3);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
    }
}
