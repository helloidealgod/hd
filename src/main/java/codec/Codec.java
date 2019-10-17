package codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Codec {
    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.map.output.compress", "true");
        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        configuration.set("mapreduce.output.fileoutputfromat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        Job job = Job.getInstance(configuration, "codec");
        job.setJarByClass(Codec.class);
        FileInputFormat.setInputPaths(job, new Path(argv[0]));
        FileOutputFormat.setOutputPath(job, new Path(argv[1]));

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
