package inputform;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Combine {

    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "combileFile");
        
        job.setJarByClass(Combine.class);

        job.setInputFormatClass(WholeFileInputFormMap.class);

        WholeFileInputFormMap.addInputPath(job, new Path(argv[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(argv[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(SmallFile2SequenceFile.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.waitForCompletion(true);
    }

    static class SmallFile2SequenceFile extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text fileName;

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileName, value);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fileSplit = (FileSplit) inputSplit;
            Path path = fileSplit.getPath();
            fileName = new Text(path.toString());
        }
    }
}
