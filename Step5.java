
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step5 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text Value, Context context) throws IOException, InterruptedException {
            String[] value = Value.toString().split("\t");
            String[] str = value[1].split("\\s+");
            context.write(new Text(value[0]), new Text(str[0] + " " + str[1] + " " + str[2]
                    + " " + str[3] + " " + str[4] + " " + str[5]));// tr: r0 r1 tr01 nr0 tr10 nr1)
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode();
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = Values.iterator();
            double result = 0;
            long n = context.getConfiguration().getLong("n",0);
            while(it.hasNext()){
                String[] str = it.next().toString().split(" ");
                double tr01 = Double.parseDouble(str[2]);
                double nr0 = Double.parseDouble(str[3]);
                double tr10 = Double.parseDouble(str[4]);
                double nr1 = Double.parseDouble(str[5]);
                result = (tr01+tr10)/(n);
                result = result * (1/(nr0+nr1));
            }
            context.write(key,new DoubleWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "final");
        
        FileSystem fs = FileSystem.get(new URI("s3://output-hadoop-test/n"), conf);
        Path filePath = new Path("s3://output-hadoop-test/n");
        FSDataInputStream fsDataInputStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        long n = (long)Double.parseDouble(br.readLine());
        job.getConfiguration().setLong("n",n);

        FileSystem.setDefaultUri(conf, new URI(args[1]));
        //set format for input and output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //path to input and output files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(Step5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
