import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,IntWritable>{
        public void map(LongWritable lineId,Text line, Mapper.Context context) throws IOException,  InterruptedException {
            String[] value = line.toString().split("\t");
            if (value.length >= 2)
                context.write(new Text(value[0]), new IntWritable(Integer.parseInt(value[2])));
        }

    }

    public static class ReducerClass  extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> Values, Context context) throws IOException, InterruptedException {
            int sum =0;
            while(Values.iterator().hasNext()){
                sum = sum + Values.iterator().next().get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Trigram count");

        FileSystem.setDefaultUri(conf, new URI(args[1]));
        //set format for input and output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().set("mapreduce.output.basename",args[0].substring(args[0].length()-1));
        //path to input and output files
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]),true);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setJarByClass(Step1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

