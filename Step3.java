
import java.io.IOException;
import java.net.URI;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3 {


    public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text Value, Context context) throws IOException, InterruptedException {
            String[] value = Value.toString().split("\t");
            String[] r_values = value[1].split("\\s+");
            IntWritable r0 = new IntWritable(Integer.parseInt(r_values[0]));
            context.write(r0, new Text(value[0] + "~" + r_values[1]));// r0 (tr ~ r1)
        }
    }

    public static class PartitionerClass extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get();
        }
    }


    public static class ReducerClass extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it1 = Values.iterator();// r0 (tr~r1 '\t' tr~r1)
            ArrayList<String> list = new ArrayList<>();
            int sum = 0;
            int counter = 0;
            while (it1.hasNext()) {
                String[] combined_values_list = it1.next().toString().split("\t");
                for(int i = 0; i < combined_values_list.length; i++){
                    String[] tuple = combined_values_list[i].split("~");
                    sum += Integer.parseInt(tuple[1]);
                    list.add(tuple[0] + "\t" + key.get() + " " + tuple[1] + " "); //tr \t r0 r1
                    counter++;
                }
            }
//            str = str.substring(0, str.length() - 1);
            for(String trigram_r0 : list){
                String[] key_val = trigram_r0.split("\t");
                context.write(new Text(key_val[0]) ,new Text(key_val[1] + sum +" "+counter));//tr: r0 r1 [r1,r1,...]
            }
        }
    }



        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "nr0");

            FileSystem.setDefaultUri(conf, new URI(args[1]));
            //set format for input and output
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            //path to input and output files
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileSystem.get(conf).delete(new Path(args[1]), true);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setJarByClass(Step3.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(MapperClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setPartitionerClass(PartitionerClass.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
