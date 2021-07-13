import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//public static class CustomInputFormat extends FileInputFormat{
//    public static class RecordReaderClass extends RecordReader<Text,IntWritable>{
//        LineRecordReader reader;
//
//        @Override
//        public void initialize(InputSplit split, TaskAttemptContext context)
//                throws IOException {
//            reader.initialize(split, context);
//        }
//
//        @Override
//        public void close() throws IOException {
//            reader.close();
//        }
//
//        @Override
//        public boolean nextKeyValue() throws IOException, InterruptedException {
//            return reader.nextKeyValue();
//        }
//
//        @Override
//        public Text getCurrentKey() throws IOException, InterruptedException {
//            return new Text(reader.getCurrentValue().toString().split(" ")[0]); //Trigram
//        }
//
//        @Override
//        public IntWritable getCurrentValue() throws IOException, InterruptedException {
//            return new IntWritable(Integer.parseInt(reader.getCurrentValue().toString().split(" ")[1]));
//        }
//
//        @Override
//        public float getProgress() throws IOException, InterruptedException {
//            return reader.getProgress();
//        }
//
//    }
//
//    @Override
//    public RecordReader<Text,IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        return new RecordReaderClass();
//    }
//}
public class Step2 {

    enum TotalOcc {
        TotalOcc_num
    }
    public static class TaggedValue implements Writable {
        private Text Tag;
        private IntWritable Value;
        public TaggedValue(){
            Tag = null;
            Value = null;
        }
        public TaggedValue(Text Tag,IntWritable Value){
            this.Tag=Tag;
            this.Value=Value;
        }
        public Text getTag() {
            return Tag;
        }

        public Writable getValue() {
            return Value;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(Tag.toString());
            dataOutput.writeInt(Value.get());

        }

        public void readFields(DataInput dataInput) throws IOException {
            Tag = new Text(dataInput.readUTF());
            Value = new IntWritable(dataInput.readInt());
        }


    }

    public static class MapperClass extends Mapper<LongWritable,Text,Text,TaggedValue> {
        // The map gets a key and tagged value (of 2 types) and emits the key and the value
        @Override
        public void map(LongWritable key, Text Value, Context context) throws IOException,  InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String str = fileName.substring(0, 1);
            String tagValue = str.equals("0")? "0":"1";
            String[] value = Value.toString().split("\t");
            context.getCounter(TotalOcc.TotalOcc_num).increment(Long.parseLong(value[1]));
            context.write(new Text(value[0]), new TaggedValue(new Text(tagValue),new IntWritable(Integer.parseInt(value[1]))));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,TaggedValue> {

        public int getPartition(Text key, TaggedValue value, int numPartitions) {
            return key.hashCode();
        }
    }

    public static class ReducerClass  extends Reducer<Text,TaggedValue,Text,Text> {
    @Override
    public void reduce(Text key,Iterable<TaggedValue> taggedValues, Context context) throws IOException, InterruptedException {
        TaggedValue val0 = taggedValues.iterator().next();
        String val2 = taggedValues.iterator().hasNext() ? taggedValues.iterator().next().getValue().toString() : "0";
        if (val0.getTag().toString().equals("0"))
            context.write(key,new Text(val0.getValue().toString()+" "+val2));// tr:r0 r1
        else{
            context.write(key,new Text(val2+" "+val0.getValue().toString()));
        }
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Combine r0 r1");

        FileSystem.setDefaultUri(conf, new URI(args[1]));
        //set format for input and output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //path to input and output files
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileSystem.get(conf).delete(new Path(args[2]),true);

        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.setJarByClass(Step2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaggedValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        if (job.waitForCompletion(true)){
            //Create a path
            Path hdfswritepath = new Path( "s3://input-file-hadoop/n");
            //Init output stream
            FSDataOutputStream outputStream=FileSystem.get(conf).create(hdfswritepath);
            outputStream.writeUTF(String.valueOf(job.getCounters().findCounter(TotalOcc.TotalOcc_num).getValue()));
            outputStream.close();
            System.exit(0);
        }
        System.exit(1);
    }
}