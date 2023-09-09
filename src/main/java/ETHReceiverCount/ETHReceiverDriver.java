package ETHReceiverCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETHReceiverDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        conf.set("fs.defaultFS", "hdfs://192.168.1.20:9000");

        Job job = Job.getInstance(conf, "ETHReceiver");
        job.setJarByClass(ETHReceiverDriver.class);
        if(args[2].equals("1"))
        {
            job.setMapperClass(ETHReceiverMapper.class);
            job.setCombinerClass(ETHReceiverCombiner.class);
            job.setReducerClass(ETHReceiverReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(ETHTuple.class);
        }
        else if (args[2].equals("2"))
        {
            job.setMapperClass(Top10Mapper.class);
            job.setReducerClass(Top10Reducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            //job.setInputFormatClass(KeyValueTextInputFormat.class);
        }
        else
        {
            job.setMapperClass(UniswapMapper.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem fs = FileSystem.get(conf);

        Path out;
        if(args[2].equals("1"))
            out = new Path("/ETHReceiver");
        else if(args[2].equals("2"))
            out = new Path("/Top10Transfer");
        else
            out = new Path("/uniswapETH");

        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        long start = System.nanoTime();
        int exit = job.waitForCompletion(true) ? 0 : 1;
        long end = System.nanoTime();
        System.out.println("Elapsed Time in nano seconds: "+ (end-start));
        System.exit(exit);
    }
}
