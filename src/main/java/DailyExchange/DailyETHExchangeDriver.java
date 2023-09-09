package DailyExchange;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DailyETHExchangeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.20:9000");
        Job job = Job.getInstance(conf, "ETHExchange");
        job.setJarByClass(DailyETHExchangeDriver.class);
        job.setMapperClass(DailyETHExchangeMapper.class);
        job.setCombinerClass(DailyETHExchangeCombiner.class);
        job.setReducerClass(DailyETHExchangeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ETHTuple.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("/ETHExchange");
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
