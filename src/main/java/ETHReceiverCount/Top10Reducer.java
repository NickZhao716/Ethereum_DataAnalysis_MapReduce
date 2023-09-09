package ETHReceiverCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.TreeMap;
import java.util.UUID;

public class Top10Reducer extends
        Reducer<NullWritable, Text,NullWritable, Text> {
    private TreeMap<Double,Text> res= new TreeMap<Double,Text>();

    public void reduce(NullWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        for(Text val:value){
            Text tmp = new Text(val.toString());
            String[] valString = val.toString().split(",");
            res.put(Double.parseDouble(valString[0]),tmp);
            if(res.size()>200)
                res.remove(res.firstKey());
        }



        for(Text t:res.descendingMap().values()){
            context.write(NullWritable.get(),t);
        }


    }
}
