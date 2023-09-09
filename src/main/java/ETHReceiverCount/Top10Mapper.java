package ETHReceiverCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.UUID;


public class Top10Mapper extends
        Mapper<Object, Text, NullWritable, Text> {
    private TreeMap<BigInteger,Text> topRes = new TreeMap<BigInteger,Text>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text info = new Text();
        String[] val = value.toString().split(",");
        val[0]=val[0].split("\t")[1];

        if(val.length!=4)
            return;


        for(int i =0;i<val.length;i++){
            byte[] delimiter = ",".getBytes(StandardCharsets.UTF_8);
            byte[] addIn = val[i].getBytes(StandardCharsets.UTF_8);
            info.append(addIn,0,addIn.length);
            if(i!=val.length-1)
            {
                info.append(delimiter,0,delimiter.length);
            }
        }

        topRes.put(new BigInteger(val[1]),info);
        if(topRes.size()>200)
            topRes.remove(topRes.firstKey());
    }

    protected void cleanup(Context context) throws IOException,InterruptedException{
        for(Text t: topRes.values()){
            context.write(NullWritable.get(),t);
        }
    }
}
