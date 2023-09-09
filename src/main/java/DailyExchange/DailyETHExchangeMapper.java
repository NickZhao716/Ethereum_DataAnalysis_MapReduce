package DailyExchange;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class DailyETHExchangeMapper extends
        Mapper<Object, Text, Text, ETHTuple> {
    private Text outKey = new Text();
    private ETHTuple ETHTuple = new ETHTuple();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split(",");
        int days =0;
        if(val[0].equals("hash")||val.length!=15)
            return;

        days = (int) (Long.parseLong(val[3])/6400);
        if(Long.parseLong(val[3])%64000>0)
        {
            days+=1;
        }

        ETHTuple.setDate(days);
        ETHTuple.setVolume(val[7]);

        ETHTuple.setFromAddress(val[5]);

        ETHTuple.setToAddress(val[6]);
        ETHTuple.setTupleType("ETH-Exchange-Meta");

        outKey.set(ETHTuple.getDate().toString());
        context.write(outKey, ETHTuple);
    }

}
