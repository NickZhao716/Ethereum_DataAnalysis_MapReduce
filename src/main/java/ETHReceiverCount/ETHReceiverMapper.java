package ETHReceiverCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Map;


public class ETHReceiverMapper extends
        Mapper<Object, Text, Text, ETHTuple> {
    private Text outKey = new Text();
    private ETHTuple ETHTuple = new ETHTuple();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split(",");
        int days =0;
        if(val[0].equals("hash")||val.length!=15||val[7].equals("0")||!val[7].equals("0x7a250d5630b4cf539739df2c5dacb4c659f2488d"))
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
        ETHTuple.setTotalReduceCount(1);
        ETHTuple.setTupleType("ETH-Exchange-Meta");

        outKey.set(val[6]+"\t"+ETHTuple.getDate().toString());
        context.write(outKey, ETHTuple);
    }



}
