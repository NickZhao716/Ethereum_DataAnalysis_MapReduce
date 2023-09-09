package ETHReceiverCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.UUID;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Map;


public class ETHReceiverReducer extends
        Reducer<Text, ETHTuple,Text, ETHTuple> {
    private ETHTuple res = new ETHTuple();
    public void reduce(Text key, Iterable<ETHTuple> value, Context context) throws IOException, InterruptedException {
        ETHTuple firstTuple = value.iterator().next();
        LocalDate date = firstTuple.getDate();
        BigInteger sum = new BigInteger(firstTuple.getVolume());
        BigInteger[] sumAndRemains;
        String fromAddress = firstTuple.getFromAddress();
        String toAddress = firstTuple.getToAddress();
        long txCount = firstTuple.getTotalReduceCount();
        String tupleType = firstTuple.getTupleType();

        for(ETHTuple val:value){
            sum = sum.add(new BigInteger(val.getVolume()));
            txCount+= val.getTotalReduceCount();
        }

        sumAndRemains = sum.divideAndRemainder(new BigInteger("1000000000000000000"));

        res.setDate(date);
        if (sumAndRemains[1].toString().length()>=5)
        {
            res.setVolume(sumAndRemains[0]+"."+sumAndRemains[1].toString().substring(0,4));
        }
        else
        {
            res.setVolume(sumAndRemains[0]+"."+sumAndRemains[1]);
        }

        res.setFromAddress(fromAddress);
        res.setToAddress(toAddress);
        res.setTotalReduceCount(txCount);
        res.setTupleType(tupleType);
        Text newKey = new Text(UUID.randomUUID().toString());
        context.write(newKey,res);
    }

}
