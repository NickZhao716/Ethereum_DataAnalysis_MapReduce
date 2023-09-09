package ETHReceiverCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;

public class ETHReceiverCombiner extends
        Reducer<Text, ETHTuple,Text, ETHTuple> {
    private ETHTuple res = new ETHTuple();

    public void reduce(Text key, Iterable<ETHTuple> value, Context context) throws IOException, InterruptedException {
        ETHTuple firstTuple = value.iterator().next();
        LocalDate date = firstTuple.getDate();
        BigInteger sum = new BigInteger(firstTuple.getVolume());
        String fromAddress = firstTuple.getFromAddress();
        String toAddress = firstTuple.getToAddress();
        long txCount = firstTuple.getTotalReduceCount();
        String tupleType = firstTuple.getTupleType();

        for(ETHTuple val:value){
            sum = sum.add(new BigInteger(val.getVolume()));
            txCount+=val.getTotalReduceCount();
        }

        res.setDate(date);
        res.setVolume(sum.toString());
        res.setFromAddress(fromAddress);
        res.setToAddress(toAddress);
        res.setTotalReduceCount(txCount);
        res.setTupleType(tupleType);
        context.write(key,res);
    }
}
