package ETHReceiverCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

public class UniswapMapper extends
        Mapper<Object, Text, NullWritable, Text> {
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private static final String requestHeader = "https://api.coingecko.com/api/v3/coins/ethereum/history?date=";
    private static final DecimalFormat df = new DecimalFormat("0.00");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text info = new Text();
        String[] val = value.toString().split(",");
        val[0]=val[0].split("\t")[1];

        if(val.length!=4)
            return;

        if(!val[2].equals("0x7a250d5630b4cf539739df2c5dacb4c659f2488d"))
            return;


        for(int i =0;i<val.length;i++){
            byte[] delimiter = "\t".getBytes(StandardCharsets.UTF_8);
            byte[] addIn = val[i].getBytes(StandardCharsets.UTF_8);
            info.append(addIn,0,addIn.length);
            if(i!=val.length-1)
            {
                info.append(delimiter,0,delimiter.length);
            }
        }
        byte[] delimiter = "\t".getBytes(StandardCharsets.UTF_8);
        info.append(delimiter,0,delimiter.length);
        double volumeDouble = Double.parseDouble(val[0]);
        double currentPrice = getETHPrice(LocalDate.parse(val[3]));
        byte[] usd = df.format(volumeDouble * currentPrice).getBytes(StandardCharsets.UTF_8);
        info.append(usd,0,usd.length);
        byte[] uniswapS = "uniswap".getBytes(StandardCharsets.UTF_8);
        info.append(delimiter,0,delimiter.length);
        info.append(uniswapS,0,uniswapS.length);
        context.write(NullWritable.get(),info);
    }
    private double getETHPrice(LocalDate date) throws IOException {
        StringBuilder sb = new StringBuilder(requestHeader);
        sb.append(date.getDayOfMonth()).append("-").append(date.getMonthValue()).append("-").append(date.getYear());
        sb.append("&localization=false");
        HttpGet request = new HttpGet(sb.toString());
        Map jsonContent = null;
        try (CloseableHttpResponse response = httpClient.execute(request)) {

            // Get HttpResponse Status
            System.out.println(response.getStatusLine().toString());

            HttpEntity entity = response.getEntity();
            Header headers = entity.getContentType();
            if (entity != null) {
                // return it as a String
                String result = EntityUtils.toString(entity);
                jsonContent = (Map<String,String>) JSONValue.parse(result);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //httpClient.close();
        Map<String,Map> marketData = (Map<String, Map>) jsonContent.get("market_data");
        Map<String,Double> currentPrice = (Map<String, Double>) marketData.get("current_price");
        return currentPrice.get("usd");
    }
}
