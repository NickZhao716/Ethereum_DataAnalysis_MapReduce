package ETHReceiverCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

public class ETHTuple implements Writable {
    final private LocalDate firstDate = LocalDate.parse("2015-07-30");
    private LocalDate date;
    private String volume = "0";
    private String fromAddress;
    private String toAddress;
    private long totalReduceCount;

    private String tupleType;


    public void setDate(int days) {
        date = firstDate.plusDays(days);
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public LocalDate getDate(){
        return date;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getVolume() {
        return volume;
    }



    public String getFromAddress() {
        return fromAddress;
    }

    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    public String getToAddress() {
        return toAddress;
    }

    public void setToAddress(String toAddress) {
        this.toAddress = toAddress;
    }

    public long getTotalReduceCount() {
        return totalReduceCount;
    }

    public void setTotalReduceCount(long totalReduceCount) {
        this.totalReduceCount = totalReduceCount;
    }

    public String getTupleType() {
        return tupleType;
    }

    public void setTupleType(String tupleType) {
        this.tupleType = tupleType;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date.toString());
        dataOutput.writeUTF(volume);
        dataOutput.writeUTF(fromAddress);
        dataOutput.writeUTF(toAddress);
        dataOutput.writeLong(totalReduceCount);
        dataOutput.writeUTF(tupleType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date = LocalDate.parse(dataInput.readUTF());
        volume = dataInput.readUTF();
        fromAddress = dataInput.readUTF();
        toAddress = dataInput.readUTF();
        totalReduceCount = dataInput.readLong();
        tupleType = dataInput.readUTF();
    }

    public String toString(){
        if(tupleType.equals("ETH-Exchange-Meta"))
        {
            //return "reduced_transaction: "+totalReduceCount+" Volume: "+volume;
            return volume+","+totalReduceCount+","+toAddress+","+date;
        }
        return null;
    }
}
