import java.math.BigInteger;

public class test {
    public static void main(String[] args) {
        BigInteger sum = new BigInteger("15888854456555644123");
        sum = sum.add(new BigInteger("111223345648"));
        sum = sum.add(new BigInteger("111223345648"));
        sum = sum.add(new BigInteger("111223345648"));
        sum = sum.add(new BigInteger("111223345648"));
        BigInteger[] a = sum.divideAndRemainder(new BigInteger("1000000000000000000"));
    }
}
