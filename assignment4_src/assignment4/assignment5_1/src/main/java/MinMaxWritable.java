import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.io.Writable;

public class MinMaxWritable implements Writable {
    private String symbol;
    private int maxVolume;
    private int minVolume;
    private Double maxClose;

    MinMaxWritable(){
        this.maxVolume=-1;
        this.minVolume=-1;
        this.maxClose=0.0;
    }

    MinMaxWritable(MinMaxWritable stock){
        this.maxVolume=stock.getMaxVolume();
        this.minVolume= stock.getMinVolume();
        this.maxClose=stock.getMaxClose();
    }

    MinMaxWritable(String inputLine) {

        String[] fields = inputLine.split(",");
        if (fields == null || fields.length != 9) {
            System.err.println("Bad data. The expected format is 9 columns seperated by comma.");
            return; // skip the bad data
        }

        try {
            this.symbol = fields[1];
            this.maxVolume=Integer.parseInt(fields[7]);
            this.minVolume=Integer.parseInt(fields[7]);
            this.maxClose = Double.parseDouble(fields[8]);
        } catch (Exception e) {
            System.err.println("Bad data. Cannot parse the symbol, date, volume and price in column 2, 3, 8 and 9.");
            e.printStackTrace();
            return;
        }
    }


    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int getMaxVolume() {
        return maxVolume;
    }

    public void setMaxVolume(int maxVolume) {
        this.maxVolume = maxVolume;
    }

    public int getMinVolume() {
        return minVolume;
    }

    public void setMinVolume(int minVolume) {
        this.minVolume = minVolume;
    }

    public Double getMaxClose() {
        return maxClose;
    }

    public void setMaxClose(Double maxClose) {
        this.maxClose = maxClose;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(this.maxVolume);
        out.writeInt(this.minVolume);
        out.writeDouble(this.maxClose);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.maxVolume = in.readInt();
        this.minVolume = in.readInt();
        this.maxClose = in.readDouble();
    }

    @Override
    public String toString() {
        return maxVolume+","+minVolume+","+maxClose;
    }
}
