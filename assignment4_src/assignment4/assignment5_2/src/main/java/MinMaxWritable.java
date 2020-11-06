import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.io.Writable;

public class MinMaxWritable implements Writable {
    private String symbol;
    private Date maxVolumeDate;
    private Date minVolumeDate;
    private Double maxClose;
    private int volume;
    public final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    MinMaxWritable(){
        this.maxVolumeDate=null;
        this.minVolumeDate=null;
        this.maxClose=0.0;
        this.volume=-1;
    }

    MinMaxWritable(MinMaxWritable stock){
        this.maxVolumeDate=new Date(stock.getMaxVolumeDate().getTime());
        this.minVolumeDate=new Date(stock.getMinVolumeDate().getTime());
        this.maxClose=stock.getMaxClose();
        this.volume=stock.getVolume();
    }

    MinMaxWritable(String inputLine) {

        String[] fields = inputLine.split(",");
        if (fields == null || fields.length != 9) {
            System.err.println("Bad data. The expected format is 9 columns seperated by comma.");
            return; // skip the bad data
        }

        try {
            this.symbol = fields[1];
            this.volume=Integer.parseInt(fields[7]);
            this.maxVolumeDate = dateFormatter.parse(fields[2]);
            this.minVolumeDate = dateFormatter.parse(fields[2]);
            this.maxClose = Double.parseDouble(fields[8]);
        } catch (Exception e) {
            System.err.println("Bad data. Cannot parse the symbol, date, volume and price in column 2, 3, 8 and 9.");
            e.printStackTrace();
            return;
        }
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Date getMaxVolumeDate() {
        return maxVolumeDate;
    }

    public void setMaxVolumeDate(Date maxVolumeDate) {
        this.maxVolumeDate = new Date(maxVolumeDate.getTime());
    }

    public Date getMinVolumeDate() {
        return minVolumeDate;
    }

    public void setMinVolumeDate(Date minVolumeDate) {
        this.minVolumeDate = new Date(minVolumeDate.getTime());
    }

    public Double getMaxClose() {
        return maxClose;
    }

    public void setMaxClose(Double maxClose) {
        this.maxClose = maxClose;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeLong(this.maxVolumeDate.getTime());
        out.writeLong(this.minVolumeDate.getTime());
        out.writeDouble(this.maxClose);
        out.writeInt(this.volume);


    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.maxVolumeDate = new Date(in.readLong());
        this.minVolumeDate = new Date(in.readLong());
        this.maxClose = in.readDouble();
        this.volume=in.readInt();
    }

    @Override
    public String toString() {
        return dateFormatter.format(maxVolumeDate) + "\t" + dateFormatter.format(minVolumeDate) + "\t" + maxClose;
    }
}
