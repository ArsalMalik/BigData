   import java.io.DataInput;
   import java.io.DataOutput;
   import java.io.IOException;
   import org.apache.hadoop.io.Writable;
   
   public class MMM
     implements Writable
   {
     Integer minimum;
     Integer maximum;
     Double median;
   
     public MMM()
     {
       this.minimum = 0;
       this.maximum = 0;
       this.median = 0.0D;
     }
   
     public void write(DataOutput out) throws IOException {
       out.writeInt(this.minimum);
       out.writeInt(this.maximum);
       out.writeDouble(this.median);
     }
   
     public void readFields(DataInput in) throws IOException {
       this.minimum = new Integer(in.readInt());
       this.maximum = new Integer(in.readInt());
       this.median = new Double(in.readDouble());
     }
   
     public String toString()
     {
       return "Minimum = " + this.minimum + "\tMaximum = " + this.maximum + "\tMedian = " + this.median;
     }
   
     public Integer getMinimum() {
       return this.minimum;
     }
   
     public Integer getMaximum() {
       return this.maximum;
     }
   
     public Double getMedian() {
       return this.median;
     }
   
     public void setMinimum(Integer minimum) {
       this.minimum = minimum;
     }
   
     public void setMaximum(Integer maximum) {
       this.maximum = maximum;
     }
   
     public void setMedian(Double median) {
       this.median = median;
     }
   }
