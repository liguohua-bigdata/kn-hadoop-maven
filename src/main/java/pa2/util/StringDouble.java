package pa2.util;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StringDouble implements WritableComparable<StringDouble> {
    private String str;
    private Double dou;

    @Override
    public void write(DataOutput out) throws IOException {
        StringBuffer sb = new StringBuffer();
        sb.append(str);
        sb.append(",");
        sb.append(dou);
        out.writeUTF(sb.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public int compareTo(StringDouble o) {
        return 1;
    }

    @Override
    public String toString() {
        return str + "," + dou;
    }

}
