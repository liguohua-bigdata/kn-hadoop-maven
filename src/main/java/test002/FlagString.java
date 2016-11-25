package test002;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * 自定义的JavaBean
 */
public class FlagString implements WritableComparable<FlagString> {
    private String value;//记录phoneComment or userName
    private int flag; // 标记 0:表示phone表 1：表示user表

    public FlagString(String value, int flag) {
        super();
        this.value = value;
        this.flag = flag;
    }

    public String getValue() {
        return value;
    }

    public int getFlag() {
        return flag;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(flag);
        out.writeUTF(value);

    }

    public void readFields(DataInput in) throws IOException {
        this.flag = in.readInt();
        this.value = in.readUTF();
    }

    public int compareTo(FlagString o) {
        if (this.flag >= o.getFlag()) {
            if (this.flag > o.getFlag()) {
                return 1;
            }
        } else {
            return -1;
        }
        return this.value.compareTo(o.getValue());
    }

}
