package pa2.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringIntegerList implements Writable {
    private List<StringInteger> stringIntegerList = new LinkedList<>();
    private Pattern stringIntegerPattern = Pattern.compile("<([^>]+),(\\d+)>");

    public void push(StringInteger stringInteger) {
        stringIntegerList.add(stringInteger);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeCompressedString(out, this.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String StringIntegerStr = WritableUtils.readCompressedString(in);
        readFromString(StringIntegerStr);
    }

    public void readFromString(String stringIntegerStr) {
        Matcher m = stringIntegerPattern.matcher(stringIntegerStr);
        List<StringInteger> tempList = new LinkedList<>();
        while (m.find()) {
            StringInteger index = new StringInteger(m.group(1), Integer.parseInt(m.group(2)));
            tempList.add(index);
        }
        this.stringIntegerList = tempList;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < stringIntegerList.size(); i++) {
            StringInteger index = stringIntegerList.get(i);

                sb.append(index + ",");

        }
        //remove the last ","
        if (sb != null || sb.length() > 1) {
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    public List<StringInteger> getStringIntegerList() {
        return stringIntegerList;
    }

    public void setStringIntegerList(List<StringInteger> stringIntegerList) {
        this.stringIntegerList = stringIntegerList;
    }
}
