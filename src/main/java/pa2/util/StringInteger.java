package pa2.util;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StringInteger implements WritableComparable<StringInteger> {
    private String wordText;
    private Integer wordCount;


    public StringInteger() {
    }

    public StringInteger(String wordText, Integer wordCount) {
        this.wordText = wordText;
        this.wordCount = wordCount;
    }

    public void setWordText(String wordText) {
        this.wordText = wordText;
    }

    public void setWordCount(Integer wordCount) {
        this.wordCount = wordCount;
    }

    public String getWordText() {
        return wordText;
    }

    public Integer getWordCount() {
        return wordCount;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wordText = in.readUTF();
        wordCount = in.readInt();


    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(wordText);
        out.writeInt(wordCount);

    }



    @Override
    public String toString() {
        return '<' + wordText + ',' + wordCount + '>';
    }

    @Override
    public int compareTo(StringInteger o) {
        return 1;
    }
}
