package test002;

/**
 * Created by liguohua on 16/9/21.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 此类读取phone
 */
public class MultiMap1 extends Mapper<LongWritable, Text, Text, FlagString> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlagString>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.length() > 0) {
            String[] str = line.split(",");
            if (str.length == 2) {
                Text phoneNo = new Text(str[0].trim());//outkey
                String phoneComment = str[1].trim();//outvalue
                context.write(phoneNo, new FlagString(phoneComment, 0)); // flag=0,表示phone表
            }
        }
    }
}