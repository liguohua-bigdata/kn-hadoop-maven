package test002;

/**
 * Created by liguohua on 16/9/21.
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiRedu extends Reducer<Text, FlagString, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<FlagString> flagStrings, Reducer<Text, FlagString, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        // 最后输出的格式为: userName,key,phoneComment
        String phoneComment = "";
        String userName = "";

        for (FlagString flagString : flagStrings) {
            if (flagString.getFlag() == 0) {
                //phone
                phoneComment = flagString.getValue();
            } else if (flagString.getFlag() == 1) {
                //user
                userName = flagString.getValue();

            }
        }
        //每个inkey处理完
        String phoneNo = key.toString();
        context.write(NullWritable.get(), new Text(phoneNo + userName + phoneComment));
    }
}