package reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_SplitWord_Fourth extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		//-1表示返回所有的子字符串数
		String[] keyArray = key.toString().split("\t", -1);
		if ("RETERM".equals(keyArray[0])) {
			 String	number = "";
			for (Text text : values) {
			 number = String.valueOf(text);
			}
			//number表示每个字重复了几次,NullWritable.get()是空值
			System.out.println(key.toString() + "\t" + number);
			context.write(NullWritable.get(), new Text(key.toString() + "\t" + number));
		} 

	
	
	}
}

