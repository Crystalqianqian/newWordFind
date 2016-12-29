package reduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_SplitWord_Third extends Reducer<Text, Text, NullWritable, Text> {
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
		} else if ("TERM".equals(keyArray[0])) {
			 String	number = "";
			for (Text text : values) {
			 number = String.valueOf(text);
			}
//			System.out.println(number);
			System.out.println(key.toString() + "\t" + number);
			context.write(NullWritable.get(), new Text(key.toString() + "\t" + number));
		}else if ("DWORD".equals(keyArray[0])) {
			 String	number = "";
				for (Text text : values) {
				 number = String.valueOf(text);
				}
			//number表示每个字重复了几次,NullWritable.get()是空值
			System.out.println(key.toString() + "\t" + number);
			context.write(NullWritable.get(), new Text(key.toString() + "\t" + number));
			
		}else if ("TWORD".equals(keyArray[0])) {
			 String	number = "";
				for (Text text : values) {
				 number = String.valueOf(text);
				}
			//number表示每个字重复了几次,NullWritable.get()是空值
			System.out.println(key.toString() + "\t" + number);
			context.write(NullWritable.get(), new Text(key.toString() + "\t" + number));
			
		}else if ("WORD".equals(keyArray[0])) {
			int number = 0;
			Map<String, Integer> leftMap = new HashMap<String, Integer>();
			Map<String, Integer> rightMap = new HashMap<String, Integer>();
			//算出在一个map的新词里,左邻字和右邻字重复的次数
			for (Text text : values) {
				//number为一个新词重复了多少次
				number++;
				String[] valueArray = text.toString().split("\t", -1);
				if (leftMap.get(valueArray[0]) == null) {
					leftMap.put(valueArray[0], 1);
				} else {
					int leftNumber = leftMap.get(valueArray[0]) + 1;
					leftMap.put(valueArray[0], leftNumber);
				}
				if (rightMap.get(valueArray[2]) == null) {
					rightMap.put(valueArray[2], 1);
				} else {
					int rightNumber = rightMap.get(valueArray[2]) + 1;
					rightMap.put(valueArray[2], rightNumber);
				}
			}
			double leftFreq = 0;
			double rightFreq = 0;
			//信息熵(信息熵越大,自由度越高)
			for (String k : leftMap.keySet()) {
				//保留小数后8位
				BigDecimal x = new BigDecimal(leftMap.get(k)).divide(new BigDecimal(number), 8, BigDecimal.ROUND_DOWN);
				leftFreq -= x.setScale(8).doubleValue() * Math.log(x.setScale(8).doubleValue());
			}

			for (String k : rightMap.keySet()) {
				BigDecimal x = new BigDecimal(rightMap.get(k)).divide(new BigDecimal(number), 8, BigDecimal.ROUND_DOWN);
				rightFreq -= x.setScale(8).doubleValue() * Math.log(x.setScale(8).doubleValue());
			}
			//通过信息熵排除
			if (leftFreq > 2 && rightFreq > 2) {
				System.out.println(key.toString() + "\t" + number);
				context.write(NullWritable.get(), new Text(key.toString() + "\t" + number ));
			}
		}else if ("SIZE".equals(keyArray[0])) {
			context.write(NullWritable.get(), new Text(key.toString() ));
			System.out.println(key);
		}

	
	
	}
}
