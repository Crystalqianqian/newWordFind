package map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class Map_SplitWord_Fourth extends Mapper<LongWritable, Text, Text, Text> {

	private static IKAnalyzer IK;
	private static TokenStream TS;
	
	private static final Pattern DATAPATTERN = Pattern.compile("[\\pP\\pM\\pZ]");
	private static final Pattern CHINESEPATTERN = Pattern.compile("\\s*|\\t*|\\r*|\\n*");
	
	private static String word = "";
	private static StringReader stringReader = null;
    private int WORD_NUMBER = 0;
    private Map<String, Integer> termMap = new HashMap<String, Integer>();
	private Map<String, Integer> wordMap = new HashMap<String, Integer>();
	private Map<String, Integer> retermMap = new HashMap<String, Integer>();
	private Map<String, Integer> dwordMap = new HashMap<String, Integer>();
	private Map<String, Integer> twordMap = new HashMap<String, Integer>();
    
    private static Logger logger=LoggerFactory.getLogger(Map_SplitWord_Fourth.class);
    
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println(">>>>>>>>>>开始初始化分词器<<<<<<<<<<");
		IK = new IKAnalyzer(false);
		try {
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Path[] paths = context.getLocalCacheFiles();
        readCacheFile(paths[0]);
		System.out.println(">>>>>>>>>>分词器初始化完毕<<<<<<<<<<");
		
		
		//通过頻数和内部凝固程度排除
		for(String word:wordMap.keySet()){
			
			String[] termSplit = word.split("/");
//			System.out.println(termSplit[0]+termSplit[1]+termSplit[2]+termSplit[3]);
			//x为没歌词的頻数
			BigDecimal x = new BigDecimal(wordMap.get(word)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP);
//			System.out.println(x);
			
			List<Integer> yList = new ArrayList<>();
			List<Integer> yyList = new ArrayList<>();
			List<Integer> yyyList = new ArrayList<>();
//			List<Integer> tList = new ArrayList<>();
			//四个字的词分三种情况:第一种情况
			for(String term:termMap.keySet()){
				
				if (term.equals(termSplit[0])) {
					
					yList.add(termMap.get(term));
					
					for (String tword:twordMap.keySet()) {
						
						if (tword.equals(termSplit[1]+termSplit[2]+termSplit[3])) {
							
							yList.add(twordMap.get(tword));
						}
					}
				}
			}
			//四个字的词分三种情况:第二种情况
			for(String dword:dwordMap.keySet()){
				
				if (dword.equals(termSplit[0]+termSplit[1])) {
					
					yyList.add(dwordMap.get(dword));
					
					for (String dwordd:dwordMap.keySet()) {
						
						if (dwordd.equals(termSplit[2]+termSplit[3])) {
							
							yyList.add(dwordMap.get(dwordd));
						}
					}
//				System.out.println(yyList.get(0)+"  "+yyList.get(1));
				}
			}
			
			//四个字的词分三种情况:第三种情况
			for(String tword:twordMap.keySet()){
				
				if (tword.equals(termSplit[0]+termSplit[1]+termSplit[2])) {
					
					yyyList.add(twordMap.get(tword));
					
					for (String term:termMap.keySet()) {
						
						if (term.equals(termSplit[3])) {
							
							yyyList.add(termMap.get(term));
						}
					}
				}
			}
			
			
			
			BigDecimal y1 = new BigDecimal(yList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y2 = new BigDecimal(yList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y3 = new BigDecimal(yyList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y4 = new BigDecimal(yyList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y5 = new BigDecimal(yyyList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y6 = new BigDecimal(yyyList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			
			yList = null;
			yyList = null;
			yyyList = null;
			
			Double temp1 = y1.multiply(y2).doubleValue();
			Double temp2 = y3.multiply(y4).doubleValue();
			Double temp3 = y5.multiply(y6).doubleValue();
			
			if (temp1 != 0 && temp2 != 0 && temp3 != 0) {
				double doubleValue1 = x.divide(new BigDecimal(temp1), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
				double doubleValue2 = x.divide(new BigDecimal(temp2), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
				double doubleValue3 = x.divide(new BigDecimal(temp3), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
				
//				System.out.println(doubleValue);
				//20以下 '有一' '了我' '了一'
				if ((doubleValue1> 20.0 && x.doubleValue() > 0.000125) 
						|| (doubleValue2> 20.0 && x.doubleValue() > 0.000125)
						|| (doubleValue3> 20.0 && x.doubleValue() > 0.000125)) {
					
//					wordAfterMap.put(word, x);
					
//					context.write(new Text(word), new Text(String.valueOf(x)+"\t"+String.valueOf(doubleValue)));
//					System.out.println("RETERM" + "\t" + termSplit[0] + termSplit[1] + termSplit[2] + termSplit[3]);
					context.write(new Text("RETERM" + "\t" + termSplit[0] + termSplit[1] + termSplit[2] + termSplit[3]), new Text(String.valueOf(wordMap.get(word))));
				}
			}
		}
//		for (String term : termMap.keySet()) {
//			context.write(new Text("TERM" + "\t" + term), new Text(String.valueOf(termMap.get(term))));
//		}
//		
//		for (String term : twordMap.keySet()) {
//			context.write(new Text("TWORD" + "\t" + term), new Text(String.valueOf(twordMap.get(term))));
//		}
//		
//		for (String term : dwordMap.keySet()) {
//			context.write(new Text("DWORD" + "\t" + term), new Text(String.valueOf(dwordMap.get(term))));
//		}
		
//		for (String term : retermMap.keySet()) {
//			context.write(new Text("RETERM" + "\t" + term), new Text(String.valueOf(retermMap.get(term))));
//			System.out.println("RETERM" + "\t" + term+String.valueOf(retermMap.get(term)));
//		}
		
//		context.write(new Text("SIZE" + "\t" + "总数" + WORD_NUMBER), new Text());
		
		
		for (String term : retermMap.keySet()) {
			context.write(new Text("RETERM" + "\t" + term), new Text(String.valueOf(retermMap.get(term))));
//			System.out.println("RETERM" + "\t" + term+String.valueOf(retermMap.get(term)));
		}
		
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String text = value.toString();
		List<String> tokens = new ArrayList<String>();
		
		try {
			stringReader = new StringReader(text);
			TS = IK.tokenStream("", stringReader);
			CharTermAttribute term = TS.addAttribute(CharTermAttribute.class);
			TS.reset();
			while (TS.incrementToken()) {
				word = term.toString();
				tokens.add(word);
							
				}
		}catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	private void readCacheFile(Path cacheFilePath) throws IOException {
//		BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
		BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split("\t");
//			System.out.println(temp);
			for (int j = 0; j < temp.length; j++) {
				if (temp[0].equals("TERM")) {
					termMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("WORD")) {
					wordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("SIZE")) {
					WORD_NUMBER = Integer.parseInt(temp[2]);
				}else if (temp[0].equals("DWORD")) {
					dwordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("TWORD")) {
					twordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("RETERM")) {
					retermMap.put(temp[1], Integer.parseInt(temp[2]));
//					System.out.println(temp[1] +"   "+temp[2]);
				}
			}
		}
		reader.close();
	}
	/**
	 * 判断字符是否是中文
	 *
	 * @param c
	 *            字符
	 * @return 是否是中文
	 */
	private boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	/**
	 * 判断字符串是否是乱码
	 *
	 * @param string字符串
	 * @return 是否是乱码
	 */
	private boolean isMessyCode(String string) {
		Matcher m = CHINESEPATTERN.matcher(string);
		String after = m.replaceAll("");
		String temp = after.replaceAll("\\p{P}", "");
		char[] ch = temp.trim().toCharArray();
		float chLength = ch.length;
		float count = 0;
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (!Character.isLetterOrDigit(c)) {
				if (!isChinese(c)) {
					count = count + 1;
				}
			}
		}
		float result = count / chLength;
		if (result > 0.4) {
			return true;
		} else {
			return false;
		}
	}
}

