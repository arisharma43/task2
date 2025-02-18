package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// StringTokenizer itr = new StringTokenizer(value.toString());
		String[] fields = value.toString().split(",");
		// if (!fields[0].equals("YEAR")) {
		// word.set(fields[7]);
		// context.write(word, counter);
		// }
		if (fields.length > 8) {
			String oAirport = fields[7];
			if (!oAirport.equals("ORIGIN_AIRPORT") && !oAirport.isEmpty()) {
				word.set(oAirport);
				context.write(word, counter);
			}
		}
		// while (itr.hasMoreTokens()) {
		// word.set(itr.nextToken());
		// context.write(word, counter);
		// }
	}
}