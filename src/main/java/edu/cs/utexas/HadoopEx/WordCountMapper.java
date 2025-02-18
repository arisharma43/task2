package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	// private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();
	private Text delayAndCount = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		// Task 1:
		// if (fields.length > 8) {
		// String oAirport = fields[7];
		// if (!oAirport.equals("ORIGIN_AIRPORT") && !oAirport.isEmpty()) {
		// word.set(oAirport);
		// context.write(word, counter);
		// }
		// }
		// Task 2:
		if (fields.length > 11 && !fields[0].equals("YEAR")) { // Ensure there are enough columns
			String airline = fields[4];
			String delay = fields[11];

			if (!airline.equals("AIRLINE") && !airline.isEmpty()) {
				try {
					word.set(airline);

					// Validate and handle empty delay values
					int departDelay = delay.isEmpty() ? 0 : Integer.parseInt(delay);

					// Emit (airline, "delay,1") as key-value pair
					delayAndCount.set(departDelay + ",1");
					context.write(word, delayAndCount);
				} catch (NumberFormatException e) {
					// TODO: handle exception
				}

			}
		}
	}
}