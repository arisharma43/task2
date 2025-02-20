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
	private Text airline = new Text();
	private Text delayAndCount = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		if (fields.length > 11 && !fields[0].equals("YEAR")) { // Ensure there are
			// enough columns
			String delay = fields[11];

			if (!fields[4].equals("AIRLINE") && !fields[4].isEmpty()) {
				try {
					airline.set(fields[4]);

					// Validate and handle empty delay values
					int departDelay = delay.isEmpty() ? 0 : Integer.parseInt(delay);

					// Emit (airline, "delay,1") as key-value pair
					delayAndCount.set(departDelay + ",1");
					context.write(airline, delayAndCount);
				} catch (NumberFormatException e) {
					// TODO: handle exception
				}

			}
		}
	}
}