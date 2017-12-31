package mvnHadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MaxTemperatureMapperTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
						"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new MaxTemperatureMapper())
				.withInput(new LongWritable(0), value)
				.withOutput(new Text("1950"), new IntWritable(-11)).runTest();
		// test if the result after mapper is equals what we expect.
	}

	@Test
	public void ignoresMissingTemperatureRecord() throws IOException,
			InterruptedException {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
						"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		// Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new MaxTemperatureMapper())
				.withInput(new LongWritable(0), value).runTest();
	}

	@Test
	public void returnsMaximumIntegerInValues() throws IOException,
			InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
				.withReducer(new MaxTemperatureReducer())
				.withInput(new Text("1950"),
						Arrays.asList(new IntWritable(10), new IntWritable(5)))
				.withOutput(new Text("1950"), new IntWritable(10)).runTest();
	}


}
