package skuehn.datacube.space.eval;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CardinalityReducer extends
		Reducer<Text, Text, IntWritable, IntWritable> {

	Configuration conf;
	String delimiter;
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		delimiter = conf.get(Constants.KEY_SEP, Constants.DEFAULT_KEY_SEP);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int fieldNum = new Integer(key.toString().split(delimiter)[0]);
		int numDistinctVals = 0;
		Text lastVal = null;
		for(Text value : values) {
			if(lastVal == null || value.compareTo(lastVal) !=  0) {
				lastVal = new Text(value);
				numDistinctVals++;
			}
		}
		context.write(new IntWritable(fieldNum), new IntWritable(numDistinctVals));
	}
}
