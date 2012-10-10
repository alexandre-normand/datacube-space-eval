package skuehn.datacube.space.eval;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CardinalityMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Configuration conf;
	private String fieldDelimiter;
	private int numDimensions;
	private String keySep;
	private static final Text VAL = new Text();

	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		fieldDelimiter = conf.get(Constants.FIELD_DELIMITER,
				Constants.DEFAULT_DELIMITER);
		numDimensions = conf.getInt(Constants.NUM_DIMENSIONS, -1);
		keySep = conf.get(Constants.KEY_SEP, Constants.DEFAULT_KEY_SEP);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] dimensions = value.toString().split(Pattern.quote(fieldDelimiter));
		if (dimensions.length != numDimensions) {
			context.getCounter(Constants.COUNTER_GROUP,
					Constants.INCORRECT_NUM_FIELDS).increment(1L);
			return;
		}
		for (int i = 0; i < dimensions.length; i++) {
			VAL.set(dimensions[i]);
			context.write(new Text(i + keySep + dimensions[i]), VAL);
		}
	}
}
