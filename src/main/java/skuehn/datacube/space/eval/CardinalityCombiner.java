package skuehn.datacube.space.eval;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Removes duplicate values
 */
public class CardinalityCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(values.iterator().hasNext()) {
			context.write(key, values.iterator().next());
		}
	}
}
