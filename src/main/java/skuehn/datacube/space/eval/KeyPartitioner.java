package skuehn.datacube.space.eval;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<Text, Text> implements Configurable {

	private Configuration conf;
	
	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		String fieldId = key.toString().split(conf.get(Constants.KEY_SEP, Constants.DEFAULT_KEY_SEP))[0];
		int field = new Integer(fieldId);
		return field % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(final Configuration conf) {
		this.conf = new Configuration(conf);		
	}

}
