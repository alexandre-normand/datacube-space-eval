package skuehn.datacube.space.eval;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupingComparator extends WritableComparator {

	protected KeyGroupingComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		// <field-id-num><KEY_SEP><value>
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		String[] t1Items = t1.toString().split(Constants.DEFAULT_KEY_SEP);
		String[] t2Items = t2.toString().split(Constants.DEFAULT_KEY_SEP);
		int comp = t1Items[0].compareTo(t2Items[0]);
		return comp;

	}

}
