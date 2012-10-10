package skuehn.datacube.space.eval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Assess the cardinality of each field in a dataset
 * The dataset field seperator must be a string literal, so '	' is a TAB.
 */
public class CardinalityEvalJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        
        if (remainingArgs.length < 4) {
            System.err.println("Usage: SpaceEvalJob <field-delimiter (string literal)> <num-dimensions> <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        
        Configuration conf = getConf();
        conf.set(Constants.FIELD_DELIMITER, remainingArgs[0]);
        conf.set(Constants.NUM_DIMENSIONS, remainingArgs[1]);
        Job job = new Job(conf, Constants.JOB_NAME);
        job.setJarByClass(getClass());
        
        // Map-side config
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class); 
        job.setMapperClass(CardinalityMapper.class);
        TextInputFormat.addInputPath(job, new Path(remainingArgs[2]));

        // Reduce-side config
        job.setCombinerClass(CardinalityCombiner.class);
        job.setReducerClass(CardinalityReducer.class);
        job.setGroupingComparatorClass(KeyGroupingComparator.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setNumReduceTasks(new Integer(conf.get(Constants.NUM_DIMENSIONS)));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[3]));
        
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CardinalityEvalJob(), args);
        System.exit(res);
    }
}
