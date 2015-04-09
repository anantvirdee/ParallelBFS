import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class ParallelBFS {
	
	public static void main(String args[]) throws Exception{
		
		if (args.length != 2) {
		      System.err.println("Usage: ParallelBFS <input path> <output path>");
		      System.exit(-1);
		    }
		
			Job job = new Job();
		    
		    job.setJarByClass(ParallelBFS.class);
		    job.setJobName("Parallel BFS ");
		    

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.setMapperClass(ParallelBFSMapper.class);
		    job.setReducerClass(ParallelBFSReducer.class);
		    
		    
		    // Below 2 Lines added
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);

		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
	    	
	}

}
