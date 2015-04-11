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
	
	public static enum UpdateCounter{
		UPDATED
	};
	
	
	public static void main(String args[]) throws Exception{
		
		//Number of iteration/depth of parallelBFS initially hardcoding
		int iterations = 3;
		
		//To keep track of depth of bfs
		int depth = 0;
		//Setting up configuration
		//Configuration conf	=	new Configuration();
		//conf.set("Updated Counter",);
		
		//Checking if program is passed a file(containing graph info) initially as an argument
		if (args.length != 1) {
		      System.err.println("Usage: ParallelBFS <input path> ");
		      System.exit(-1);
		    }
		
		Path input_path;
		Path output_path;
		
		//To run the first iteration of mapreduce taking user's input file i.e ParallelBFS <input path> 
		
		Job job = new Job();
	    
	    job.setJarByClass(ParallelBFS.class);
	    job.setJobName("Parallel BFS "+depth);
	    
	    //Setting the mapreduce output_path
	    output_path = new Path("files/parallel-bfs/depth_"+depth);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output_path);
	    
	    job.setMapperClass(ParallelBFSMapper.class);
	    job.setReducerClass(ParallelBFSReducer.class);
	    
	    
	    // Below 2 Lines added
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.waitForCompletion(true);
	    
	    depth++;
		
		for(int i = 1; i < iterations; i++){
				
				job = new Job();
			    
			    job.setJarByClass(ParallelBFS.class);
			    job.setJobName("Parallel BFS "+depth);
			    
			    input_path = new Path("files/parallel-bfs/depth_"+(depth - 1)+"/");
			    output_path= new Path("files/parallel-bfs/depth_"+depth);
			    
			    FileInputFormat.addInputPath(job, input_path);
			    FileOutputFormat.setOutputPath(job, output_path);
			    
			    job.setMapperClass(ParallelBFSMapper.class);
			    job.setReducerClass(ParallelBFSReducer.class);
			    
			    
			    // Below 2 Lines added
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(Text.class);
	
			    job.setOutputKeyClass(IntWritable.class);
			    job.setOutputValueClass(Text.class);
			    
			    job.waitForCompletion(true);
			    depth++;
		}
	    	
	}

}
