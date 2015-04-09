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
		
			// variable to keep track of the recursion depth
	    	int depth = 0;
	    	
	    	Configuration conf =	new Configuration();
			// set the depth into the configuration
			conf.set("recursion.depth", depth + "");
		    
		    Job job = new Job(conf);
		    long counter = job.getCounters().findCounter(ParallelBFSReducer.UpdateCounter.UPDATED).getValue();
		    
	    	
	    	while(counter > 0){
	    		
				//line added for counter
				conf =	new Configuration();
				// set the depth into the configuration
				conf.set("recursion.depth", depth + "");
			    
			    job = new Job(conf);
			    
			 // counter from the previous running import job
			    counter = job.getCounters().findCounter(ParallelBFSReducer.UpdateCounter.UPDATED).getValue();
			     
			    
			    
			    job.setJarByClass(ParallelBFS.class);
			    job.setJobName("Parallel BFS " +depth);
			    
			    
			    //depth++;
			    
	
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    
			    job.setMapperClass(ParallelBFSMapper.class);
			    job.setReducerClass(ParallelBFSReducer.class);
			    
			    
			    //Newly added till fs.delete
			    
			 // always work on the path of the previous depth
			    Path in = new Path("files/graph-exploration/depth_" + (depth - 1) + "/");
			    Path out = new Path("files/graph-exploration/depth_" + depth);
			  
			    SequenceFileInputFormat.addInputPath(job, in);
			    // delete the outputpath if already exists
			 // configuration should contain reference to your namenode
			    FileSystem fs = FileSystem.get(new Configuration());
			    if (fs.exists(out))
			     fs.delete(out, true);
		
			    SequenceFileOutputFormat.setOutputPath(job, out);
			    
			    // Below 2 Lines added
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(Text.class);
	
			    job.setOutputKeyClass(IntWritable.class);
			    job.setOutputValueClass(Text.class);
			    
			    //Changed from System.exit(job.waitForCompletion(true) ? 0 : 1);
			    job.waitForCompletion(true);
			    depth++;
			    counter = job.getCounters().findCounter(ParallelBFSReducer.UpdateCounter.UPDATED).getValue();
	    	}
	}

}
