package fuentes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;

public class MRPGAMaster {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "MRPGA_project");
		job.setJarByClass(fuentes.MRPGAMaster.class);
		
		switch (Integer.parseInt(args[0])){
		
		case 1: //Problema 'Hola Mundo'
			System.out.println("MASTER: PROBLEMA HOLA MUNDO");
			job.setMapperClass(fuentes.HWorldPseudoMapper.class);
			job.setReducerClass(fuentes.HWorldPseudoReducer.class);
			//job.setCombinerClass(fuentes.HWorldPseudoReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setPartitionerClass(fuentes.RandomPartitioner.class);
		    break;
			
		case 2: //Problema 'OneMAX'
			System.out.println("MASTER: PROBLEMA ONEMAX");
			job.setMapperClass(fuentes.OneMAXMapper.class);
			//job.setReducerClass(fuentes.OneMAXReducer.class);
			//job.setCombinerClass(fuentes.OneMAXReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setPartitionerClass(fuentes.RandomDoublePartitioner.class);
		    break;
			
		case 3: //Problema 'PPeaks'
			System.out.println("MASTER: PROBLEMA PPEAKS");
			job.setMapperClass(fuentes.PPEAKSMapper.class);
			//job.setReducerClass(fuentes.PPEAKSReducer.class);
			//job.setCombinerClass(fuentes.PPEAKSReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setPartitionerClass(fuentes.RandomDoublePartitioner.class);
		    break;
		
		}
		
		
	    //final String HDFS_MAPPER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	    //Path hdfsConfMapPath = new Path(HDFS_MAPPER_CONFIGURATION_FILE);
	    
	    //final String HDFS_REDUCER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	    //Path hdfsConfRedPath = new Path(HDFS_REDUCER_CONFIGURATION_FILE);
	    
	    //URI uriMapper =new URI("/user/hadoop-user/data/mapper_configuration.dat");
	    //URI uriReducer =new URI("/user/hadoop-user/data/reducer_configuration.dat");
	   
	    //Tenemos que ver si existen esos ficheros primero en el DFS
	    //if (fs.exists(hdfsConfMapPath))System.out.println("MASTER:existe el fichero de conf de Mappers");
	    //if (fs.exists(hdfsConfRedPath))System.out.println("MASTER:existe el fichero de conf de Reducers");
	    
//	    //Añadimos los ficheros de configuracion como distribuidos...
//		DistributedCache.addCacheFile(uriMapper,conf);
//		DistributedCache.addCacheFile(uriReducer,conf);
//		
//		String configureCacheName = new Path(HDFS_MAPPER_CONFIGURATION_FILE).getName();
//		System.out.println("MASTER:el nombre del fichero cache es "+configureCacheName);

		//Especificamos los tipos de salida...
//	    job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(IntWritable.class);

		/*Especificamos los directorios de entrada y salida que van a utilizarse
	     */
	    FileInputFormat.addInputPath(job, new Path("input"));
	    Path outputPath = new Path("output");
	    		
	    //FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(job.getConfiguration()),job.getConfiguration());
	    
	    try {
	    	if (fs.exists(outputPath)) {
	    		//remove the directory first
	    		fs.delete(outputPath,true);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("NODO MASTER:Se ha producido un error borrando el dir");
	    	System.exit(1);
	    }
 
	    FileOutputFormat.setOutputPath(job, outputPath);
	 
	    job.waitForCompletion(true);
	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
