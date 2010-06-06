package common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Clase que representa el nodo Master del sistema <code>MapReduce</code> 
 * implementado. Se encargará de lanzar el propio trabajo <code>MapReduce</code>,
 * indicando cuál será la clase <code>Mapper</code>, cuál la <code>Reducer</code>,
 * etc.
 * @author Alberto Luengo Cabanillas
 *
 */
public class MRPGAMaster extends Configured implements Tool {
	
	/**
	 * Método que lanza un trabajo<code>MapReduce</code>, obteniendo la 
	 * configuración necesaria del HDFS subyacente (parámetros, sistemas de ficheros,etc)
	 * @param numProblem Número que indica el número de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @param iter Iteracióna actual del sistema en el que se enmarca esta clase.
	 */
	void launch(int numProblem, String iter) {
		Configuration conf = new Configuration();
		Job job = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			job = new Job(conf, "MRPGA_project");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job.setJarByClass(common.MRPGAMaster.class);
		
		
		switch (numProblem){
		
		case 1: //Problema 'Frase Objetivo'
			System.out.println("MASTER: PROBLEMA FRASE OBJETIVO");
			job.setMapperClass(targetphrase.TargetPhraseMapper.class);
			job.setReducerClass(targetphrase.TargetPhraseReducer.class);
			//job.setCombinerClass(fuentes.HWorldPseudoReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setPartitionerClass(common.RandomPartitioner.class);
		    job.setJobName("mrpga-target-"+iter);
		    break;
			
		case 2: //Problema 'OneMAX'
			System.out.println("MASTER: PROBLEMA ONEMAX");
			job.setMapperClass(onemax.OneMAXMapper.class);
			job.setReducerClass(onemax.OneMAXReducer.class);
			//job.setCombinerClass(onemax.OneMAXReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setPartitionerClass(common.RandomDoublePartitioner.class);
		    job.setJobName("mrpga-onemax-"+iter);
		    break;
			
		case 3: //Problema 'PPeaks'
			System.out.println("MASTER: PROBLEMA PPEAKS");
			job.setMapperClass(ppeaks.PPEAKSMapper.class);
			job.setReducerClass(ppeaks.PPEAKSReducer.class);
			//job.setCombinerClass(ppeaks.PPEAKSReducer.class);
			//Especificamos los tipos de salida...
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setPartitionerClass(common.RandomDoublePartitioner.class);
		    job.setJobName("mrpga-ppeaks-"+iter);
		    break;
		
		}
		

		/*Especificamos los directorios de entrada y salida que van a utilizarse
	     */
	    try {
			FileInputFormat.addInputPath(job, new Path("input"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    Path outputPath = new Path("output");
	    //FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(job.getConfiguration()),job.getConfiguration());
	    
	    try {
	    	if (fs.exists(outputPath)) {
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(outputPath,true);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("MASTER:Se ha producido un error borrando los directorios");
	    	System.exit(1);
	    }
 
	    FileOutputFormat.setOutputPath(job, outputPath);
	 
	    try {
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	/**
	 * Método de la clase <code>ToolRunner</code> que parsea los parámetros 
	 * introducidos por consola y ejecuta el método <code>launch</code> y accede 
	 * a la configuración (clase <code>Configuration</code>) del HDFS subyacente.
	 * @param args Array de parámetros que recibe del <code>Coordinador</code>.
	 * @return Código de salida (0-->"Ejecución correcta", -1-->"Error").
	 */
	public int run(String[] args) throws Exception {
		int	numProblem = Integer.parseInt(args[0]);
		String iter = args[1];
		launch(numProblem, iter);
		return 0;
	}
	
	/**
	 * Método principal de la clase <code>MRPGAMaster</code> cuya única función es 
	 * llamar al método <code>run</code> de la misma clase.
	 * 
	 * @param argv Array de comandos recibidos del <code>Coordinador</code>.
	 * @throws Exception Excepción genérica.
	 */
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRPGAMaster(), argv);
	}
}