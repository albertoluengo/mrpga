package common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.MRPGAMapper;

/**
 * Clase que representa el nodo Master del sistema <code>MapReduce</code> 
 * implementado. Se encargar&#225; de lanzar el propio trabajo <code>MapReduce</code>,
 * indicando cu&#225;l ser&#225; la clase <code>Mapper</code>, cu&#225;l la <code>Reducer</code>,
 * etc.
 * @author Alberto Luengo Cabanillas
 *
 */
public class MRPGAMaster extends Configured implements Tool {
	
	/**
	 * M&#233;todo que lanza un trabajo<code>MapReduce</code>, obteniendo la 
	 * configuraci&#243;n necesaria del HDFS subyacente (par&#225;metros, sistemas de ficheros,etc)
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @param iter Iteraci&#243;na actual del sistema en el que se enmarca esta clase.
	 */

	void launch(String iter, int numReducers, Class<? extends MRPGAMapper> problemMapperClass, Class<? extends MRPGAReducer> problemReducerClass) {
		Configuration conf = new Configuration();
		Job job = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("MRPGAMASTER: Error instanciando el HDFS...");
			System.exit(0);
		}
		try {
			job = new Job(conf, "MRPGA_project");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("MRPGAMASTER: Error instanciando el trabajo...");
			System.exit(0);
		}
		
		job.setNumReduceTasks(numReducers);
		job.setMapperClass(problemMapperClass);
		job.setReducerClass(problemReducerClass);
		job.setCombinerClass(problemReducerClass);
		//Especificamos los tipos de salida...
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setPartitionerClass(common.RandomDoublePartitioner.class);
	    job.setJobName("mrpga-"+problemReducerClass+iter);
	    //Aunque este 'deprecated' esta linea es necesaria para utilizar las clases hija
	    //si se empaqueta la aplicación como un JAR
	    ((JobConf) job.getConfiguration()).setJar("mrpga_cluster.jar");

	    
			
		/*Especificamos los directorios de entrada y salida que van a utilizarse
	     */
	    try {
			FileInputFormat.addInputPath(job, new Path("input"));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("MRPGAMASTER:Se ha producido un error leyendo el directorio de entrada...");
	    	System.exit(0);
		}
	    Path outputPath = new Path("output");
	    
	    try {
	    	if (fs.exists(outputPath)) {
	    		//Eliminamos el directorio de salida primero...
	    		fs.delete(outputPath,true);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("MRPGAMASTER:Se ha producido un error borrando los directorios");
	    	System.exit(0);
	    }
 
	    FileOutputFormat.setOutputPath(job, outputPath);
	 
	    try {
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
		
	/**
	 * M&#233;todo de la clase <code>ToolRunner</code> que parsea los par&#225;metros 
	 * introducidos por consola y ejecuta el m&#233;todo <code>launch</code> y accede 
	 * a la configuraci&#243;n (clase <code>Configuration</code>) del HDFS subyacente.
	 * @param args Array de par&#225;metros que recibe del <code>Coordinador</code>.
	 * @return C&#243;digo de salida (0-->"Ejecuci&#243;n correcta", -1-->"Error").
	 */
	public int run(String[] args) throws Exception {
		String iter = args[0];
		int numReducers = Integer.parseInt(args[1]);
		Class<? extends MRPGAMapper> problemMapperClass = (Class<? extends MRPGAMapper>) Class.forName(args[2]);
		Class<? extends MRPGAReducer> problemReducerClass = (Class<? extends MRPGAReducer>) Class.forName(args[3]);
		launch(iter, numReducers,problemMapperClass, problemReducerClass);
		return 0;
	}
	
	/**
	 * M&#233;todo principal de la clase <code>MRPGAMaster</code> cuya &#250;nica funci&#243;n es 
	 * llamar al m&#233;todo <code>run</code> de la misma clase.
	 * 
	 * @param argv Array de comandos recibidos del <code>Coordinador</code>.
	 * @throws Exception Excepci&#243;n gen&#233;rica.
	 */
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRPGAMaster(), argv);
	}
}
