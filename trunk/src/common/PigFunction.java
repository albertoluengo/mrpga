package common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;

/**
 * Clase que ejecuta c&#243;digo <code>Pig Latin</code> embebido en Java, 
 * que se encarga de recopilar los distintos individuos sub-&#243;ptimos que 
 * generan los nodos <code>Reduce</code> (almacenados
 * en un fichero de texto) y aplica sobre ellos las operaciones 
 * que se necesiten. Adem&#225;s, generar&#225; un nuevo fichero de poblaci&#243;n para que
 * comience una nueva iteraci&#243;n en el sistema.
 * @author Alberto Luengo Cabanillas
 *
 */
public class PigFunction {

	/**
	 * M&#233;todo principal de la clase <code>PigFunction</code> que se encarga
	 * de procesar los ficheros de poblaci&#243;n resultantes de los nodos 
	 * <code>Reducer</code>.
	 * @param args Array de comandos.
	 * @throws IOException Excepci&#243;n lanzada al haber alg&#250;n problema manipulando
	 * ficheros o directorios.
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String inputFile = args[0];
		int iteration = Integer.parseInt(args[1]);

		FileSystem fs = FileSystem.get(conf);
		Path resultPath = new Path("pigResults");
	    
	    try {
	    	if (fs.exists(resultPath)) {
	    		//Borro el directorio con todo su contenido
	    		fs.delete(resultPath,true);
	    	}
	    }
	    catch (IOException ioe) {
	    	System.err.println("PIGFUNCTION["+iteration+"]:Se ha producido un error borrando el dir de salida de Pig");
	    	System.exit(1);
	    }
		
	    String popIterationName = "input/population_"+iteration+".dat";
		PigServer pigServer = new PigServer("mapreduce");
		
		pigServer.registerQuery("raw_data = load '" + inputFile + "' using PigStorage('	');");
		pigServer.registerQuery("B = foreach raw_data generate $0 as id;");
		pigServer.registerQuery("store B into 'pigResults';");
		pigServer.renameFile("pigResults/part-00000", popIterationName);
		
		//Cuando acabo, borro el contenido del dir 'pigResults' (en el que sólo quedarán los logs...)
		fs.delete(resultPath,true);

	}

}
