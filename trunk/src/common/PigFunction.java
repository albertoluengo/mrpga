package common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;

/**
 * Clase que ejecuta código <code>Pig Latin<code> embebido en Java, 
 * que se encarga de recopilar los distintos individuos sub-óptimos que 
 * generan los "reduce" locales (que estaran almacenados
 * en un formato de documento de texto) y aplica sobre ellos las operaciones 
 * que se necesiten. Además, generará un nuevo fichero de población para que
 * comience una nueva iteración en el sistema.
 * @author Alberto Luengo Cabanillas
 *
 */
public class PigFunction {

	/**
	 * Método principal de la clase <code>PigFunction</code> que se encarga
	 * de procesar los ficheros de población resultantes de los nodos 
	 * <code>Reducer</code>.
	 * @param args Array de comandos.
	 * @throws IOException Excepción lanzada al haber algún problema manipulando
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
