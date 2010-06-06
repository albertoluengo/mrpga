package common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;

public class PigFunction {

	/**
	 * Este metodo ejecuta codigo Pig Latin embebido en Java, de tal forma que recopile los
	 * distintos individuos sub-optimos que generen los "reduce" locales (que estaran almacenados
	 * en un formato de documento de texto) y aplique sobre ellos las operaciones que necesitemos
	 * (merge, order, filter y select -en principio...). Ademas le tendra que enviar el fichero
	 * resultante con la poblacion optima de la iteracion al Coordinador, para que este se la 
	 * pase al master y comience una nueva iteracion
	 * @author Alberto Luengo Cabanillas
	 *
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
	    	System.err.println("COORDINADOR["+iteration+"]:Se ha producido un error borrando el dir de salida de Pig");
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
