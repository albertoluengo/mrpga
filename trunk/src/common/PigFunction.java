package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
		int iteration = Integer.parseInt(args[0]);
		int numReducers = Integer.parseInt(args[1]);

		FileSystem fs = FileSystem.get(conf);
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		String userName = commas[0];
		String pigResultsDir = "/user/"+userName+"/pigResults";
		String pigTempResultsDir = "/user/"+userName+"/pigTempResults";
		String pigResultsFile = pigResultsDir+"/pigResults.dat";
		
		Path resultTempPath = new Path(pigTempResultsDir);
		Path resultPath = new Path(pigResultsDir);
		Path resultsFilePath = new Path(pigResultsFile);
		FSDataOutputStream dos = fs.create(resultsFilePath, true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
		String strLine;
	    
	    String popIterationName = "input/population_"+iteration+".dat";
		PigServer pigServer = new PigServer("mapreduce");
		
		String inputFiles = "output/part-r-*";
		
		pigServer.registerQuery("raw_data = load '"+inputFiles+"' using PigStorage('\t');");
		pigServer.registerQuery("B = foreach raw_data generate $0 as id;");
		pigServer.registerQuery("store B into 'pigTempResults';");
		
		for (int numRed=0;numRed<numReducers; numRed++)
		{
			Path partialPopPath = new Path("pigTempResults/part-0000"+numRed);
			FSDataInputStream dis = fs.open(partialPopPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(dis));
			while ((strLine = br.readLine()) != null)   {
				bw.write(strLine+"\n");
			}
			dis.close();
		}
		bw.close();
		
		pigServer.renameFile(pigResultsFile, popIterationName);
		
		//Cuando acabo, borro el contenido de los dir 'pigResults'
		fs.delete(resultPath,true);
		fs.delete(resultTempPath,true);

	}

}
