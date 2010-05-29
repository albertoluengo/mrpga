package common;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.pig.backend.executionengine.ExecException;

/**Las funciones del coordinador seran las siguientes:
 * 1.- Recibir inicialmente la poblacion inicial del cliente (generada aleatoriamente), asi
 * 		como el numero de iteraciones que se va a realizar
 * 2.- Comunicarse con el HDFS del nodo master para pasarle la poblacion en cada iteracion
 * 		(sobreescribiendo la de la anterior iteracion)
 * 3.- Finalmente, recibir la descendencia filtrada por el nodo Pig de cada iteracion 
 * 		que conformara la entrada de la siguiente.
 * 3b.- Que sea el propio coordinador el que realice el script Pig
 * 4.- Realizar el calculo iterativo...
 *
 */

public interface ICoordinador {

	public String readFromClientAndIterate(int numpop, int maxiter, int debug, int boolElit, String numProblem, int endCriterial)throws Exception;
	public void uploadToHDFS(JobContext cont, String population) throws IOException;
	public void replacePopulationFile(Path original, Path actual) throws IOException;
	public void runPigScript(String filePath, int iterations, Configuration conf)throws ExecException, IOException;
	public String printBestIndividual(Hashtable hashTable);
	public Hashtable<String, Integer> generateIndividualsTable(Path filePath)throws IOException;
	public String readFromHDFS(String stringPath);
}
