package common;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * Interfaz que declara los m&#233;todos que ejecutar&#225; la clase <code>Coordinador.java</code>
 * @author Alberto Luengo Cabanillas
 */
public interface ICoordinador {

	/**
	 * M&#233;todo principal de la clase <code>Coordinador</code> que realizar&#225; las
	 * iteraciones indicadas por la clase <code>Cliente</code> y lanzar&#225;
	 * trabajos <code>MapReduce</code> sobre <code>Hadoop</code> y <code>Pig</code> para procesar las distintas
	 * poblaciones que se vayan generando, hasta obtener un resultado.
	 * @param numpop Tama&#241;o (entero) de la poblaci&#243;n a procesar.
	 * @param numReducers N&#250;mero de tareas <code>Reducer</code> que lanzar&#225; el trabajo <code>MapReduce</code>
	 * @param maxiter N&#250;mero m&#225;ximo de iteraciones por las que va a atravesar el sistema.
	 * @param debug N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si interesa guardar un hist&#243;rico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param boolElit N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si se introduce elitismo o no en la generaci&#243;n de descendencia.
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @param endCriterial N&#250;mero entero (0-->"Por Iteraciones", 1-->"Por Objetivo") que indica la forma de terminaci&#243;n del algoritmo.
	 * @param gene_number Longitud de los individuos de las poblaciones a procesar (no aplicable al problema <code>TargetPhrase</code>).
	 * @param configValues Tabla Hash con los valores de configuracion del problema cargado.
	 * @param userDir Directorio personal del usuario
	 * @return Cadena de texto con el/los mejor(es) individuo(s) encontrado(s).
	 * @throws IOException Excepci&#243;n que se lanza al manipular ficheros o directorios.
	 * @throws ExecException Excepci&#243;n que lanza un script <code>Pig</code> si encuentra alg&#250;n problema en su ejecuci&#243;n.
	 * @throws Exception Excepci&#243;n padre gen&#233;rica de <code>Java</code>
	 */
	public String readFromClientAndIterate(int numpop, int numReducers, int maxiter, int debug, int boolElit, String problemName, String reducerName, int endCriterial, int gene_number, Hashtable configValues, String userDir)throws Exception;
	
	/**
	 * M&#233;todo que copia los ficheros de configuraci&#243;n y poblacionales generados 
	 * en la clase <code>Cliente.java</code> a los directorios especificados del
	 * HDFS.
	 * @param cont Instancia de <code>JobContext</code> que recoge 
	 * el contexto del trabajo MapReduce a ejecutar.
	 * @param population Ruta del sistema de ficheros anfitri&#243;n 
	 * en el que se ha generado la poblaci&#243;n inicial.
	 * @param boolElit N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si se introduce elitismo o no en la generaci&#243;n de descendencia.
	 * @param userDir Directorio personal del usuario
	 * @throws IOException Excepci&#243;n que se lanza al manipular ficheros o directorios.
	 */
	public void uploadToHDFS(JobContext cont, String population, int boolElit, String userDir) throws IOException;
	
	/**
	 * M&#233;todo que lee el fichero de poblaci&#243;n que existe en el HDFS y 
	 * lo reemplaza por el de su descendencia antes de entrar en la 
	 * siguiente iteraci&#243;n.
	 * @param original Ruta a la poblaci&#243;n "padre" dentro del HDFS.
	 * @param actual Ruta a la poblaci&#243;n "actual" dentro del HDFS.
	 * @throws IOException Excepci&#243;n lanzada si existe alg&#250;n problema manipulando directorios o ficheros.
	 */
	public void replacePopulationFile(Path original, Path actual) throws IOException;
	
	/**
	 * M&#233;todo que lee los registros de una tabla Hash y los procesa para buscar
	 * los mejores individuos, seg&#250;n el criterio establecido por <code>numProblem</code>.
	 * @param hashTable Tabla hash que contiene todos los pares "individuo-fitness" de la poblaci&#243;n actual.
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @return Cadena de texto con los mejores individuos encontrados.
	 */
	public String printBestIndividual(Hashtable hashTable, String bestFitness);
	
	/**
	 * M&#233;todo que lee un fichero y parsea su contenido como una <code>HashTable</code> con el
	 * fin de facilitar su posterior procesamiento.
	 * @param numReducers Número de tareas reduce a lanzar.
	 * @param numProblem N&#250;mero que indica el n&#250;mero de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @return Una tabla Hash con todos los elementos de la actual poblaci&#243;n en forma "individuo-fitness".
	 * @throws IOException Excepci&#243;n lanzada si existe alg&#250;n problema manipulando directorios o ficheros.
	 */
	public Hashtable generateIndividualsTable(int numReducers)throws IOException;
}
