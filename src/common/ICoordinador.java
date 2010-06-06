package common;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * Interfaz que declara los métodos que ejecutará la clase <code>Coordinador.java</code>
 * @author Alberto Luengo Cabanillas
 */
public interface ICoordinador {

	/**
	 * Método principal de la clase <code>Coordinador</code> que realizará las
	 * iteraciones indicadas por la clase <code>Cliente</code> y lanzará
	 * trabajos "MapReduce" sobre "Hadoop" y "Pig" para procesar las distintas
	 * poblaciones que se vayan generando, hasta obtener un resultado.
	 * @param numpop Tamaño (entero) de la población a procesar.
	 * @param maxiter Número máximo de iteraciones por las que va a atravesar el sistema.
	 * @param debug Número entero (1-->"Sí", 0-->"No") que indica si interesa guardar un histórico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param boolElit Número entero (1-->"Sí", 0-->"No") que indica si se introduce elitismo o no en la generación de descendencia.
	 * @param numProblem Número que indica el número de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @param endCriterial Número entero (0-->"Por Iteraciones", 1-->"Por Objetivo") que indica la forma de terminación del algoritmo.
	 * @param gene_number Longitud de los individuos de las poblaciones a procesar (no aplicable al problema <code>TargetPhrase</code>).
	 * @return Cadena de texto con el/los mejor(es) individuo(s) encontrado(s).
	 * @throws IOException Excepción que se lanza al manipular ficheros o directorios.
	 * @throws ExecException Excepción que lanza un script <code>Pig</code> si encuentra algún problema en su ejecución.
	 * @throws Exception Excepción padre genérica de <code>Java</code>
	 */
	public String readFromClientAndIterate(int numpop, int maxiter, int debug, int boolElit, String numProblem, int endCriterial, int gene_number)throws Exception;
	
	/**
	 * Método que copia los ficheros de configuración y poblacionales generados 
	 * en la clase <code>Cliente.java</code> a los directorios especificados del
	 * HDFS.
	 * @param cont Instancia de <code>JobContext</code> que recoge 
	 * el contexto del trabajo MapReduce a ejecutar.
	 * @param population Ruta del sistema de ficheros anfitrión 
	 * en el que se ha generado la población inicial.
	 * @param boolElit Número entero (1-->"Sí", 0-->"No") que indica si se introduce elitismo o no en la generación de descendencia.
	 * @throws IOException Excepción que se lanza al manipular ficheros o directorios.
	 */
	public void uploadToHDFS(JobContext cont, String population, int boolElit) throws IOException;
	
	/**
	 * Método que lee el fichero de población que existe en el HDFS y 
	 * lo reemplaza por el de su descendencia antes de entrar en la 
	 * siguiente iteración.
	 * @param original Ruta a la población "padre" dentro del HDFS.
	 * @param actual Ruta a la población "actual" dentro del HDFS.
	 * @throws IOException Excepción lanzada si existe algún problema manipulando directorios o ficheros.
	 */
	public void replacePopulationFile(Path original, Path actual) throws IOException;
	
	/**
	 * Método que lee los registros de una tabla Hash y los procesa para buscar
	 * los mejores individuos, según el criterio establecido por <code>numProblem</code>.
	 * @param hashTable Tabla hash que contiene todos los pares "individuo-fitness" de la población actual.
	 * @param numProblem Número que indica el número de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @return Cadena de texto con los mejores individuos encontrados.
	 */
	public String printBestIndividual(Hashtable hashTable, String numProblem);
	
	/**
	 * Método que lee un fichero y lo procesa como una <code>HashTable</code> con el
	 * fin de facilitar su posterior procesamiento.
	 * @param filePath Ruta en el HDFS del fichero a procesar.
	 * @param numProblem Número que indica el número de problema a ejecutar (1-->"TargetPhrase", 2-->"OneMAX", 3-->"PPEAKS").
	 * @return Una tabla Hash con todos los elementos de la actual población en forma "individuo-fitness".
	 * @throws IOException Excepción lanzada si existe algún problema manipulando directorios o ficheros.
	 */
	public Hashtable generateIndividualsTable(Path filePath, String numProblem)throws IOException;
}
