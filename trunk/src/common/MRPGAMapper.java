package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public abstract class MRPGAMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	private Text subjectAsWord = new Text();
	private String USERNAME = "";
	private Hashtable mapParameters = new Hashtable();
	private Hashtable problemConfigParameters = new Hashtable();
	private int numElemProcessed = 0;
	private static final Log LOG = LogFactory.getLog(MRPGAMapper.class.getName());
	

	public MRPGAMapper() {}
	
	
	public abstract void problemSetup(Hashtable configParams, Hashtable mappersParams);
	/**
	 * M&#233;todo <code>override</code> que se ejecutar&#225; una &#250;nica vez en el sistema
	 * que servir&#225; para leer y parsear los par&#225;metros de configuraci&#243;n necesarios
	 * para los nodos <code>Mapper</code>.
	 * @param cont Contexto en el que se ejecuta el trabajo <code>MapReduce</code>.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 */
	@Override
	public void setup(Context cont)throws IOException {
		
		Configuration conf = cont.getConfiguration();
		FileSystem hdfs = FileSystem.get(conf);
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		USERNAME = commas[0];
		String HDFS_GENERAL_PARAMS_FILE="/user/"+USERNAME+"/data/general_params.dat";
		String HDFS_PROBLEM_PARAMS_FILE="/user/"+USERNAME+"/data/problem_params.dat";
		Path path = new Path(HDFS_GENERAL_PARAMS_FILE);
		Path problemParamsPath = new Path(HDFS_PROBLEM_PARAMS_FILE);
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			throw new IOException("El fichero especificado " +HDFS_GENERAL_PARAMS_FILE + " no existe");
		}
		
		if (!hdfs.isFile(path))
		{
			throw new IOException("El fichero especificado "+HDFS_GENERAL_PARAMS_FILE + " no existe");
		}
		
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(problemParamsPath))
		{
			throw new IOException("El fichero especificado " +HDFS_PROBLEM_PARAMS_FILE + " no existe");
		}
		
		if (!hdfs.isFile(problemParamsPath))
		{
			throw new IOException("El fichero especificado "+HDFS_PROBLEM_PARAMS_FILE + " no existe");
		}
		
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		String strLine;
		String[]keys = {"numReducers", "numIterations","numPopulation", 
				"popPerMapper","gene_length","chromKind","boolElit", 
				"mutation", "mutationRate", "crossProb", "tournWindow", "debugging", "endCriterial",
				"userDir","userName"};
		int index=0;
		 while ((strLine = br.readLine()) != null)   {
			String lineValue = strLine.substring(strLine.indexOf(':')+1,strLine.length()); 
			mapParameters.put(keys[index], lineValue);
	        index++;
	      }
		 dis.close();
		
		 FSDataInputStream dis2 = hdfs.open(problemParamsPath);
			BufferedReader br2 = new BufferedReader(new InputStreamReader(dis2));
			int numLine=0;
			 while ((strLine = br2.readLine()) != null)   {
				String lineKey = strLine.substring(0,strLine.indexOf(':'));
				String lineValue = strLine.substring(strLine.indexOf(':')+1,strLine.length());
				problemConfigParameters.put(lineKey, lineValue);
		        numLine++;
		      }
			 dis2.close();
			 
		//Miramos si es necesario realizar alguna inicialización específica del problema
		problemSetup(problemConfigParameters,mapParameters);
		 
	}
	
	public abstract DoubleWritable calculateFitness(Hashtable configParams, Hashtable mappersParams, Text individual);
	
	/**
	 * MM&#233;todo <code>override</code> que "mapea" los distintos individuos
	 * para generar una lista de pares (clave,valor) que leer&#225;n posteriormente
	 * los nodos <code>Reducer</code>
	 * @param key La clave del par (clave,valor) que genera este m&#233;todo.
	 * @param value Objeto que representa al individuo de una poblaci&#243;n.
	 * @param context Contexto en el que se ejecuta el trabajo <code>MapReduce</code>.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 * @throws InterruptedException Excepción propia de <code>Hadoop</code> que se lanza si se interrumpe alguna transacci&#243;n at&#243;mica.
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		int numPop = Integer.parseInt((String)mapParameters.get("numPopulation"));
		int boolElit = Integer.parseInt((String)mapParameters.get("boolElit"));
		int debug = Integer.parseInt((String)mapParameters.get("debugging"));
		StringTokenizer itr = new StringTokenizer(line);
		String bestCritFitness = problemConfigParameters.get("bestFitness").toString();
		Text bestIndiv = new Text();
		double bestFitness = 0;
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			DoubleWritable elemFitness = calculateFitness(problemConfigParameters, mapParameters, subjectAsWord);
			
			//Seguimos la pista del mejor elemento...
			if (bestCritFitness.equals("minor")) {
				bestFitness = 999999; 
				if (elemFitness.get() < bestFitness) {
					bestFitness = elemFitness.get();
					bestIndiv = subjectAsWord;
					}
			}
			else {
				bestFitness = -999999; 
				if (elemFitness.get() > bestFitness) {
					bestFitness = elemFitness.get();
					bestIndiv = subjectAsWord;
					}
			}
			context.write(subjectAsWord, elemFitness);
			numElemProcessed++;
			
			if ((numElemProcessed == numPop -1)&&(boolElit==1)) {
				closeAndWrite(debug,bestIndiv,bestFitness);
			}
		}
	}
	
	/**
	 * M&#233;todo que, una vez todos los elementos hayan sido procesados, 
	 * escribe en un fichero global el mejor de ellos (si se ha elegido
	 * introducir elitismo).
	 * @param debug N&#250;mero entero (1-->"S&#237;", 0-->"No") que indica si interesa guardar un hist&#243;rico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param bestIndiv Texto que representa al mejor individuo encontrado en una poblaci&#243;n por un <code>Mapper</code>, seg&#250;n los criterios del problema concreto.
	 * @param bestFitness N&#250;mero que representa al mejor fitness encontrado en una poblaci&#243;n por un <code>Mapper</code>, seg&#250;n los criterios del problema concreto.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 */
	public void closeAndWrite(int debug,Text bestIndiv, double bestFitness) throws IOException {
		String bestDir = "/user/"+USERNAME+"/bestIndividuals";
		String bestFile = bestDir+"/bestIndiv.dat";
		Path bestDirPath = new Path(bestDir);
		Path bestIndivPath = new Path(bestFile);
		FileSystem hdfs = FileSystem.get(new Configuration());
		/**
		 * HDFS no permite que multiples mappers escriban en el mismo fichero,
		 * por lo que que creamos uno por cada mapper...
		 */
		if (hdfs.exists(bestDirPath)) {
    		//Eliminamos el directorio de los mejores individuos primero...
    		hdfs.delete(bestDirPath,true);
    	}
		FSDataOutputStream dos = hdfs.create(bestIndivPath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
		/**
		 * Escribo el valor del individuo y su fitness, para que luego el Reducer lo lea...
		 */
		bw.write(bestIndiv.toString()+"\n");
		bw.write(bestFitness+"\n");
		bw.close();
	}	
}
