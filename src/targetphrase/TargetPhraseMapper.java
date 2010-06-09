package targetphrase;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Clase que implementa todas las funciones necesarias de un nodo <code>Mapper</code> 
 * en un trabajo <code>MapReduce</code>: se encargar&#225; de evaluar el "fitness" de cada individuo,
 * as&#237; como de generar los distintos pares <clave, fitness> necesarios para que los
 * nodos <code>Reducer</code> los puedan procesar. 
 * @author Alberto Luengo Cabanillas
 */
public class TargetPhraseMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text subjectAsWord = new Text();
	private String USERNAME = "";
	private Hashtable mapParameters = new Hashtable();
	private int numElemProcessed = 0;
	private static final Log LOG = LogFactory.getLog(TargetPhraseMapper.class.getName());
	
	/**
	 * M&#233;todo que calcula el "fitness" de cada individuo. En el caso del problema
	 * <code>TargetPhrase</code> consistir&#225; en incrementarlo en funci&#243;n de la
	 * diferencia entre cada uno de los caracteres del individuo y de la frase
	 * objetivo.
	 * @param individual Individuo a procesar
	 * @return Valor num&#233;rico con precisi&#243;n <code>double</code> que representa el fitness del individuo.
	 */
	private IntWritable calculateFitness(String target, Text individual) {
		int targetSize=target.length();
		String textAsString = individual.toString();
		int fitness=0;
		for (int j=0; j<targetSize; j++) {
			fitness += Math.abs((textAsString.toCharArray()[j] - target.charAt(j)));
		}
		return new IntWritable(fitness);
	}
	
	/**
	 * M&#233;todo <code>override</code> que se ejecutar&#225; una &#250;nica vez en el sistema
	 * que servir&#225; para leer y parsear los par&#225;metros de configuraci&#243;n necesarios
	 * para los nodos <code>Mapper</code>.
	 * @param cont Contexto en el que se ejecuta el trabajo <code>MapReduce</code>.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 */
	@Override
	protected void setup(Context cont)throws IOException {
		Configuration conf = cont.getConfiguration();
		FileSystem hdfs = FileSystem.get(conf);
		String users = conf.get("hadoop.job.ugi");
		String[] commas = users.split(",");
		USERNAME = commas[0];
		String HDFS_MAPPER_CONFIGURATION_FILE="/user/"+USERNAME+"/data/mapper_configuration.dat";
		Path path = new Path(HDFS_MAPPER_CONFIGURATION_FILE);
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			throw new IOException("El fichero especificado " +HDFS_MAPPER_CONFIGURATION_FILE + " no existe");
		}
		
		if (!hdfs.isFile(path))
		{
			throw new IOException("El fichero especificado "+HDFS_MAPPER_CONFIGURATION_FILE + " no existe");
		}
		
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));
		String strLine;
		String[]keys = {"targetPhrase","numPopulation","debugging","elitism","gene_length"};
		int index=0;
		 while ((strLine = br.readLine()) != null)   {
			mapParameters.put(keys[index], strLine);
	        index++;
	      }
		 dis.close();
		 
	}
	
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
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		int numPop = Integer.parseInt((String)mapParameters.get("numPopulation"));
		String targetPhrase = (String)mapParameters.get("targetPhrase");
		int boolElit = Integer.parseInt((String)mapParameters.get("elitism"));
		int debug = Integer.parseInt((String)mapParameters.get("debugging"));
		StringTokenizer itr = new StringTokenizer(line);
		int bestFitness = 999999; 
		Text bestIndiv = new Text();
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			IntWritable elemFitness = calculateFitness(targetPhrase, subjectAsWord);
			
			//Seguimos la pista del mejor elemento...
			if (elemFitness.get() < bestFitness) {
				bestFitness = elemFitness.get();
				bestIndiv = subjectAsWord;
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
	 * @param debug N&#250;mero entero (1-->"S&#237;	", 0-->"No") que indica si interesa guardar un hist&#243;rico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param bestIndiv Texto que representa al mejor individuo encontrado en una poblaci&#243;n por un <code>Mapper</code>, seg&#250;n los criterios del problema concreto.
	 * @param bestFitness N&#250;mero que representa al mejor fitness encontrado en una poblaci&#243;n por un <code>Mapper</code>, seg&#250;n los criterios del problema concreto.
	 * @throws IOException Excepci&#243;n que se lanza si ha habido alg&#250;n error manipulando ficheros o directorios.
	 */
	public void closeAndWrite(int debug,Text bestIndiv, int bestFitness) throws IOException {
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
