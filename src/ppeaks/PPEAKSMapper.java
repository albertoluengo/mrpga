package ppeaks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Hashtable;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import targetphrase.TargetPhraseMapper;


/**
 * Clase que implementa todas las funciones necesarias de un nodo <code>Mapper</code> 
 * en un trabajo <code>MapReduce</code>: se encargará de evaluar el "fitness" de cada individuo,
 * así como de generar los distintos pares <clave, fitness> necesarios para que los
 * nodos <code>Reducer</code> los puedan procesar. 
 * @author Alberto Luengo Cabanillas
 *
 */
public class PPEAKSMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	//Representacion del individuo...
	private Text subjectAsWord = new Text();
	private int numElemProcessed = 0;
	private String USERNAME;
	private Hashtable mapParameters = new Hashtable();
	private Random r;
	private static final Log LOG = LogFactory.getLog(PPEAKSMapper.class.getName());
	
	// Numero de picos
	private static int peaks_number = 200;
	// Vector de picos
	private static short peak[][];
	// Longitud de los cromosomas...
	private int gene_length;
    
    PPEAKSMapper() {
		r = new Random(System.nanoTime());
	}
	
    /**
	 * Método que calcula el "fitness" de cada individuo. En el caso del problema
	 * <code>PPEAKS</code> calculará el pico más cercano a dicho individuo, siendo
	 * el fitness óptimo a conseguir 1.0.
	 * @param individual Individuo a procesar
	 * @return Valor numérico con precisión <code>double</code> que representa el fitness del individuo.
	 */
	private DoubleWritable calculateFitness(String individual) {
		double fitness = 0.0;
	    //Bits en comun con el pico mas cercano
	    double nearest_peak = 999.0;
	    int i = 0, peaks = 0, distHamming = 0;
	    double currentDistance = 0.0;
	    double []distances = new double[peaks_number];
	    //LOG.info("PEAKS NUMBER ES "+peaks_number);
	    //LOG.info("GENE LENGTH ES "+gene_length);
	    for(peaks=0; peaks<peaks_number; peaks++)
	    {
	      //...calculamos la distancia Hamming...
	      distHamming = 0;	
	      for(int pos=0;pos<gene_length;pos++)
	      {
	    	  short current_peak = peak[peaks][pos];
    		  if(current_peak!=Integer.parseInt(individual.charAt(pos)+""))
    			  distHamming++;
	      }
	      distances[peaks] = distHamming;
	      //LOG.info("DISTANCE["+peaks+"] VALE "+distances[peaks]);
	    }
	    
	    //Buscamos ahora el valor mas pequeño...
	    for (i=0;i<distances.length;i++)
	    {
	    	currentDistance = distances[i];
	    	//LOG.info("LA DISTANCIA ACTUAL ES "+currentDistance);
	    	//LOG.info("NEAREST PEAK VALE "+nearest_peak);
	    	if (currentDistance < nearest_peak)
	    		nearest_peak = currentDistance;
	    }
	    //LOG.info("MAPPER: EL PICO MAS CERCANO ES "+nearest_peak);
	    fitness = (double)(nearest_peak / (double)individual.length());
	    //LOG.info("MAPPER: EL FITNESS DEL INDIVIDUO ES "+fitness);
	    
		return new DoubleWritable(fitness);
	}
	
	
	@Override
	protected void setup(Context cont)throws IOException {
		LOG.info("***********DENTRO DEL SETUP DEL MAPPER**********");
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
		String[]keys = {"targetPhrase","numPopulation","elitism","debugging","gene_length"};
		int index=0;
		 while ((strLine = br.readLine()) != null)   {
			mapParameters.put(keys[index], strLine);
	        index++;
	      }
		 dis.close();
		
		gene_length = Integer.parseInt((String)mapParameters.get("gene_length")); 
		 
		//Creo los picos una unica vez...
    	peak = new short[peaks_number][gene_length];
    	int peaks = 0, i = 0;
    	for(peaks=0;peaks<peaks_number;peaks++)
    	{
    		for(i=0;i<gene_length;i++)
    			if(r.nextDouble()<0.5)	
    				peak[peaks][i] = 1;
    			else	
    				peak[peaks][i] = 0;
    	}
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		int numPop = Integer.parseInt((String)mapParameters.get("numPopulation"));
		int boolElit = Integer.parseInt((String)mapParameters.get("elitism"));
		int debug = Integer.parseInt((String)mapParameters.get("debugging"));
		StringTokenizer itr = new StringTokenizer(line);
		double bestFitness = -999999; 
		Text bestIndiv = new Text();
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			DoubleWritable elemFitness = calculateFitness(subjectAsWord.toString());
//			LOG.info("MAPPER: EL INDIVIDUO ES "+subjectAsWord.toString());
//			LOG.info("MAPPER: EL FITNESS DEL INDIVIDUO ES "+elemFitness);
			//Seguimos la pista del mejor elemento...
			if (elemFitness.get() > bestFitness) {
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
	 * Método que, una vez todos los elementos hayan sido procesados, 
	 * escribe en un fichero global el mejor de ellos (si se ha elegido
	 * introducir elitismo).
	 * @param debug Número entero (1-->"Sí", 0-->"No") que indica si interesa guardar un histórico de poblaciones procesadas en un directorio 'oldPopulations' del HDFS.
	 * @param bestIndiv Texto que representa al mejor individuo encontrado en una población por un <code>Mapper</code>, según los criterios del problema concreto.
	 * @param bestFitness Número que representa al mejor fitness encontrado en una población por un <code>Mapper</code>, según los criterios del problema concreto.
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