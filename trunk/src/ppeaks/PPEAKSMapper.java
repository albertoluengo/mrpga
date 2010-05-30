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

/**
 * 
 * Solution fitness ===> 1.0	(minimum fitness is 0.0)
 * Parameter ranges ===> 0 and 1
 * GOAL: locate a peak from P peaks
 *
 */
public class PPEAKSMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	//Representacion del individuo...
	private Text subjectAsWord = new Text();
	private int numElemProcessed = 0;
	private String USERNAME;
	private Hashtable mapParameters = new Hashtable();
	private Random r;
	
	// Numero de picos
	private static int peaks_number = 500;
	// Vector de picos
	private static short peak[][];
	// Longitud de los cromosomas...
	private int gene_length;
	//Contadores
    private int i, peaks, aux;
    
    PPEAKSMapper() {
		r = new Random(System.nanoTime());
	}
	
	
	private DoubleWritable calculateFitness(String individual) {
		double fitness = 0.0;
	    //Bits en comun con el pico mas cercano
	    int nearest_peak = 0;
		//Para cada pico...
	    for(peaks=0; peaks<peaks_number; peaks++)
	    {
	      //...calculamos la distancia Hamming...
	      for(aux=0,i=0;i<individual.length();i++) 
	    	  if(peak[peaks][i]!=Integer.parseInt(individual.charAt(i)+""));	
	    		  aux++;

	      if((gene_length-aux)>nearest_peak)	
	    	  nearest_peak = gene_length-aux;
	    }

	    fitness = (double)nearest_peak / (double)gene_length;
		return new DoubleWritable(fitness);
	}
	
	
	@Override
	protected void setup(Context cont)throws IOException {
		//LOG.info("***********DENTRO DEL SETUP DEL MAPPER**********");
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
		
		gene_length = Integer.parseInt((String)mapParameters.get("gene_length")); 
		 
		//Creo los picos una unica vez...
    	peak = new short[peaks_number][gene_length];
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
	
	/**Una vez todos los elementos hayan sido procesados, escribimos en un
	 * fichero global el mejor de ellos (si queremos introducir elitismo)...
	 */
	public void closeAndWrite(int debug,Text bestIndiv, double bestFitness) throws IOException {
		String bestDir = "/user/"+USERNAME+"/bestIndividuals";
		String bestFile = bestDir+"/bestIndiv.txt";
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