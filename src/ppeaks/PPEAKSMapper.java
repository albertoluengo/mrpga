package ppeaks;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
	
	//Length of chromosome...
	private static int N;
	//Number of Peaks...
	private static int P = 1000;
	//The peaks...
	private static short peak[][];		
	private Text subjectAsWord = new Text();
	private int boolElit = 1;
	private int debug = 1;
	String USERNAME = this.getUserName();
	final String BEST_INDIVIDUAL_FILE = "/user/"+USERNAME+"/bestIndividuals/bestIndiv.txt";
	Path bestIndivPath = new Path(BEST_INDIVIDUAL_FILE);
	//Indicamos el fichero de configuracion que debe leer...
	final String HDFS_MAPPER_CONFIGURATION_FILE="/user/"+USERNAME+"/hadoop-user/data/mapper_configuration.dat";
	
	private String getUserName() {
		Configuration conf = new Configuration();
		String userName = conf.get("eclipse.plug-in.user.name");
		return userName;
	}

	private DoubleWritable calculateFitness(String indiv, int cont) {	
		double fitness = 0.0;		// Fitness of the individual
	    int    nearest_peak;		// Bits in common with nearest peak
	    int    j, p, hd;			// Counters
	    long milis = new java.util.GregorianCalendar().getTimeInMillis();
		Random r = new Random(milis);
	    

	    if(cont==1) 	// Only the first time create the peaks
	    {
	      N = indiv.length();
	      peak = new short[P][N];  	// GET MEMORY
	      for(p=0;p<P;p++)
	      {
	        for(j=0;j<N;j++)
	        if(r.nextDouble()<0.5)	peak[p][j] = 1;
	        else	peak[p][j] = 0;
	      }
	    }
	    
	    nearest_peak = 0;

	    for(p=0; p<P; p++)	// For every peak do
	    {
	      // Calculamos la distancia Hamming...
	      for(hd=0,j=0;j<N;j++) if(peak[p][j]!=(int)indiv.charAt(j)) hd++;

	      if((N-hd)>nearest_peak)	nearest_peak = N-hd;
	    }

	    fitness = (double)nearest_peak / (double)N;
	    return new DoubleWritable(fitness);
	}

	@Override
	protected void setup(Context cont)throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Path path = new Path(HDFS_MAPPER_CONFIGURATION_FILE);
		//Validamos primero el path de entrada antes de leer del fichero
		if (!hdfs.exists(path))
		{
			throw new IOException("El fichero especificado " +HDFS_MAPPER_CONFIGURATION_FILE + "no existe");
		}
		
		if (!hdfs.isFile(path))
		{
			throw new IOException("El fichero especificado "+HDFS_MAPPER_CONFIGURATION_FILE + "no existe");
		}

		
	}
	

	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String individual = value.toString();
		StringTokenizer itr = new StringTokenizer(individual);
		double bestFitness = 1.0;
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Text bestIndiv = new Text();
		int cont = 0;
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			DoubleWritable elemFitness = calculateFitness(individual,cont);
			context.write(subjectAsWord, elemFitness);
		
		/**Al acabar de procesar todos los elementos, escribimos en un fichero global 
		 * del DFS el mejor descendiente obtenido
		 */
		if (boolElit == 1) {
			//Buscamos el mejor elemento
			if (elemFitness.get() < bestFitness) {
				bestFitness = elemFitness.get();
				bestIndiv = subjectAsWord;
				}
			FSDataOutputStream dos = hdfs.create(bestIndivPath, true);
			/**Si el valor para debug que leemos del fichero es true, creamos un fichero
			 * nuevo cada iteración, si no lo sobreescribimos (overwrite=true)
			 */
			if (debug==0) {
				hdfs.delete(bestIndivPath,true);
				dos = hdfs.create(bestIndivPath, true);
				}
			dos.writeChars(bestIndiv.toString());	
			dos.close();
			}
		//Aumentamos el contador...
		cont++;
		}
	}
}