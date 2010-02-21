package fuentes;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class OneMAXMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	//Palabra que representa al individuo...
	private Text subjectAsWord = new Text();
	private int boolElit = 1;
	private int debug = 0;
	final String BEST_INDIVIDUAL_FILE = "/user/hadoop-user/bestIndividuals/bestIndiv.txt";
	Path bestIndivPath = new Path(BEST_INDIVIDUAL_FILE);
	//Indicamos el fichero de configuracion que debera leer
	final String HDFS_MAPPER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	
	
	
	private DoubleWritable calculateFitness(String individual) {		
		double fitness = 0.0;
		for (int i=0; i<individual.length(); i++) {
			if (individual.charAt(i)=='1')
				fitness += 1.0;
		}
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
		FileSystem hdfs = FileSystem.get(new Configuration()); 
		Text bestIndiv = new Text();
		double bestFitness = 0;
		
		
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			DoubleWritable fitness = calculateFitness(individual);
			context.write(subjectAsWord, fitness);
		
		/**Al acabar de procesar todos los elementos, escribimos en un fichero global 
		 * del DFS el mejor descendiente obtenido
		 */
		if (boolElit == 1) {
			//Buscamos el mejor elemento
			if (fitness.get() > bestFitness) {
				bestFitness = fitness.get();
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
		}
	}
}

