package fuentes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;


/**
 *
 * Los nodos Mapper ejecutar�n la funci�n Map definida por el usuario y almacenar�n los
 * resultados de forma local. Dicha funci�n Map en el caso que nos ocupa, consistir� en
 * calcular el fitness de su trozo de poblacion, orden�ndolo.
 * 
 *
 */
//Mapper<Key In, Value In, Key Out, Value Out>
public class HWorldMapper extends Mapper<Object, Text, Text, IntWritable> {

	//Palabra que representa al descendiente...
	private Text subjectAsWord = new Text();
	private int fitness = 0;
	private FileSystem fs; // filesystem
	private Path [] cacheFiles;
	
	
	//Parametro de configuracion que necesita el Mapper (en este caso, la palabra objetivo)
	String targetPhrase = "";
	
	//Indicamos el fichero de configuracion que debera leer
	final String HDFS_MAPPER_CONFIGURATION_FILE="/user/hadoop-user/data/mapper_configuration.dat";
	
	private int calculateFitness(String target) {
		
		int targetSize=target.length();
		int fitness=0;
		for (int j=0; j<targetSize; j++) {
			fitness += Math.abs((subjectAsWord.charAt(j) - target.charAt(j)));
		}
		return fitness;
	}
	
	private void loadMapperConfiguration(Path cachePath) throws IOException {
		BufferedReader paramReader = new BufferedReader(new FileReader(cachePath.toString()));
		try {
			String line;
			while ((line = paramReader.readLine())!=null){
				targetPhrase=line;
			}
			}finally {
				paramReader.close();
		}
			System.out.println("MAPPER:La frase objetivo es: "+targetPhrase);
	}
	
	@Override
	protected void setup(Context cont) {
		try {
			//URI[] remoteCacheFiles = DistributedCache.getCacheFiles(cont.getConfiguration());
			//System.out.println("Los ficheros remotos son: "+remoteCacheFiles.toString());
			
			fs = FileSystem.getLocal(new Configuration());
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(fs.getConf());
			System.out.println("Los ficheros son: "+cacheFiles.toString());
			
			//Esta instruccion devuelve el nombre del fichero "mapper_conifguration.dat"
			String configureCacheName = new Path(HDFS_MAPPER_CONFIGURATION_FILE).getName();
			
			//Path [] cacheFiles = DistributedCache.getLocalCacheFiles(cont.getConfiguration());
			System.out.println("Los ficheros son: "+cacheFiles.toString());
			if (null != cacheFiles && cacheFiles.length > 0) {
				for (Path cachePath: cacheFiles) {
					if (cachePath.getName().equals(configureCacheName)) {
						loadMapperConfiguration(cachePath);
						break;
					}
				}
			}
		} catch (IOException ioe) {
			System.err.println("IOException reading Mapper file from distributed cache");
			System.err.println(ioe.toString());
		}
	}
	
	/*@Override
	protected void setup(Context cont) {
		try {				
		      fs = FileSystem.getLocal(new Configuration());
		      localFiles = DistributedCache.getLocalCacheFiles(cont.getConfiguration());
		      System.out.println("Los ficheros son: "+localFiles.toString());
		} catch (IOException ioe) {
			System.err.println("IOException reading Mapper file from distributed cache");
			System.err.println(ioe.toString());
		}
	}*/
	
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{

		// Abro el primer fichero en la cache distribuida en el sistema de ficheros local
	    FSDataInputStream localFile = fs.open(cacheFiles[0]);
	    System.out.println("MAPPER: La linea que estoy leyendo es: "+localFile.toString());
		String line = value.toString();
		//System.out.println("MAPPER: La linea que estoy leyendo es: "+line.toString());
		StringTokenizer itr = new StringTokenizer(line);
		while(itr.hasMoreTokens()) {
			subjectAsWord.set(itr.nextToken());
			//System.out.println("MAPPER: La palabra que estoy leyendo es: "+subjectAsWord.toString());
			fitness=this.calculateFitness(targetPhrase);
			context.write(subjectAsWord, new IntWritable(fitness));
		}
	}
	
	@Override
    protected void cleanup(Context cont) throws IOException {
	  //SequenceFile.Writer writer = this.loadSequenceWriter();
      fs.close();
    }
	
}
