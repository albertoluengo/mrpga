package fuentes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.Reporter;

/**
 * Los nodos reducer ejecutar�n la funci�n reduce en funci�n de las entradas que obtengan,
 * generalmente, de la m�quina local. En caso de que no est� la carga balanceada, algunos
 * nodos reducer podr�n obtener los resultados intermedios de m�quinas "vecinas". La funci�n
 * "reduce" definida por el usuario, en este caso generar� la descendencia y la almacenar� 
 * de forma local...(habr� que encontrar una manera de mand�rselos al coordinador para que los 
 * procese...)
 * 
 *
 */
public class HWorldReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private Text keyword = new Text();
	private int fitnessValue = 0;
	private IntWritable result = new IntWritable();
	private long milis = new java.util.GregorianCalendar().getTimeInMillis();
	Random r = new Random(milis);
	HashSet reducerParams = new HashSet();
    SequenceFile.Writer writer = null;
	//RecordWriter recordWriter= null;
	
	//Indicamos el fichero de configuracion que debera leer
	final String HDFS_REDUCER_CONFIGURATION_FILE="/user/hadoop-user/data/reducer_configuration.dat";

	private void elitism(String population, String subOptimal, int elitSize) {
		for (int i=0; i<elitSize; i++) {
			subOptimal = population;
		}
	}
	
	/*//Cambiamos un miembro por otro de la poblacion...
	private void mutate(String word, String target)
	{
		int targetSize = target.length();
		int ipos = r.nextInt(1000) % targetSize;
		int delta = (r.nextInt(1000) % 90) + 32; 
		target= ((target + delta) % 122);
	}*/
	
	private void mate(String population, String subOptimal, float mutation, int popSize,float elitRate, String target) {
		int elitSize = (int)(popSize * elitRate);
		int targetSize = target.length(); 
		
		//Nos quedamos con los mejores...
		elitism(population,subOptimal,elitSize);
		
		//Emparejamos el resto...
		// Mate the rest
		for (int i=elitSize; i<popSize; i++) {
			int i1 = r.nextInt(1000) % (popSize / 2);
			int i2 = r.nextInt(1000) % (popSize / 2);
			int spos = r.nextInt(1000) % targetSize;

			subOptimal = population.substring(0, spos) + 
			population.substring(spos, elitSize - spos);

			//TODO: DESARROLLAR LO DE LA MUTACION!!!!!!!!!!
			//if (r.nextInt(1000) < mutation) mutate(subOptimal);
		}
	}
		
	private void loadReducerConfiguration(Path cachePath) throws IOException {
		BufferedReader paramReader = new BufferedReader(new FileReader(cachePath.toString()));
		try {
			String line;
			while ((line = paramReader.readLine())!=null){
				this.reducerParams.add(line);
			}
			}finally {
				paramReader.close();
		}
	}
	
	/*private RecordWriter loadRecordWriter() throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"reducer");
		FileSystem fs = FileSystem.get(conf);
		Context context = new Context(conf,null, null, null, recordWriter, null, null, null, null, null);
		Path outDir = new Path("/user/hadoop-user", "output");
	    Path outFile = new Path(outDir, "suboptimal.txt");
		TextOutputFormat<Text, IntWritable> text = new TextOutputFormat<Text, IntWritable>();
		text.setOutputPath(job, outFile);
		RecordWriter<Text, IntWritable> recordWriter = text.getRecordWriter(context);
		return recordWriter;
	}*/
	
	private SequenceFile.Writer loadSequenceWriter() throws IOException {
		Configuration conf = new Configuration();
		JobContext jCont = new JobContext(conf, null);
		FileSystem fs = FileSystem.get(jCont.getConfiguration());	
	    Path outDir = new Path("/user/hadoop-user", "output");
	    Path outFile = new Path(outDir, "suboptimal.txt");
	    writer = SequenceFile.createWriter(fs, conf,
	        outFile, Text.class, IntWritable.class, 
	        CompressionType.NONE);
	    return writer;
	}
	
	@Override
	protected void setup(Context cont) throws IOException{
		writer = this.loadSequenceWriter();
		try {
			String configureCacheName = new Path(HDFS_REDUCER_CONFIGURATION_FILE).getName();
			Path [] cacheFiles = DistributedCache.getLocalCacheFiles(cont.getConfiguration());
			if (null != cacheFiles && cacheFiles.length > 0) {
				for (Path cachePath: cacheFiles) {
					if (cachePath.getName().equals(configureCacheName)) {
						loadReducerConfiguration(cachePath);
						break;
					}
				}
			}
		} catch (IOException ioe) {
			System.err.println("REDUCER: IOException reading Reducer configuration file from distributed cache");
			System.err.println(ioe.toString());
		}
	}

	/*@Override
	protected void setup(Context cont) throws IOException, InterruptedException{
		recordWriter = this.loadRecordWriter();
		try {
			String configureCacheName = new Path(HDFS_REDUCER_CONFIGURATION_FILE).getName();
			Path [] cacheFiles = DistributedCache.getLocalCacheFiles(cont.getConfiguration());
			if (null != cacheFiles && cacheFiles.length > 0) {
				for (Path cachePath: cacheFiles) {
					if (cachePath.getName().equals(configureCacheName)) {
						loadReducerConfiguration(cachePath);
						break;
					}
				}
			}
		} catch (IOException ioe) {
			System.err.println("REDUCER: IOException reading Reducer configuration file from distributed cache");
			System.err.println(ioe.toString());
		}
	}*/
	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int fitness = 0;	
		for (IntWritable val : values) {
		      fitness += val.get();
		    }
		result.set(fitness);
		context.write(key, result);
		//recordWriter.write(key, result);
		writer.append(new Text(key), new IntWritable(result.get()));
	}
	
	/*@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int fitness = 0; 
		for (IntWritable val : values) {
		      fitness += val.get();
		    }
		result.set(fitness);
		writer.append(key,result);
	}*/

	/**Una vez se haya hecho el reduce, se deberan copiar los resultados a un fichero local
	 * para que el coordinador pueda leer dichos ficheros, y borrarlos para la siguiente iteracion... 
	 */
 
	/*@Override
    protected void cleanup(Context cont) throws IOException {
 
      //TERCERA FORMA
      //write output to a file
	  Configuration conf = new Configuration();
	  JobContext jCont = new JobContext(conf, null);
	  FileSystem fs = FileSystem.get(jCont.getConfiguration());	
      Path outDir = new Path("/user/hadoop-user/output", "output");
      Path outFile = new Path(outDir, "suboptimal.txt");
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
          outFile, Text.class, IntWritable.class, 
          CompressionType.NONE);
      writer.append(new Text(keyword), new IntWritable(fitnessValue));
      writer.close();
    }*/
	
	@Override
    protected void cleanup(Context cont) throws IOException,InterruptedException {
      writer.close();
    }
	
	
}
