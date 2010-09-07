package common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import common.Chromosome;

public class StringChromosome extends Chromosome implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public StringChromosome(){}
  	
	@Override
	public String generate(int length) {
		String individuo = "";
		int i=0;
		//char spanish_chars[]={'!','.','?',' ',',',';'};
		//Arrays.sort(spanish_chars);
		Random r = new Random(System.nanoTime());
		int position = 0;
			while ( i < length){
				char c = (char)r.nextInt(255);
				//position = Arrays.binarySearch(spanish_chars, c);
				if((c>='0' && c<='9') || (c>='a' && c<='z') || (c>='A' && c<='Z')) 
				{
					individuo += c;
					i ++;
				}
			}
		return individuo;
	}
}
