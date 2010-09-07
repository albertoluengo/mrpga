package common;

import java.io.Serializable;
import common.Chromosome;

public class BinaryChromosome extends Chromosome implements Serializable {

	private static final long serialVersionUID = 1L;
	

	public BinaryChromosome(){}

	@Override
	public String generate(int length) {
		String individuo="";
		int cont=0;
		double prob =0.0;
		while (cont < length)
		{
			prob = java.lang.Math.random();
			if ((prob <= 0.5))
				individuo += '0';
			else
				individuo += '1';
			cont ++;
		}
		return individuo;
	}
	  
}