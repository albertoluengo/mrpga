package common;

/**
 * Clase abstracta que declara las operaciones más comunes que tendrán los individuos
 * de una población
 * @author Alberto Luengo Cabanillas
 */
public abstract class Chromosome {

	public Chromosome(){}
	
	public abstract String generate(int length);
}
