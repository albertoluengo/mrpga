**************************** MapReduce Environment For Genetic Algorithms ***********************

Author: Alberto Luengo Cabanillas (aluengocabanillas@gmail.com)

This package contains the source code for MapReducing Simple Algorithms.

Requirement: Hadoop 0.20.1 or higher (and it's dependencies)

###################################################################################
To compile the class files and the jar files, run

$ ant compile jar

from this directory. This will create a build and bin directory for the class files, by compiling 
against Hadoop 0.20.1 jar and Pig 0.4.0 located in the "lib.hadoop.pig.dir" directory. Also, it will create 
mrpga.jar with MapReduced simple genetic algorithms.

###################################################################################
To execute, go to the root Hadoop directory and run, 

$ hadoop jar mrpga.jar <mapperName> <reducerName> <xmlName> <nReducers> <nIterations> <sizePop> <geneNumber> <chromKind> <crossProb> <boolElit> <mutation> <mutationRate> <tournWindow> <debug> <endCriterial> <user_dir>

where: 
***<mapperName> Mapper class name of problem to solve,
***<reducerName> Reducer class name of problem to solve, 
***<xmlName> XML config file name, 
***<nReducers> number of reducer tasks to use,
***<nIterations> number of iterations that system will perform,
***<sizePop> is the number of individuals in each population, 
***<geneNumber> corresponds to the length of each individual,
***<chromKind> type of chromosomes representation (actually 'binary' or 'other'), 
***<crossProb> is the probability of crossing individuals, 
***<boolElit> indicates if problem uses elitism, 
***<mutation> indicates if problem uses individuals mutation,
***<mutationRate> is the probability of mutating an individual,
***<tournWindow> number of individuals participating in the selection tournament,
***<debug> keeps a list with old populations already processed, 
***<endCriterial> indicates the way of terminating the algorithm (0-->by iterations, 1-->by target) and, finally,
***<user_dir> appropiate directory where temporal I/O will be stored. 

For example, for 'TargetPhrase' problem, we could execute:
$ hadoop jar mrpga.jar TargetPhraseMapper TargetPhraseReducer TargetPhrase 1 1 512 16 other 0.6 1 1 0.12 5 1 0 ./

###################################################################################