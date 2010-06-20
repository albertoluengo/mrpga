**************************** MapReduce Environment For Genetic Algorithms ***********************

Author: Alberto Luengo Cabanillas (aluengocabanillas@gmail.com)

This package contains the source code for MapReducing Simple Algorithms.

Requirement: Hadoop 0.20.1 or higher (and it's dependencies)

###################################################################################
To compile the class files and the jar files, run

$ ant compile jar

This will create a build and bin directory for the class files, by compiling 
against Hadoop 0.20.1 jar and Pig 0.4.0 located in the "lib.hadoop.pig" directory. Also, it will create 
mrpga.jar with MapReduces simple genetic algorithms.

###################################################################################
To execute, go to the root Hadoop directory and run, 

$ hadoop jar mrpga.jar <numProblem> <nReducers> <nIterations> <sizePop> <geneNumber> <crossProb> <boolElit> <mutation> <debug> <endCriterial> [<targetPhrase>]

where: 
***<numProblem> is the number of the problem to execute (1-->TargetPhrase, 2-->OneMax, 3-->PPeaks),
***<nReducers> is the number of Reducer tasks the MapReduce job will launch, 
***<nIterations> is the number of iterations the system will performance, 
***<sizePop> is the number of individuals in each population, 
***<geneNumber> corresponds to the length of each individual (set to target length in "TargetPhrase" problem), 
***<crossProb> is the probability of crossing individuals, 
***<boolElit> and <mutation> indicates if the problem uses elitism and mutation, 
***<debug> keeps a list with old populations already processed and, finally, 
***<endCriterial> indicates the way of terminating the algorithm (0-->by iterations, 1-->by target). 

Optionally, there´s the <targetPhrase> parameter that modifies the default target phrase ("Hello_world!") in TargetPhrase problem.


For example, for 'TargetPhrase' problem, we could execute:
$ hadoop jar mrpga.jar 1 1 1024 0 0.6 0 1 0 0 Good_bye!

For 'OneMAX' problem, we could execute:
$ hadoop jar mrpga.jar 2 1 1024 64 0.6 0 1 0 0

For 'PPEAKS' problem, we could execute:
$ hadoop jar mrpga.jar 3 1 1024 64 0.6 0 1 1 0

###################################################################################