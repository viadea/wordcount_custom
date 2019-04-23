hadoop fs -rmr /hao/wordout
hadoop fs -rmr /hao/wordfinal
yarn jar wordcount.jar WordCount -Dyarn.app.mapreduce.am.log.level=DEBUG -Dmapreduce.map.log.level=DEBUG -Dmapreduce.reduce.log.level=DEBUG -Dmapreduce.job.reduces=3 -Dmapreduce.fileoutputcommitter.algorithm.version=2 /hao/input /hao/wordfinal 

