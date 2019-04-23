export DIR=wordcount_classes
rm -fR $DIR
mkdir $DIR
javac -classpath /opt/mapr/hadoop/hadoop-2.7.0/lib/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/mapreduce/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/* -d $DIR *.java 
jar -cvf wordcount.jar -C $DIR .
