all:
	javac -cp ${HADOOP_CLASSPATH} -d fwk FrameWork.java  TokenizerMapper.java IntSumReducer.java 
	jar -cvf fwk.jar -C fwk/ .
