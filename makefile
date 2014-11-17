VPATH=src:bin:dest

step1=Initialize
step2=Calculator

${step2}.jar: ${step2}.java ${step1}.jar
	rm -rf bin/${step2}
	mkdir bin/${step2}
	javac -classpath \
		/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.0.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.4.0.jar -d bin/${step2} src/${step2}.java
	jar -cvf dest/${step2}.jar -C bin/${step2} .

${step1}.jar: ${step1}.java
	rm -rf bin/${step1}
	mkdir bin/${step1}
	javac -classpath \
		/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.0.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.4.0.jar -d bin/${step1} src/${step1}.java
	jar -cvf dest/${step1}.jar -C bin/${step1} .

clean:
	rm -rfv bin/* dest/*
