VPATH=src:bin:dest

file=TestTest
#file=Initialize

${file}.jar: ${file}.java
	rm -rf bin/${file}
	mkdir bin/${file}
	javac -classpath \
		/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.0.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.4.0.jar -d bin/${file} src/${file}.java
	jar -cvf dest/${file}.jar -C bin/${file} .

clean:
	rm -rfv bin/* dest/*
