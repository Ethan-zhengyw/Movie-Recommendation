class=TestTest

hadoop fs -rm -r /movie/*
hadoop jar dest/${class}.jar hadoop.group.${class} /ml-1m/r1.train /movie/output
