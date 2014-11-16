class=Calculator

#hadoop fs -rm -r /movie/*
#hadoop jar dest/${class}.jar hadoop.group.${class} /ml-1m/r1.train /movie/output

hadoop fs -rm -r /movie/output2
hadoop jar dest/${class}.jar hadoop.group.${class} /ml-10M100K/movies.dat /movie/output2
