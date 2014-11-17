#class=Initialize
#
#hadoop fs -rm -r /movies/one_step1
#hadoop jar dest/${class}.jar hadoop.group.${class} /movie/ratings.dat /movies/one_step1

class=Calculator
hadoop fs -rm -r /movies/one_step2
hadoop jar dest/${class}.jar hadoop.group.${class} /movie/movies.dat /movies/one_step2
