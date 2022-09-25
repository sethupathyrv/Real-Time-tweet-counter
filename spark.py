from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests

from kafka import KafkaProducer
bootstrap_servers=['localhost:9092']


conf = SparkConf()
conf.setAppName("TwitterStream")
spc = SparkContext(conf=conf)
spc.setLogLevel("error")
stc = StreamingContext(spc, 2)
stc.checkpoint("checkpoints")
ld = stc.socketTextStream("localhost",9009)
dStream=ld.window(10,10)
#print(dataStream)
#file_object = open('sparkdata.txt', 'a')
def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
        	globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
        print("-.-.-.-.-.-.-.-.-.-.- %s --.-.-.-.-.-.-.-.-.-" % str(time))
        r = rdd.collect()
        #print(r)
        
        if (r != []):
    
          sql_cont = get_sql_context_instance(rdd.context)
    	
          row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
    	
          hasht_df = sql_cont.createDataFrame(row_rdd)
    	
          hasht_df.registerTempTable("hashtags")
    	
          hashtag_cts_df = sql_cont.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
          hashtag_cts_df.show()
        
          
        else:
          e = sys.exc_info()[0]
          print("Error: %s" % e)
	
	
    		

def agg_count(new_values, total_sum):
  if total_sum is None:
    total_sum=0
  return sum(new_values,total_sum)



	

words = dStream.flatMap(lambda line: line.split(" "))
tags = words.filter(lambda w: ('#' in w and(w=='#elon' or w=='#bitcoin' or w=='#NFT' or w=='#india' or w=='#metaverse' or w=='#modi' or w=='#honest'))).map(lambda x: (x, 1))
tagstot = tags.updateStateByKey(agg_count)
tagstot.foreachRDD(process_rdd)
stc.start()
stc.awaitTermination()



	
	


