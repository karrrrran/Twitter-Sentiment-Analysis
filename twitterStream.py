from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    poscount = []
    negcount = []
    for count in counts:
        if(count[0][0]=="positive"):
            poscount.append(count[0][1])
            negcount.append(count[1][1])
        else:
            poscount.append(count[1][1])
            negcount.append(count[0][1])
    pos = plt.plot(range(12), poscount,'go-', label='Positive')
    neg = plt.plot(range(12), negcount,'ro-', label='Negative')
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc = 'upper left')
    plt.show()
    plt.savefig('plot.png')
    plt.close()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    fwords = set()
    with open(filename) as f:
        for l in f:
            fwords.add(l.split()[0])
    return fwords


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    allwords = tweets.flatMap(lambda x:x.split(" "))
    def reaction(w):
        if w in pwords:
            return "positive"
        if w in nwords:
            return "negative"
        else:
            return "neutral"
    
    allwords = tweets.flatMap(lambda x:x.split(" ")) 
    b = allwords.map(lambda x: (reaction(x.lower()),1))
    count = b.reduceByKey(lambda x,y:x+y)
    count = count.filter(lambda x: (x[0]=="positive" or x[0]=="negative"))    


    def updateFunction(v,c):
        if c is None:
            c = 0
        return sum(v,c)  

    runningCounts = count.updateStateByKey(updateFunction)
    runningCounts.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
