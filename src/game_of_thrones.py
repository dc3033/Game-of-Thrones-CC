#Daniel Chan
#Game of Thrones Coding Challenge

#imports
import pyspark
import re

#function that takes a file's body and cleans it by removing punctuation, splitting on spaces, and setting it to lowercase
def clean(file_body):
    #file is the actual text, body is the location of the file
    file,body = file_body
    #results are returned in a set to eliminate duplicates
    body_set = set(re.split("\W+",body.lower()))
    return(body_set,file)

#function that sets the filename of a word from that file as that word's value
def filename_value(word_set_file):
    word_set,file = word_set_file
    try:
        #split the file location, then take the filename from it
        filename = int(file.split("/")[-1])
        tup=()
        for word in word_set:
            if word != "":
                tup +=((word,[filename]),)
        return tup
    except:
        print('filename not int')


if __name__ == "__main__":
    sc = SparkContext(appName="invertedIndex", conf=SparkConf().set("spark.driver.host", "localhost"))

    #load all of the files in the input directory into a RDD
    allFilesRDD =  sc.wholeTextFiles("..\\input\\*")

    #clean the text of the files, and set the words as keys and filenames as values
    cleanedRDD = allFilesRDD.map(clean).map(filename_value).flatMap(lambda x:x)

    #reduce the previous rdd so that word keys now have a list of all the filenames that contain that word
    reducedRDD = cleanedRDD.reduceByKey(lambda x,y : x + y)

    #assign an index to every key value pair in the previous rdd
    indexedRDD = reducedRDD.zipWithIndex()

    #create a dictionary with words as keys and indices as values
    indexDict = final_rdd.map(lambda x: (x[0][0],x[1])).collectAsMap()

    #write the dictionary to a text file
    dictFile = open("..\\output\\dictionary.txt","w")
    dictFile.write( str(indexDict) )
    dictFile.close()

    #create an inverted index by setting indices as keys and filename lists as values, then output in a text file
    final_rdd.map(lambda x : (x[1],x[0][1])).saveAsTextFile("..\\output\\inverted_index.txt")
