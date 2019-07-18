#imports
#from pyspark import SparkContext, SparkConf
import pyspark
import re
import unittest

class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

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

class basicTest(PySparkTestCase):

    def test_clean(self):
        test_input = [('path/name', 'Test inPut!\n#1')]
        test_rdd = self.sc.parallelize(test_input).map(clean)
        self.assertEqual(test_rdd.collect(), [({'1', 'input', 'test'}, 'path/name')])

    def test_filename_value(self):
        test_input = [({'1', 'input', 'test'}, 'path/name/1')]
        test_rdd = self.sc.parallelize(test_input).map(filename_value)
        self.assertEqual(test_rdd.collect(), [(('input', [1]), ('1', [1]), ('test', [1]))])

if __name__== "__main__":
    unittest.main()
