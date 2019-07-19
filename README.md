# Game-of-Thrones
The goal of this coding challenge is to take a collection of documents and build an invertex index from them. To do this, we need to create a dictionary that matches every word from the documents with a unique ID, then we need to create an inverted index that gives, for every word, the list of documents it appears in.

# Requirements
* Java 1.8
* Python 3.7
* Pyspark

To install Java 1.8 and Python 3.7 on Linux/Mac, enter the following commands into your terminal:

    sudo apt install openjdk-8-jdk
    sudo apt install python3.7
    
To install Pyspark, follow the guides in the links below to install Anaconda, then Pyspark.

Anaconda: https://www.digitalocean.com/community/tutorials/how-to-install-anaconda-on-ubuntu-18-04-quickstart
Pyspark: https://mortada.net/3-easy-steps-to-set-up-pyspark.html

# Repo Directory Structure
    ├── README.md 
    ├── src
    │   └── game_of_thrones.py
    |   └── got_unittest.py
    ├── input
    │   └── 0
    │   └── ...
    │   └── 44
    ├── output
        └── dictionary.txt
        └── inverted_index

# Run Instructions
From your terminal, set your current directory to the directory where game_of_thrones.py is located, then enter the following command:

    python game_of_thrones.py
    
# Approach
First, an RDD is created from reading all of the files in the input directory, giving us both the body of each file as well as the file path.

Next, the body of each file is cleaned with a function that removes punctuation, sets everything to lowercase, and splits elements by spaces.

Next, the filename is extracted from the file path for each file and a tuple is created for each word and its corresponding filename.

Next, the RDD is reduced by key, resulting in each word having a list of files that word appears in as its key. The RDD is then zipped with index to assign a unique index to each word.

Finally, the dictionary is created by setting the word as key and index as value, and collecting as map to return a dictionary. The inverted index is created by setting the index as key and file list as value, then saving the result as a text file.
