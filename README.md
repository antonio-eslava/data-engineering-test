# data-engineering-test
# The DataScience Data Engineering Test - A parallelized solution suggestion (in Python)

Three years ago, Jordan Halterman  uploaded to Data Science Inc. GitHub account, a small battery of tests. These tests were aimed to evaluate the abilities of a Data Engineering candidate.

Several companies are using them as a technical basis for hiring processes and this problem, in itself, is an interesting example of ETL. 

Here is my proposal of solution using parallelization. 

### The problem: GitHub can´t parse the file

Jordan said that the test came from a real-world experience working on client data. The question? An apparently simple .tsv file that couldn´t be read by normal tsv readers and parsers.

As the author says, as long as GitHub cannot accurately parse the file, neither can our Hadoop, Pig, or data warehouse utilities.

**The real problem: line feeds in the middle of some fields**

Every record starts with a Line Feed character (0Ah) and finishes with the next Line Feed. Every field in the record starts with a Tab character (09h) and finishes with the next Tab.

The problem comes when Line Feeds gate-crashed in the middle of the record. Virtually every parser in the world understands Line Feed as a record end and would break a record like that in small pieces

**The challenge is now clear**

It´s imperative to do something with this file. The proposed challenge is to write a simple script for transforming ***data.tsv***, the original file, into a properly formatted tab-separated values (TSV) file. That could be read by any standard CSV/TSV parser.

The main formatting suggestion for the solution is:
* Fields that contain tab characters should be given back quoted.
It is consistent with the spirit of losing as less data as possible from the original file. Even those damned line feeds must be kept. If the misplaced line feeds are enclosed in quotes, its destructive capacity is overturned.
And those strange 00s?
Until now, we have been focused in the central problem and set aside collateral questions. Let´s deal with them.
 
As it can be seen, every byte in the file is escorted by a 00h character. Let´s make way to another small difficulty: the original file is encoded in UTF-16LE, and the corrected file must be delivered in UTF-8.
After checking the even positions in the file, I concluded that all of them were 00h. So, if I would remove them in the result file, no information would be lost.
A simple solution
A simple solution would be going down the file, byte to byte, and, for each 0Ah found, checking if it is in the middle of a record. In that case, we should enclose all the field in quotes.
How do I do that? Using a “quotes” field. If the 0Ah is found in the middle of a field, this parameter is raised to True. And, when the record end is found, the whole record is wrapped into quotes.
A normal record
Let´s see a normal record. At starting, the field value is null, and the t value is 0. t will act as the field number (five fields or each record: id, first_name, last_name, account_number, email).
Each time a 09h (\t, tab) is found, the field values are changed.
 
When a 0Ah (line feed) is found, the record is finished. And the record values are reset.
A defective record
Here is a record with problems. The 0Ah is found when t = 2. So, the line feed is in the middle of the record, not at its end. The “quotes” parameter is raised and the whole field is wrapped into quotes.
 
And, voilà, mission accomplished. The offending 0Ah is now locked up among quotes. 
 
The file can now be easily read by a normal tables parses. R for example:
 
Parallelization
The code for this simple script is in gitHub. But it hasn´t really a great relevance. There are thousands of more complicated problems solved each day by the legion of programmers giving support to our digitalized world.
 The real challenge comes with the parallelization 
For bonus points, ambitious candidates can parallelize their algorithm.
I´m not actually a candidate, but I´m ambitious. Especially regarding Parallelization, Big Data, Machine Learning and all this exciting stuff.
And for helping people who starts with parallelization is because I have written this article. Not for experts and specialists, but for learners needing a concrete case. An interesting case beyond the parallelization of any easy “hello world” file.
The behaviour of parallelization
There are a lot of approximations to this technique, but I have had in mind the successful and ubiquitous framework of Apache Spark.
The idea for this process is to take a dataset (the given file, in this case), divide them in several parts, and give each of these parts to a worker, node, container, … in a separate CPU. 
The same script is executed in each of those containers. The results for each container are gathered and merging all these parts gives a final result.
Advantages? Time. The time a CPU walks for all over the file is divided by the number of workers doing the same job …. In parallel. 😊
 
The division problem
We can then suppose that this process will work fine, at an incredible speed, delivering exact results without losing data.
Everything would be as the text says:
Given an arbitrary byte position and length, the algorithm cleans a portion of the full data set and produces a unique TSV output file.
Concatenating the outputs of multiple processes should result in a well-formed TSV file containing no duplicates.
But here we have the key of the problem:
It's important to note that the arbitrary position may not necessarily be the start of a new line.
Each complete record in each worker will be treated and “fixed” using the already provided script. But, what about the records cut by the division?
Each part of the original file will start with the tail of a record and will end with the head of another. The script won’t be able of count the fields for decide if one of them contains a spurious 0Ah.
The broken fields
Let´s suppose that the record #201 is split among two parts of the file. One of them would end that way: 
id: 201
first_name: Kuame
last_name: Cole
account_number: 774
And other of then would start like that:
account_number: 288
email: penatibus.et@dolor.org


The worker that treats each fragment doesn´t know anything about the other part of the record. Then, it will be necessary to set a process that reunite the record parts after processing and fixing them. If we don´t treat these fragments in an appropriate manner, we could lose information, or have partial records.
The solution
Before proposing my own solution to the problem, I would like to comment some small considerations:
•	I could have divided the process in a set of Spark transformations and processes. But I have tried to make a unique script. Several transformations could provide more time and computational cost that a unique process split into a set of parallel workers.
•	I have explored other approaches like regular expressions, or separator for Spark. But, none of them seemed to be simple. And I want to use this article for to help people who are starting with parallelization and ETL.
•	It is supposed that every record has an id field, that field is consecutive, and there is no gap in the series. (This is important for the function firstRec() to infer the value of the id field for that record.
•	There is a script in Python that is easily adaptable to any parallelization framework or technique.
For this sample, I have divided the original data set (data.tsv) in five equal parts (part0.tsv, part1.tsv, part2.tsv, part3.tsv, part4.tsv). Each one of this parts is the feed for each worker in a parallelized system. In this case, and for testing purposes, the same script treats each part sequentially.
Intermediate files
For each part (0 to 4), the original part (partX.tsv) is read by three functions: firstRec(), mainRecs(), lastRec(), that create three intermediate files: fixedStartX.tsv, fixedMainX.tsv, fixedEndX.tsv. 
The half records, united
Every resulting file is fixed. The problem now is the half-records in the starting and the ending of each part.
For each of them, the lastRec() and  firstRec() functions have created files (fixedEndX.tsv, fixedStartX.tsv), that have the same id. For the fixedEndX.tsv file, when a field is unknown, it is included, but with a Null value (nothing between the 09h separators). For both, If the field is partial, its part is passed to the file.
Then, for each pair End-Start, we will have two files. For example:
 
Is a fixedEndX.tsv file. And:
 
Is a fixedStartX.tsv one.
As we can see, the first of them is formed by the fields id=201, first_name=Kuame, last_name=Cole, and a partial account_number field, 774.
The End file is formed by the fields id=201, first_name=Null, last_name=Null, a partial account_number field, with value 288, and email=penatibus.et@dolor.org.
The function mixEndStart() takes each field for both records and concatenate them. The Null  fields don´t add anything, and the partial field is restored. The id field has a special treatment for not to be duplicated.
How to know the id field when you don´t have the beginning of the record?
There is a small piece of code that takes several fields ahead (15 in the proposal), checks if there are two consecutives numbers separated by five fields, and guess that the id number is the minor of them minus one:
<code>
The final reassembling
Once the fixedXXX.tsv files ready, it´s necessary to reassemble them sequentially. Each parallelization framework has its own procedures or, as in the example shown, the own script will do this task at the end. The result: the original file with those offending internal 0Ah enclosed in quotes, without loss of data.
Only a simple approach to show the parallelization problem
Of course, there will be a lot of more effective, clever, cleaner or sophisticated approaches to this problem. My aim was only to give to the beginners of parallelization an interesting problem and suggest how to solve it. In such a way that they can touch the main problem of the code independence for each of the nodes or workers.
The code for this test is available in …. 
