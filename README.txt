Project 2 for CSCE 608
Bowen Lan: 227001476
Qing Cao: 127004024

All the results for sql commands within test.txt is in result.pdf
Only one command with operator NOT cannot run correctly, because NOT is not implemented yet
All other commands is good to go even with []

The IDE we use is IntelliJ IDEA, Just compiled a jar file for grader to test
it can be run by using command : java -jar 608_p2.jar
It will ask you to input a command like: file D:\t1.txt, and output the results to the terminal.

There are snippet extracted from test.txt which we used to debug different functions
t1: natural join on table r s t
t2: two pass sort on table course; if you delete the tuples to only have
	ten or less left, it will start to test one pass sort
t3: cross join order for table t1 t2 t3 t4 t5 t6
t4: test complicated expression tree for command with parentheses to change the order on two tables:
	SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam = 100 OR course2.exam = 100 ]
t5: test complicated expression tree for command with parentheses to change the order on one table:
	SELECT * FROM course WHERE [ exam = 100 OR homework = 100 ] AND project = 100
t6: 200 tuples from project 1
t7: test insert command with select as values
	INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course

Implementation:
DISTINCT is done by sorting first, and when output it will checked with previous one.
For multiple tables, DISTINCT is done when joining, also the null tuple will be eliminated when join

For Join, we have one-pass, two-pass, one-pass is to put all blocks of smaller table
into memory; two-pass is to put one group(8 blocks) of one relation in the memory.

Use Heap to do the sort for one-pass and two-pass algorithm, we need to store disk position 
two-pass, because we need to know to which sublist the tuple in the heap belongs, in order to
get the next tuple in the sublist when this sublist in the main memory is exhausted.

For Natural Join, iterate one block from each table and join them if the fields are same

The code is commented thoroughly.
Thanks!

