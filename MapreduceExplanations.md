Mapreduce Explanations
=========

Average Grade
-------------------------
This example finds the average grade for a bunch of students' grade records.
  
The input file is in the form:
> Bobby Math 95 Science 86 History 68 English 75  
> Billy Science 100 Juggling 90 Math 80  
> .  
> .  
> .

Where each line starts with a student's name, next are pairs of classes and grades.  
  
The output of the program is a list of `<class, average grade>` that finds the average grade from all students for the given class. 
  
The output should look like:
> (Juggling, 90)  
> (History, 86)  
> (Math, 92)  
> (Science, 92)  
> (English, 85)  

This is all completed in the three map reduce stages: **map**, **shuffle**, and **reduce**.
  
In the **map** stage, the input file is split into separate lines and each line is sent to a worker thread. The worker thread reads through the line and then outputs a list of `<class, grade>` pairs.
  
Once all the workers have finished, it changes to the **shuffle** stage. Here, all pairs with the same class are grouped together.
  
Next, in the **reduce** stage, each group of classes is sent to a worker thread (for example, a worker thread may get all of the pairs with math classes). It is in this stage that it looks through all the grades for its given class and calculates the average. It then aggregates all of the reducers output into the final list of `<class, average grade>` pairs.

Statewide High Low Temperature
-------------------------
This example takes a list of cities and their high and low temperature, and outputs a list of states and their overall high and low temperatures determined from the input cities
  
The input file is in the form:
> City1 high low  
> City2 hgih low  
> .  
> .  
> .
  
The output of the program is a list of `<state, overall high low>` that takes into account all of the given cities in the state to find the overall high temperature and low temperatue

This is all completed in the three map reduce stages: **map**, **shuffle**, and **reduce**.
  
In the **map** stage, the input file is split into separate lines and each line is sent to a worker thread. The worker thread reads through the line figures out which state a city is in, and outputs `<state, hi low>`
  
Once all the workers have finished, it changes to the **shuffle** stage. Here, all pairs with the same state are grouped together.
  
Next, in the **reduce** stage, each group of states is sent to a worker thread (for example, a worker thread may get all of the pairs with "CA"). It is in this stage that it looks through all the high and low temperature for its given cities and calculates the overall high and overall low. It then aggregates all of the reducers output into the final list of `<state, hi lo>` pairs.

Character Count
-------------------------
This example takes a general text file and outputs a list of the number of occurences of words of certain length. # of words with 1 character, # of words with 2 characters etc...

The output of the program is a list of `<charCount, number of words witht that many characters>`

The output should look like:
>  (1, 217)  
>  (2, 501)  
>  (3, 577)  
>  (4, 511)  
>  (5, 257)  
>  (6, 260)  
>  (7, 193)  
>  (8, 104)  
>  (9, 77)  
>  (10, 47)  
>  (11, 14)  
>  (12, 9)  
>  (13, 7)  
>  (14, 6)  

This is all completed in the three map reduce stages: **map**, **shuffle**, and **reduce**.
  
In the **map** stage, the input file is split into separate lines and each line is sent to a worker thread. The worker thread reads through the line and creates a pair for each word with an initial count of one. `<charCountforWord1, 1>, <charcountForWord2, 1>...`
  
Once all the workers have finished, it changes to the **shuffle** stage. Here, all pairs with the same charCounts are grouped together.
  
Next, in the **reduce** stage, each group of charCounts is sent to a worker thread (for example, a worker thread may get all of the pairs with a character count of 2). It is in this stage it counts how many words of that count there are and outputs `<charCount, number of words>`. It then aggregates all of the reducers output into the final list of `<charCount, number of words>` pairs.

