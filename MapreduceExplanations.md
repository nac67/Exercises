Mapreduce Explanations
=========

Average Grade
-------------------------
This example finds the average grade for a bunch of students' grade records.
  
The input file is in the form:
> `Bobby Math 95 Science 86 History 68 English 75 
Billy Science 100 Juggling 90 Math 80
.
.
.`

Where each line starts with a student's name, next are pairs of classes and grades.  
  
The output of the program is a list of `<class, grade>` that finds the average grade from all students for the given class. 
  
The output should look like:
> `<Juggling, 90>
<History, 86>
<Math, 92>
<Science, 92>
<English, 85>`

This is all completed in the three map reduce stages: **map**, **shuffle**, and **reduce**.
  
In the **map** stage, the input file is split into separate lines and each line is sent to a worker thread. The worker thread reads through the line and then outputs a list of `<class, grade>` pairs.
  
Once all the workers have finished, it changes to the **shuffle** stage. Here, all pairs with the same class are grouped together.
  
Next, in the **reduce** stage, each group of classes is sent to a worker thread (for example, a worker thread may get all of the pairs with math classes). It is in this stage that it looks through all the grades for its given class and calculates the average. It then aggregates all of the reducers output into the final list of `<class, average grade>` pairs.