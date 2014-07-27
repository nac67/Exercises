Exercises
=========

Projects
-------------------------
###Mapreduce
An implementation of mapreduce that that can either
* Run on one thread
* Run on multiple threads (expanding as necessary)
* Run on a fixed size pool of threads

Configuring with Eclipse
-------------------------
###In Eclipse
* File > New > Java Project
    * Use at least jdk 1.7
   
###In File Browser
* Navigate to the directory housing the eclipse project
* Delete src folder
* Copy (or create symlink) **src** folder into project folder
* Copy (or create symlink) **mapreduceExamples** into project folder

> To create symlinks do the following. In **Windows** open up command prompt with administrative privileges. Run the command  
> ```mklink /d "<Eclipse folder>\src" "<git repo>\src"```  
> But replace items between angle braces with the real paths.
>
> On **Mac/Linux** open a terminal and run the command  
> ```ln -s "<git repo>/src" "<Eclipse folder>/src"```  
> Note that the order is opposite on Windows.
> 
> There should now be links in the Eclipse folder leading to the real locations in the git folder.
    
###In Eclipse
* Right click on project and click refresh. **src** folder and **mapreduceExamples** should show up in project explorer.
* Project > Properties
    * Java Build Path > Libraries > Add Library...
    * Add JUnit4
