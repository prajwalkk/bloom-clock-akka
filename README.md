# Bloom Clock

This project aims to simulate the working of a bloom clock to study its properties with respect to a traditional vector clocks used in distributed systems.

* The Architecture and Report can be found in the [Report](Bloom_Clock_Report_Prajwal.pdf)
* All the graphs and the Jupyter notebook used to make some analyses are in this [folder](DataAnalysis)


## Features:
1. Makes use of the Actor Concurrency model.
1. Created using Akka toolkit. Used the Akka-Typed variant, which resolves many anti-patterns of the classic model of Akka actors. 
1. Coded in Scala. 

## To run the simulations
1. Clone this Repository
2. Edit the simulations you want to test in the file `resources/constants.conf`. 
3. `sbt run`