# About 
This is the repository of the final project by the **Perfect Strangers** group for [Data Science](https://www.unibo.it/it/studiare/insegnamenti-competenze-trasversali-moocs/insegnamenti/insegnamento/2024/467046), the second module of the integrated course in Computational Management of Data (I.C), a.y. 2024/2025 of [_Digital Humanities and Digital Knowledge_](https://corsi.unibo.it/2cycle/DigitalHumanitiesKnowledge), at Alma Mater Studiorum - University of Bologna. 

# The project 
The project focuses on developing a software that allows one to process data stored in different formats in two databases that can be queried. 
In particular, we worked with one CSV file and one JSON file, and created respectively one graph and one relational database. Then, we developed queries methods to retrieve information from each of them, first individually, then combining the information retrived. 

### Repository contents 
In this repository we uploaded: 
- The final Python code: [impl.py](https://github.com/edotarp/Data-science-group-project/blob/main/impl.py)
- A folder containing the data provided: [data](https://github.com/edotarp/Perfect-Stranger-DHDK-2025/tree/main/data)
- A folder containing a small section of the previous ones, to test more easily each method: [test_data](https://github.com/edotarp/Perfect-Stranger-DHDK-2025/tree/main/test_data)

### Test results
As per request we made sure the code passed the basic tests, below we include a screenshot as proof.

![Screenshot of the basic test result: OK](https://github.com/edotarp/Perfect-Stranger-DHDK-2025/blob/main/img/Screenshot%202025-05-19%20182746.png) 

## Contributors and work division
* [Chiara Picardi](chiara.picardi2@studio.unibo.it)
    - Class `Handler`
    - Class `UploadHandler`
* [Farideh Sousani](farideh.sousani@studio.unibo.it)
   - Class `JournalQueryHandler`
* [Cecilia Vesci](cecilia.vesci@studio.unibo.it)
   - Class `QueryHandler`
   - Class `CategoryQueryHandler`
* [Edoardo Tarpinelli](edoardo.tarpinelli@studio.unibo.it)
   - Class `BasicQueryEngine`
   - Class `FullQueryEngine`
     
Each contributor is also referenced in the source code through comments next to their respective class implementations.
