# Re-Vision Data Engineering Task

An ETL pipeline in the data engineering Re-Vision team.

1.[ETL Architecture](#etl-architecture-diagram)

2.[Category Tree](#category-tree)

3.[Data Model](#data-model)

4.[How to run the code](#how-to-run-the-code)



## ETL Architecture Diagram

![Alt text](images/ETL_Architecture_Diagram.png "ETL Architecture Diagram overview")

The image above lays out the processing structure. We are going to go through an overview of why we try to adhere to this structure and will try to use the problem set  to explain how we'll apply this structure to the problem set.

The first thing to note is our tech stack. We use CSVs for data storage, our transformation logic to move data from A to B is not restricted to but normally carried out in Python or Spark. We are using batch processing in the **"Full Initial Load"** however we can use Spark streaming at **Incremental Load"**.Also the current pipeline export data to CSV we can export it to any On-demand database like MYSQL or Cloud-based like AWS redshift.

Second, The attached scripts is exist in two pictures, **.ipynb** for explanation and **.py** for running.

Third, although all scripts contain the names of files as a variable, we can pass them as Command Line Arguments.

## Category Tree
![Alt text](images/CategoryTree.png "Data Model overview")

The Structure of `Catergory_Tree` is memory efficient, however, it makes it hard to retrieve any of the pieces of information under some ParentId. Thus we use **DFS Traverse** to traverse the tree and make vertex between each node, which helps in finding the whole path from the root to leaves

## Data Model
![Alt text](images/Data_Model.png "Data Model overview")


## How to run the code

### First, To run batch processing

Install `python-3.10.5`

The `requirements.txt` file should list all Python libraries scripts depend on, and they will be installed using:

```
pip install -r requirements.txt
```
### second, To run streaming process

Follow this [link](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) 
