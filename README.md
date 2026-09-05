# Vehicle Telemetry Analytics Platform using Apache Spark

## 1. Project Overview

This project presents the design and implementation of a distributed vehicle telemetry analytics platform using Apache Spark and PySpark.

The project demonstrates how distributed data processing techniques can be applied to vehicle telemetry workloads involving large volumes of sensor data. The implementation focuses on scalability, efficient distributed processing, data-skew mitigation, workload distribution, fault tolerance, and Spark execution mechanics.

The project combines architectural analysis with a practical PySpark implementation covering:

- Distributed telemetry data processing
- Narrow and wide transformations
- Aggregation and filtering
- Data skew simulation
- Two-stage salting
- Hash partitioning
- RDD lineage
- Deep lineage simulation
- Checkpointing
- Lazy evaluation
- Directed Acyclic Graphs (DAGs)
- Stage formation and task scheduling
- Data locality
- Cache versus checkpoint
- Hadoop MapReduce versus Apache Spark

---

## 2. Problem Statement

Modern connected vehicles continuously generate telemetry data from multiple sensors, including engine, battery, braking, GPS, environmental, and operational measurements.

Processing such telemetry efficiently requires a distributed architecture capable of handling increasing data volume while maintaining scalability, fault tolerance, and efficient resource utilisation.

The objective of this project is to design and demonstrate a Spark-based telemetry processing platform that can efficiently process distributed vehicle data while addressing common Big Data challenges such as data skew, network shuffling, partition imbalance, long lineage, and recovery from failures.

---

## 3. Dataset

The project uses the:

**Vehicle Maintenance Telemetry Data**

Source:

Kaggle – https://www.kaggle.com/datasets/tejalaveti2306/vehicle-maintenance-telemetry-data

The dataset contains **1,970 historical vehicle records** covering multiple vehicle brands and telemetry-related attributes.

The dataset includes information associated with areas such as:

- Engine dynamics
- Engine temperature
- Battery condition
- Braking performance
- GPS information
- Vehicle speed
- Fuel consumption
- Environmental measurements
- Predictive maintenance/failure indicators

The dataset is used as a practical workload for demonstrating distributed Spark processing techniques.

---

## 4. System Architecture

The proposed platform follows a layered distributed architecture separating data generation, ingestion, processing, storage, and analytical services.

The logical flow is:

Vehicle Sensors

↓

Data Ingestion

↓

Distributed Storage

↓

Apache Spark Processing

↓

Analytics and Aggregation

↓

Fleet Monitoring / Predictive Maintenance / Operational Applications

The architecture is designed to support scalability and fault tolerance by distributing processing across multiple worker nodes rather than depending on a single high-capacity machine.

---

## 5. Wall of Hardware and Scaling Strategy

A single physical machine eventually becomes constrained by CPU, memory, storage capacity, and network resources as telemetry volume increases.

Vertical scaling attempts to address this problem by increasing the resources of a single machine.

Horizontal scaling instead distributes the workload across multiple machines.

For the proposed telemetry platform, horizontal scaling is preferred because it provides:

- Greater aggregate processing capacity
- Distributed memory and CPU resources
- Better workload distribution
- Improved scalability as data volume grows
- Greater resilience to individual node failures

The architecture therefore follows a distributed computing approach in which telemetry processing can be expanded by adding worker nodes.

---

## 6. The Three Vs of Big Data

The telemetry workload can be understood through the Three Vs of Big Data.

### Volume

Connected vehicles can continuously generate large quantities of sensor measurements. As fleet size and monitoring frequency increase, the resulting data volume can exceed the practical limits of a single machine.

### Velocity

Telemetry data can be generated continuously and potentially at high frequency. A scalable processing platform must therefore support efficient ingestion and processing of rapidly generated data.

### Variety

Telemetry platforms may contain multiple types of measurements, including engine, battery, braking, GPS, environmental, and operational attributes.

Apache Spark provides a suitable distributed processing framework for analysing these heterogeneous telemetry workloads.

---

## 7. ACID, BASE and CAP Theorem

Traditional ACID-oriented systems prioritise strong consistency, transaction isolation, and reliability.

BASE systems instead favour:

- Basically Available
- Soft state
- Eventual consistency

For large-scale telemetry analytics, BASE-style distributed processing can be appropriate where immediate strong consistency is less important than availability and scalability.

The CAP theorem states that a distributed system cannot simultaneously guarantee perfect Consistency, Availability, and Partition tolerance under network partition conditions.

For the telemetry analytics scenario, availability and partition tolerance are important because telemetry collection and analytics should continue even when individual components experience temporary communication problems.

The architecture therefore favours an AP-oriented approach with eventual consistency for analytical telemetry workloads where appropriate.

---

## 8. Why Apache Spark

Apache Spark was selected because the telemetry workload requires repeated distributed transformations, aggregation, partitioning, skew mitigation, lineage analysis, and checkpointing.

Spark provides:

- Distributed in-memory processing
- High-level DataFrame and RDD APIs
- DAG-based execution
- Lazy evaluation
- Efficient task scheduling
- Built-in fault tolerance through lineage
- Support for partitioning and repartitioning
- Support for iterative analytics
- Integration with SQL and machine learning workloads

Compared with traditional Hadoop MapReduce, Spark reduces repeated disk-based processing and provides a more flexible execution model for iterative analytical workloads.

---

## 9. Hadoop MapReduce Implementation Concept

The project also demonstrates the logical MapReduce processing model for telemetry aggregation.

The processing flow is:

Split
--> 
Map
--> 
Shuffle
-->
Sort
-->
Reduce
--> 
Final Output


For the telemetry use case, the mapper can generate **key-value** pairs such as: Vehicle Brand → Engine Temperature


The **shuffle and sort** phase groups records with the same key.

The **reducer** then calculates an aggregated value such as average engine temperature for each vehicle brand.

The major limitation of **MapReduce** for iterative analytical workloads is its disk-oriented execution model, where intermediate results are repeatedly written to storage.

---

## 10. PySpark Implementation

The practical implementation is developed using PySpark.

The notebook establishes a SparkSession and loads the telemetry dataset into a Spark DataFrame.

The implementation includes:

- Dataset loading
- Schema validation
- Data quality inspection
- Distributed filtering
- Aggregation
- Narrow transformations
- Wide transformations
- Data-skew simulation
- Salting
- Hash partitioning
- RDD lineage inspection
- Checkpointing
- Spark execution analysis

The notebook provides executable demonstrations and output evidence for the major Spark concepts.

---

## 11. Narrow and Wide Transformations

The project demonstrates both narrow and wide dependencies.

### Narrow Transformation

A narrow transformation occurs when each output partition depends on only one parent partition.

The project uses high-engine-temperature filtering as an example.

The filtering operation can be performed locally on each partition without requiring records to be redistributed across the network.

### Wide Transformation

A wide transformation requires data to be redistributed between partitions.

The project uses grouped aggregation, such as calculating average engine temperature by vehicle brand, as an example.

Because records with the same key may exist in different partitions, Spark performs a **shuffle before the aggregation**.

This distinction is important because wide transformations introduce network communication and can create performance bottlenecks.

---

## 12. Data Skew and Salting

Data skew occurs when a small number of keys contain disproportionately large amounts of data.

The original telemetry dataset does not naturally exhibit the extreme skew required to demonstrate this problem. Therefore, the notebook intentionally creates a highly skewed workload.

The simulation creates a dominant: `Delivery_Truck` category.

The resulting workload contains a significantly larger number of records for this category than for the other vehicle brands.

This demonstrates how a heavily skewed key can create an executor hotspot during shuffle-intensive operations.

### Two-Stage Salting

To mitigate the simulated skew, the project implements a two-stage salting strategy.

A salt value is generated and combined with the original skewed key.

For example:

Delivery_Truck_0
Delivery_Truck_1
Delivery_Truck_2
Delivery_Truck_3
Delivery_Truck_4

The original dominant key is therefore divided into multiple salted keys.

The processing flow becomes:

Skewed Key
↓
Generate Salt
↓
Create Salted Key
↓
Partial Aggregation
↓
Final Aggregation

This allows the workload to be distributed across multiple partitions rather than concentrating all records under a single key.

---

## 13. Hash Partitioning

After salting, hash partitioning is applied using the salted key.

The notebook demonstrates partitioning with: `repartition(5, "salted_key")`

The resulting workload is distributed across five partitions.

The demonstrated partition distribution is approximately:

- Delivery_Truck_0: 333
- Delivery_Truck_1: 332
- Delivery_Truck_2: 376
- Delivery_Truck_3: 364
- Delivery_Truck_4: 323

The distribution demonstrates how salting combined with hash partitioning can spread a previously concentrated workload across multiple partitions.

This improves parallelism and reduces the risk of a single overloaded partition becoming the dominant bottleneck.

---

## 14. RDD Lineage and Fault Tolerance

Spark uses RDD lineage to maintain the transformation history required to reconstruct lost partitions.

Instead of storing redundant copies of every intermediate result, Spark records how an RDD was derived from previous datasets.

The notebook demonstrates lineage inspection using: `toDebugString()`

The lineage information illustrates the dependency chain created by Spark transformations.

If a partition is lost because of executor or node failure, Spark can use the lineage information to recompute the required partition.

This provides fault tolerance while avoiding the storage overhead of replicating every intermediate dataset.

---

## 15. Deep Lineage and Checkpointing

Long transformation chains can make lineage increasingly complex, particularly in iterative processing.

The notebook constructs a deep transformation chain to simulate this condition.

Checkpointing is then applied to create a stable recovery point.

The conceptual process is:

Long Transformation Chain
↓
Deep Lineage
↓
Checkpoint
↓
Lineage Truncated
↓
Stable Recovery Point

Checkpointing differs from caching because checkpointing is intended to truncate the lineage graph by writing the resulting RDD to reliable storage.

This reduces the amount of historical transformation information that Spark must retain for recovery.

---

## 16. Lazy Evaluation and DAG Execution

Apache Spark uses lazy evaluation.

Transformations are not immediately executed when they are defined.

Instead, Spark builds an execution plan and waits until an action is invoked.

Typical actions include:

- `show()`
- `count()`
- `collect()`

The conceptual workflow is:

Read Data
↓
Filter
↓
GroupBy
↓
Aggregate
↓
Action
↓
Spark Executes Optimised Plan

This allows Spark to analyse the complete workflow before execution.

Spark represents the execution plan as a Directed Acyclic Graph (DAG), where transformations and their dependencies determine how the computation is executed.

---

## 17. Stage Formation and Task Scheduling

Spark divides DAG execution into stages based primarily on dependency boundaries.

Narrow transformations can generally be pipelined within the same stage.

Wide transformations introduce shuffle boundaries and therefore create stage boundaries.

The simplified execution flow is:

Transformations
↓
DAG Construction
↓
Shuffle Boundaries
↓
Stages
↓
Tasks
↓
Executors

Each stage is divided into tasks that operate on individual partitions.

The Spark scheduler distributes these tasks across available executors.

---

## 18. Data Locality

Data locality refers to executing computation as close as possible to the data being processed.

Moving computation toward data reduces unnecessary network transfer.

Spark considers data locality when scheduling tasks, preferring execution where the required data is already available or nearby.

For large telemetry workloads, efficient locality can reduce network traffic and improve overall processing efficiency.

---

## 19. Cache versus Checkpoint

Caching and checkpointing serve different purposes.

| Feature | Cache | Checkpoint |
|---|---|---|
| Primary purpose | Speed up repeated computation | Truncate lineage and provide recovery point |
| Lineage | Preserved | Truncated |
| Storage | Memory/local disk depending on persistence level | Reliable checkpoint storage |
| Recovery | Can recompute using lineage | Recovers from checkpoint |
| Best suited for | Reusing intermediate results | Long or iterative processing |

Caching is primarily a performance optimisation.

Checkpointing is primarily a resilience and lineage-management mechanism.

The distinction is particularly important for iterative Spark workloads where lineage can become very deep.

---

## 20. Project Outcomes and Key Learnings

This project demonstrates how Spark can be used to address several practical distributed computing challenges.

### Key outcomes

- Demonstrated distributed DataFrame processing using PySpark.
- Demonstrated narrow and wide dependencies.
- Demonstrated the effect of shuffle-intensive operations.
- Simulated severe data skew.
- Implemented two-stage salting.
- Applied hash partitioning to distribute salted workloads.
- Inspected RDD lineage using `toDebugString()`.
- Simulated deep transformation lineage.
- Applied checkpointing to truncate lineage.
- Demonstrated Spark lazy evaluation.
- Explained DAG construction and execution.
- Explained stage formation and task scheduling.
- Demonstrated the importance of data locality.
- Compared caching and checkpointing.
- Compared Hadoop MapReduce with Apache Spark.

---

## 21. Conclusion

This project successfully demonstrates the design and implementation of a scalable and resilient vehicle telemetry analytics platform using Apache Spark.

The implementation combines theoretical Big Data concepts with practical PySpark demonstrations. In particular, the project addresses workload distribution through partitioning and salting, fault tolerance through RDD lineage and checkpointing, and execution optimisation through lazy evaluation and DAG-based scheduling.

The data-skew experiment demonstrates how a dominant key can create a processing hotspot and how salting combined with hash partitioning can distribute the workload more effectively.

The lineage and checkpointing experiments demonstrate the different approaches Spark uses to maintain resilience and manage recovery cost.

Overall, the project demonstrates how Apache Spark provides a flexible distributed processing framework for telemetry analytics and provides a foundation that can be extended toward real-time telemetry ingestion, streaming analytics, predictive maintenance, and larger-scale fleet management applications.

---

## 22. Repository Contents

The repository contains the following project artifacts:

- `README.md` — Project overview, architecture, implementation details, and technical explanations.
- `Vehicle_Telemetry_Spark_Project.ipynb` — Executable PySpark implementation.
- `MSc_Big_Data_Analytics_Assignment_Report.docx` — Detailed academic assignment report.
- `requirements.txt` — Python and PySpark dependencies.
- `.gitignore` — Git ignore configuration.

---

## 23. Technologies Used

- Python
- Apache Spark
- PySpark
- Spark SQL
- RDD
- Jupyter Notebook
- Pandas
- NumPy
- Matplotlib
- Seaborn

---

## 24. Reproducibility

The practical implementation is provided as a Jupyter Notebook.

To reproduce the project:

1. Install the dependencies listed in `requirements.txt`.
2. Open `Vehicle_Telemetry_Spark_Project.ipynb`.
3. Configure the Spark environment.
4. Provide access to the telemetry dataset.
5. Execute the notebook cells sequentially.
6. Review the generated outputs for data quality, aggregation, skew simulation, salting, partitioning, lineage, checkpointing, and Spark execution concepts.

---

## 25. Academic Context

**Programme:** M.Sc. Data Science and Artificial Intelligence

**Course:** Big Data Platforms & Analytics

**Project:** Designing and Optimizing a Resilient Global Vehicle Telemetry Analytics Platform using Apache Spark

**Development Platform:** Jupyter Notebook / PySpark

---

## 26. Author

**Sunita Baloda**
