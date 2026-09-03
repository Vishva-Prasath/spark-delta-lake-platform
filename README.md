# Enterprise Apache Spark & Delta Lake Data Platform

An end-to-end, production-grade Lakehouse architecture built with **Apache Spark (PySpark)** and **Delta Lake**. This platform demonstrates real-time CDC ingestion, automated data quality validation gates, quarantine management, schema evolution, and performance optimizations (Z-Ordering & Data Salting).

---

##  System Architecture

┌─────────────────────────────┐
             │    Source Systems / CDC     │
             └──────────────┬──────────────┘
                            │
                            ▼
             ┌─────────────────────────────┐
             │     Bronze Delta Layer      │  (Raw Json + Metadata)
             └──────────────┬──────────────┘
                            │
                            ▼
             ┌─────────────────────────────┐
             │     Data Quality Gate       │
             └──────┬───────────────┬──────┘
                    │               │
             Valid  │               │ Invalid
                    ▼               ▼
    ┌───────────────────────┐ ┌─────────────────────────┐
    │  Silver Delta Layer   │ │ Quarantine Delta Table  │
    │ (Cleaned & Upserted)  │ └─────────────────────────┘
    └───────────┬───────────┘
                │
                ▼
    ┌───────────────────────┐
    │   Gold Delta Layer    │  (Aggregated Business KPIs)
    └───────────┬───────────┘
                │
                ▼
    ┌───────────────────────┐
    │ Analytics & Dashboard │
    └───────────────────────┘


---

##  Tech Stack & Key Concepts

* **Processing Engine:** Apache Spark 3.5.0 (PySpark) with Adaptive Query Execution (AQE).
* **Storage Layer:** Delta Lake 3.1.0 (ACID transactions, Time Travel, Merge/Upsert, Vacuum/Optimize).
* **Data Quality:** Custom rule validation engine with automated routing to Quarantine tables.
* **Optimization Techniques:** Z-Ordering, Data Salting for skew handling, Broadcast joins, Partition Pruning.
* **Testing & Quality:** PyTest unit testing suite with isolated Spark sessions.

---

##  Repository Structure

```text
spark-delta-lake-platform/
├── configs/                # Environment-specific configuration parameters
├── src/
│   ├── ingestion/          # Bronze layer ingestion scripts
│   ├── quality/            # Data quality verification engine & rules
│   ├── transformations/    # Silver layer MERGE and Gold aggregations
│   ├── maintenance/        # Delta OPTIMIZE & VACUUM jobs
│   └── utils/              # Spark session builder & structured loggers
├── jobs/                   # End-to-end pipeline execution entry points
├── sample_data/            # Mock dataset generators
├── tests/                  # PyTest unit & integration suite
├── pytest.ini              # PyTest configuration for module resolution
└── requirements.txt        # Managed dependencies