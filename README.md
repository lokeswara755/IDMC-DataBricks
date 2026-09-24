# IDMC to Databricks Migration Framework

## Overview

This project demonstrates an end-to-end migration framework to migrate IDMC (Intelligent Data Management Cloud) mappings into Databricks.

The objective of this framework is to extract metadata from IDMC XML exports, understand the mapping logic, generate equivalent Databricks PySpark implementation, execute the generated code, create Delta tables, and validate the migrated output.

The framework follows a metadata-driven approach where IDMC mapping objects are converted into executable Databricks artifacts.

---

# Migration Architecture

IDMC XML Export
|
v
XML Reader
|
v
Metadata Parser Layer/repository parser
|
v
Graph Builder
|
v
Intermediate Representation (IR)
|
v
IR Validation
|
v
PySpark Code Generation
|
v
Generated PySpark Artifact (.py)
|
v
PySpark Execution
|
v
Delta Table Creation
|
v
Data Validation & Reconciliation


# Project Structure

IDMC-DataBricks

├── parser
│   │
│   ├── xmlreader.py
│   ├── repository_parser.py
│   ├── connector_parser.py
│   ├── source_parser.py
│   ├── target_parser.py
│   ├── mapping_parser.py
│   ├── port_parser.py
│   ├── session_parser.py
│   ├── graph_builder.py
│   │
│   └── Datavalidator.py
│
├── Generator
│   │
│   ├── IR_Generator.py
│   └── PySpark_Generator.py
│
├── Validator
│   │
│   ├── IR_Validator.py
│   └── PySpark_Validator.py
│
├── pipeline
│   │
│   └── migration_pipeline.py
│
└── notebooks
    │
    └── IDMC_Migration_Run


---

# Component Details

## 1. XML Reader

**File:**


Responsible for loading the IDMC XML export file and providing the XML structure for further processing.

---

# 2. Parser Layer

The parser layer extracts required metadata from the IDMC XML.

The following metadata is extracted:

- Repository details
- Folder information
- Source objects
- Target objects
- Connections
- Mappings
- Transformations
- Ports
- Data flow links
- Session information


## Repository Parser

Extracts:

- Repository
- Folder
- Mapping details
- Session details


## Source Parser

Extracts source metadata:

Example:

Includes:

- Target table
- Target columns
- Datatypes


## Mapping Parser

Extracts IDMC mapping logic:

- Source qualifiers
- Transformations
- Ports
- Transformation links

Example transformations:



---

# 3. Graph Builder

**File:**

The Graph Builder converts IDMC transformation links into a dependency graph.

Purpose:

- Identify transformation dependencies
- Determine execution order
- Ensure transformations are generated in the correct sequence


Example:

The generated execution order is used by the PySpark Generator.

---

# 4. Intermediate Representation (IR)

**File:**

The extracted IDMC metadata is converted into an intermediate representation before generating PySpark code.

The IR contains:

- Source metadata
- Target metadata
- Transformations
- Links
- Execution order


Benefits:

- Separates metadata extraction from code generation
- Provides a structured migration model
- Enables validation before code generation

---

# 5. IR Validation

**File:**


Validates the generated IR before creating PySpark code.

Validation includes:

- Source availability
- Target availability
- Transformation metadata
- Dependency correctness

---

# 6. PySpark Generator

**File:**

The PySpark Generator converts the IR into executable Databricks PySpark code.

The generator translates IDMC transformations into equivalent Spark operations.

## Transformation Mapping

| IDMC Transformation | Generated PySpark |
|---|---|
| Source Qualifier | Spark DataFrame read |
| Expression | withColumn() |
| Filter | filter() |
| Joiner | join() |
| Lookup | Lookup join |
| Aggregator | groupBy().agg() |
| Target | Delta table write |


Example:

IDMC Expression:

Generated PySpark:

```python
F.upper(
    F.trim(
        F.col("CUSTOMER_NAME")
    )
)

generated_pyspark/

    m_customer_order_summary.py



sample migration flow:



IDMC XML

Containing:

Sources          : 3

Transformations  : 7

Targets          : 1

Links            : 29

Generated PySpark

        |

        v

Executed in Databricks

        |

        v

Delta Target Table

        |

        v

Validation Passed



execution flow:

Notebook

    |

    v

Migration Pipeline

    |

    v

IDMC XML Migration

    |

    v

Generated PySpark Execution

    |

    v

Delta Validation


