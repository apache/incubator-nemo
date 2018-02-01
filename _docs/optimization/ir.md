---
title: Coral Intermediate Representation (IR)
permalink: /docs/ir/
---

### Overview

IR is an abstraction that we use to express the logical notion of data processing applications and the underlying execution runtime behaviors on separate layers.
It basically takes a form of directed acyclic graphs (DAGs), with which we can logically express dataflow programs.
To express various different execution properties to fully exploit different deployment characteristics, we enable flexible annotations to the IR on a separate layer.
On that layer, we can annotate specific execution properties related to the IR component.

### IR structure

Coral IR is composed of vertices, which each represent a data-parallel operator that transforms data, and edges between them, which each represents the dependency of data flow between the vertices.
Coral IR supports four different types of IR vertices:

- **UDF Vertex**: Most commonly used vertex. Each UDF vertex contains a transform which determines the actions to take for the given input data. A transform can express any kind of data processing operation that high-level languages articulate.
- **Source Vertex**: This produces data by reading from an arbitrary source like disks and distributed filesystems.
- **Metric Vertex**: Metric vertex collects and emits metric data.
- **Loop Vertex**: Loop vertex is used to express iterative workflows, summarizing the part of the IR that occurs repetitively due to iterations. This comes very useful when expressing, controlling, and optimizing iterative workloads like MLR. It also effectively summarizes workloads.

### IR annotation

Each IR vertex and edge can be annotated to be able to express the different _execution properties_.
For example, edges that the user wants to store intermediate data as local files can be annotated to use the 'local file' module for the 'Data Store' execution property.
_Execution properties_ that can be configured for IR vertices include _parallelism, executor placement, stage number_, and _schedule group_, as they are related to the computation itself.
For IR edges, it includes _data store, data flow model, data communication pattern_, and _partitioning_, as they are used for expressing the behaviors regarding data transfer.
By having an IR for expressing workloads and the related execution properties, it enables the optimization phase to be decoupled, making it easier to implement and plug in different optimizations for different _deployment characteristics_.
