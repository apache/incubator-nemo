---
title: Passes and Policies
permalink: /docs/passes_and_policies/
---

### Optimization Passes

The [Nemo IR](../ir) can be flexibly modified, both in its logical structure and annotations, through an interface called *Nemo optimization pass*.
An *optimization pass* is basically a function that takes an *Nemo IR* and outputs an optimized *Nemo IR*.

##### Compile-time passes

The modification during compile-time can be categorized in different ways:

1. **Reshaping passes** modify the shape of the IR itself by inserting, regrouping, or deleting IR vertices and edges on an Nemo IR, such as collecting repetitive vertices inside a single loop or inserting metric vertices. This modifies the logical notion of data processing applications.
2. **Annotating passes** annotate IR vertices and edges with *execution properties* with the provided logic to adjust and run the workload in the fashion that the user wants.
3. **Composite passes** are collections of passes that are grouped together for convenience.

##### Run-time passes

After the compilation and compile-time optimizations, the *Nemo IR* gets laid out as a *physical execution plan* to be submitted to and executed by the *Nemo Execution Runtime*.
While execution, an *run-time optimization pass* can be performed to perform dynamic optimizations, like solving data skew, using runtime statistics.
It takes the old *Nemo IR* and metric data of runtime statistics, and sends the newly optimized Nemo IR to execution runtime for the physical plan to be updated accordingly.

### Examples

Below are some example optimization passes that are used for different use cases:

#### Compile-time passes
**Reshaping passes**:
- Common subexpression elimination (CSE): to refactor the commonly occurring operation that unnecessarily computes multiple times.
- Loop invariant code motion (LICM): to extract an operation that does not need to be repetitively done iteratively from a loop.
- Loop Extraction: to observe the DAG structure and extract the repetitive workflow that can be refactored into a loop.
- Data Skew - Metric vertex insertion: to insert a vertex that indicated where to collect metrics and trigger data skew runtime optimization.

**Annotating passes**:
- Parallelism: to determine computational parallelism of each vertices by observing source data size and parallelism information of previous vertices
- Executor placement: to allocate different computations on specific types of resources.

#### Run-time passes
- Data-skew: to evenly re-distribute skewed data into a more evenly-distributed partitions of data.

### Optimization Policies

An **optimization policy** is composed of a specific combination of optimization passes.

Using a carefully chosen series of *optimization passes*, we can optimize an application to exploit specific *deployement characteristics*, by providing appropriate configurations and plan for the execution runtime.
A complete series of optimization passes is called a *policy*, which together performs a specific goal.

For example, to optimize an application to run on evictable transient resources, we can use a specialized executor placement pass, that places computations appropriately on different types of resources, 
and data flow model pass, that determines the fashion in which each computation should fetch its input data, with a number of other passes for further optimization.

Using different optimization policies for specific goals enables users to flexibly customize and perform data processing for different deployment characteristics.
This greatly simplifies the work by replacing the work of exploring and rewriting system internals for modifying runtime behaviors with a simple process of using pluggable policies.
It also makes it possible for the system to promptly meet new requirements through [easy extension of system capabilities](../extending_Nemo).
