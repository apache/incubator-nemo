---
title: Compiler Design
permalink: /docs/compiler_design/
---

### Overview

Compiler takes an arbitrary dataflow program as input, and outputs an optimized physical execution plan to be understood by the execution runtime. The steps are as followings:

1. **Compiler frontend** first translates the logical layer of given dataflow program written in high-level languages, like Apache Beam, into an expressive, general-purpose [Nemo Intermediate Representation (IR)](../ir).
2. Then using the [optimization pass](../passes_and_policies) interface provided by the **Compiler optimizer**, the IR can be flexibly reshaped and annotated with a variety of execution properties that configures the underlying runtime behaviors.
3. After being processed by _optimization passes_, the **Compiler backend** finally lays out the IR into a physical execution plan, composed of tasks and stages, to be carried out by the [Nemo Execution Runtime](../runtime_design).

### Frontend

The frontend of *Nemo Compiler* translates arbitrary high-level dataflow languages, like Apache Beam, into our expression of [Nemo IR](../ir) with an elementary annotation of default *execution properties*.
**Frontend** for different languages are designed as visitors that traverse given applications written in high-level dataflow languages in a topological order.
While traversing the logic, it translates each dataflow operators and edges on the way, and appends the translated IR components to the *Nemo IR builder*.
After completing the traversal, the IR builder builds the logical part of the IR after checking its integrity.
Integrity check ensures a few factors, such as ensuring vertices without any incoming edges to read source data.

### Optimizer

After the IR is created with its logical structures set up, we need an [Nemo policy](../passes_and_policies) to optimize the application for a specific goal.
To build Nemo policies safely and correctly, we provide a *policy builder* interface, which checks for the integrity while registering series of passes in a specific order.

For example, if an annotating pass requires information of specific *execution properties* to perform its work, we specify them as *prerequisite execution properties*, and check the order and the content of registered passes to ensure that the conditions have been met.
We avoid the cases where circular dependencies occur, through the default execution properties that we provide at the initiation of the Nemo IR.

Using the policy, the optimizer applies each *optimization passes* one-by-one in the provided order, and checks for the IR integrity after each optimization has been done, to ensure that the [IR](../ir) is not broken.

### Backend

After the optimizations have been applied, **Compiler backend** finally traverses and lays out the IR into a physical execution plan, which is understood by [Execution Runtime](../runtime_design).
In the backend, vertices annotated with the same stage numbers are grouped into stages, to be concurrently run in a distributed fashion, and are expressed in a form of tasks.
The generated *physical execution plan* composed of tasks, task groups (stages), and the data dependency information between them is then submitted to [Execution Runtime](../runtime_design) to be scheduled and executed.
