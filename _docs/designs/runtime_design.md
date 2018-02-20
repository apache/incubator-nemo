---
title: Runtime Design
permalink: /docs/runtime_design/
---

### Receiving a Job from the Nemo Compiler

After the compiler goes through a set of passes for optimization, the optimized Nemo IR is translated into into a 
physical form for the execution runtime to execute. This involves translations like expanding an operator annotated 
with parallelism in Nemo IR to the desired number of tasks and connecting the tasks according to the data communication 
patterns annotated on the IR edges. Physical execution plan is also in the form of a DAG, with the same values annotated 
for execution properties as the given IR DAG if necessary. Nemo IR DAG and physical execution plan can be translated 
from one another by sharing the identifiers.

### Runtime Architecture
The Nemo runtime consists of a _RuntimeMaster_ and multiple _Executors_.
_RuntimeMaster_ takes the submitted physical execution plan and schedules each _TaskGroup_ to _Executor_ for execution.

The figure below shows the Nemo runtime's overall architecture.
Our runtime's components can be broken down into two parts, the processing backbone and the extensible modules.

The processing backbone illustrated by the blue double stroked boxes in the figure below,
implements the inherent and basic code that must be executed for all Nemo jobs
(and potentially all data processing jobs). 
The code includes references to the flexible and extensible data structures 
representing our execution properties. 
For example, "executor placement" is mainly controlled in ContainerManager as an extensible map.

The extensible modules illustrated by the red dashed boxes in the figure below,
are interfaces which users can implement in order to get them to behave the way the user wants.
Each interface has been transparently integrated with other components of the runtime.

![image]({{site.baseurl}}/assets/runtime_arch.png)


### Dictionary
* Stage: A unit of execution the runtime uses for scheduling the job.
* TaskGroup: A computation unit composed of one or more tasks that can be computed in a single executor.
* Block: The unit of data output by a single task.
* Partition: A block consists of one or more partitions, depending on the _Partitioner_ choice.



