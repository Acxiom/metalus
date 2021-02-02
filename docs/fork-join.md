[Documentation Home](readme.md)

# Fork/Join
The fork and join step types are simple constructs that allow processing a list of data in a loop like construct and then 
joining the results back for further processing.

### Fork
A fork type step allows running a set of steps against a list of data simulating looping behavior. There are two ways
to process the data: *serial* or *parallel*. Serial will process the data one entry at a time, but all values will be 
processed regardless of errors. Parallel will attempt to run each value at the same time depending on the available 
resources. Fork steps may not be embedded inside other fork steps, but multiple fork steps are allowed as long as a
join step provides separation. Fork steps perform no logic, so the "engineMeta" attribute will be ignored. The required 
parameters are:

* **id** - The id of this step in the pipeline
* **type** - This should always be "fork"
* **params** - Two parameters are required:
  * **forkByValues** - should contain a reference to the list of data to process
  * **forkMethod** - must be either *serial* or *parallel*
* **nextStepId** - This should point to the first step to be used when processing each value.

The first step in the set to be used during fork processing should reference the *id* of the fork step to access the 
data from the list.

#### Embedded Fork
As of version 1.8, forks may now be embedded within other forks. A join will be required for each fork, including the
outer fork.

### Join
A join type step is used to **join** the executions of the fork step to continue processing in a linear manner. This step 
type requires a fork step. A join step is not required if all of the remaining steps in the pipeline are to be used to 
process each value in the list. Without a join step, the driver will automatically join the step values and then complete
processing.

Once a join step is encountered, the individual results of the previous steps used for fork processing will be combined 
into a list. The list will wrap the results as options and if there is not a result, *None* will be used. As an example,
if the fork is processing a list containing three elements and the first step returns a boolean based on some criteria, 
then the *primaryReturn* for that step would be a list containing either *None* or *Option[Boolean]*. Each response will 
be stored in the same list position (index) as the data in the original list. The secondary named results will return a 
single map, but the values in the map will be lists which are built identical to the main response.

The join step only requires two parameters:
*  **id** - The id of this step in the pipeline
* **type** - This should always be "join"

Example of a fork/join operation:

![Fork Join Step Overview](images/Fork_Join_Overview.png "Fork Join Step Overview")
