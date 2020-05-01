Pipelang
========

An embedded "Pipe and Filter" language that allows your program's users to query
and transform streams of data.


## Example Queries

Take all data from the "@sensor" source, filter out every number in the stream that is less than or equal to 4, and then double it:

```
@sensor | greater_than(4) | double
```

## Design


The `Interpreter` struct:
	* Allows registration of Filters that will be used in Pipelines
	* Accepts Queries and builds Pipelines out of them
	* Accepts Data for a pipeline
	* Runs the pipeline to process that data.

The `Pipeline` struct contains a list of Filters that operate on Data.
The host program decides when to have the Pipeline process the Data.
