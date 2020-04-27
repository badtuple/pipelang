Pipelang
========

An embedded "Pipe and Filter" language that allows your program's users to query
and transform streams of data.



Design
======


The `Interpreter` struct accepts string queries, parses them into a `Pipeline`
struct and then runs them.

The `Pipeline` struct contains a list of Filters that operate on Data.
