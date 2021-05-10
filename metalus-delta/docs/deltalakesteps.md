[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

#DeltaLakeSteps
This object exposes basic steps for working with deltalake tables

##Update Single Column for Deltalake
Updates a single column for a deltalake table. Equivalent to the following sql:
```sql
UPDATE <table> SET <column> = <value> WHERE <condition>
```

* **path** - The path to the deltalake table.
* **column** - The column to update.
* **value** - The value expression to use.
* **condition** - An optional where clause.

##Update Deltalake Table
Updates one or more columns for a deltalake table. Each entry in the ***set*** Map represents a column name and value expression. 
Equivalent to the following sql:
```sql
UPDATE <table>
 SET <key1> = <value1>,
     <key2> = <value2>,
     ...
WHERE <condition>
```

* **path** - The path to the deltalake table.
* **set** - Map of column names and update expressions.
* **condition** - An optional where clause.

##Delete Deltalake Table
Delete records from a deltalake table.
Equivalent to the following sql:
```sql
DELETE FROM <table> WHERE <condition>
```

* **path** - The path to the deltalake table.
* **condition** - An optional where clause.

##Upsert Deltalake Table
Merge a dataFrame with a deltalake table, updating matched columns and insert all others.
Equivalent to the following sql:
```sql
MERGE INTO <target> as <targetAlias>
USING <source> as source
ON <mergeCondition>
WHEN MATCHED [AND <whenMatched>] THEN UPDATE SET *
WHEN NOT MATCHED [AND <whenNotMatched>] THEN INSERT *
```

* **path** - The path to the deltalake table.
* **source** - The source DataFrame to merge into the delta table.
* **mergeCondition** - The the join condition for the merge.
* **sourceAlias** - The alias for the source table. Default is 'source'.
* **targetAlias** - The alias for the delta table. Default is 'target'.
* **whenMatched** - Optional condition for the whenMatched clause.
* **whenNotMatched** - Optional condition for the whenNotMatched clause.

##Upsert Deltalake Table
Merge a dataFrame with a deltalake table.
The whenMatched, deleteWhenMatched, and whenNotMatched parameters use a MatchCondition to represent the conditions/expression pairs.
Equivalent to the following sql:
```sql
MERGE INTO <target> as <targetAlias>
USING <source> as <sourceAlias>
ON <mergeCondition>
WHEN MATCHED [AND <matchCondition>] THEN UPDATE SET [<expressions> | *]
WHEN MATCHED [AND <matchCondition>] THEN DELETE
WHEN NOT MATCHED [AND <matchCondition>] THEN INSERT [<expressions> | *]
```

* **path** - The path to the deltalake table.
* **source** - The source DataFrame to merge into the delta table.
* **mergeCondition** - The the join condition for the merge.
* **sourceAlias** - The alias for the source table. Default is 'source'.
* **targetAlias** - The alias for the delta table. Default is 'target'.
* **whenMatched** - Condition and expression pair for matched records.
* **deleteWhenMatched** - Condition for deleting records when matched.
* **whenNotMatched** - Condition and expression pair for insert records when not matched.

##Vacuum Deltalake Table
Vacuum records from a deltalake table.
Equivalent to the following sql:
```sql
VACUUM <table> [RETAIN <retentionHours> HOURS]
```

* **path** - The path to the deltalake table.
* **retentionHours** - Optional hours of data to retain.

##Get Delta Table History
Get the history dataFrame for a delta table.
Equivalent to the following sql:
```sql
DESCRIBE HISTORY <table> [LIMIT <limit>]
```

* **path** - The path to the deltalake table.
* **limit** - Optional number of previous commands to retrieve.
