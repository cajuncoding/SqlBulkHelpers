# SqlBulkHelpers & MaterializedData
A library for efficient and high performance bulk processing of data with SQL Server in .NET. It greatly simplifies the ordinarily very complex process
of bulk loading data into SQL Server from C# for high performance inserts/updates, and for implementing materialized data patterns with SQL Server from .NET.

This package includes both `SqlBulkdHelpers` and `SqlBulkHelperse.MaterializedData` and can be used in conjunction with other popular SQL Server ORMs such as `Dapper`, `Linq2Sql`, `RepoDB`, etc.
 - _SqlBulkHelpers_ allow the loading of thousands (or tens of thousands) of records in seconds. 
 - The _Materialized Data_ pattern enables easy loading of *offline* staging tables with data and then switching them out to 
replace/publish to Live tables extremely efficiently (milliseconds) so that the Live tables are not blocked during the background data loading process.

**If you like this project and/or use it the please give it a Star 🌟 (c'mon it's free, and it'll help others find the project)!**

### [Buy me a Coffee ☕](https://www.buymeacoffee.com/cajuncoding)
*I'm happy to share with the community, but if you find this useful (e.g for professional use), and are so inclinded,
then I do love-me-some-coffee!*

<a href="https://www.buymeacoffee.com/cajuncoding" target="_blank">
<img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="41" width="174">
</a> 

## SqlBulkHelpers

The `SqlBulkHelpers` component of the library provides the capability to insert and update data with fantastic performance, operating on thousands or tens of thousands of records per second.
This library leverages the power of the C# SqlBulkCopy classes, while augmenting and automating the orchestration, model mapping, etc. with the following key benefits:

- Provides much more simplified facade for interacting with the SqlBulkCopy API now exposed as simple extension methods of the `SqlConnection`/`SqlTransaction` classes.
  - It's obviously easy to bulk query data from the DB into model/objects via optimized queries (e.g. Dapper or RepoDB), but inserting and updating is a whole different story.
  - The Performance improvements are DRAMATIC!
- Provides enhanced support for ORM (e.g. Dapper, LINQ, etc.) based inserts/updates with the SqlBulkCopy API by automatically mapping to/from your data model.
  - Includees support for annotation/attribute mappings from Dapper, LINQ, RepoDB, or the `SqlBulkColumn` & `SqlBulkTable` attributes provided by the library.
- Provides support for Database Tables that utilize an Identity Id column (often used as the Primary Key).
  - SQL Server Identity column values are populated on the server and retrieving the values has always been complex, but is now trivialized.
  - The library dynamically retrieves the new Identity column value and populates them back into the data Models/Objects provided so your insert will result in your models `automagically` having the Identity value from the SQL Server `insert` populated!

The `SqlBulkCopy API`, provided by Microsoft, offers **fantastic performance benefits**, but retrieving the Identity values that are auto-generated from the server is not a default capability.  And as it turns out, this is not a trivial task despite the significant benefits it provides for developers.  

However, a critical reason for developing this library was to provide support for tables with `Identity` column values. There is alot of good information on Stack
Overflow and other web resources that provide various levels of help for this kind of functionality, but there are few (if any) fully developed solutions to really
help others find an efficient way to do this end-to-end. To my knowledge `RepoDB` is the only lightweight/low-level ORM that provides this, so if you are using any other ORM such as Dapper,
then this library can be used in combination!

### Example Usage for Bulk Insert or Update:
```csharp
public class TestDataService
{
    private ISqlBulkHelpersConnectionProvider _sqlConnectionProvider
    public TestDataService(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
    {
        _sqlConnectionProvider = sqlConnectionProvider 
            ?? throw new ArgumentNullException(nameof(sqlConnectionProvider))
    }

    public async Task<IList<TestDataModel>> BulkInsertOrUpdateAsync(IEnumerable<TestDataModel> testData)
    {
        await using (var sqlConn = await _sqlConnectionProvider.NewConnectionAsync())
        await using (var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync())
        {
            var bulkResults = await sqlTransaction.BulkInsertOrUpdateAsync(testData);
            await sqlTransaction.CommitAsync();

            //Return the data that will have updated Identity values (if used).
            return bulkResults;
        }
    }
}
```

## SqlBulkHelpers.MaterializedData

The [*Materialized View pattern*](https://en.wikipedia.org/wiki/Materialized_view) (or Materialized Data) pattern within the context of 
relational databases, such as SQL Server, is often implemented as a readonly replica of remote data in local tables. This can enable massive
performance improvements via highly efficient SQL queries, while at the same time making your application more resilient via [Eventual Consistency](https://en.wikipedia.org/wiki/Eventual_consistency).

This is not a `data sync` per se, because it's only a one-way replication and conceptually the data should be treated as read-only. Though locally
the data may be augmented and extended, however any new data is owned by your system while the original materialized data is conceptually owned
by an external system.

The primary benefit of this pattern is to improve performance and resiliency. For example, if the data is not local then the power of SQL queries
is dramatically hindered because the database is unable to join across data. This means that all data correllation/joining/filtering/etc.
must occur within the application layer -- in-memory processing of the data from disparate sources. Therefore, regardless of whether the data is retrieved by an API (should be the case) or from multiple direct database connections (stop doing this please!), the responsibility to correllate/join/filter/etc. must lie in the application layer for processing and is by definition less efficient and likely poses (ususally signficant) negative implications as follows. 

For most business applications this introduces several major problems:
 - Significant performance impacts due to retrieval of (potentially large) sets of data into application memory and crunching the results via code.
   - Server load is higher, server resource utilization is higher, etc.
   - Code complexity, and ongoing maintenance, is usually much higher than efficient SQL queries would be.
 - There is additional developer impact as much more effort is required to implement data processing in the application layer than can be done with SQL queries.
 - There is a runtime dependency on the external data source. 
   - Whether it's and API or separate database connection the data must be retrieved to be processed and if there are any issues with the external data source(s) (e.g. 
connectivity, errors, etc. then your application will fail. 
 
To be fair, this isn't a new problem however the *Materialized Data/View pattern* is a robust & reliable solution to the problem because it provides a local 
replica of the data than can be used in optimized, and highly efficient, SQL queries. And that data is refreshed periodically so if there are errors in the refresh 
process your live data remains unaffected making your application significantly more resilient. This process of periodic (or event based) updating/refreshing of the 
materialized data creates an [*Eventually Consistent*](https://en.wikipedia.org/wiki/Eventual_consistency) architectural model for this data and helps you attain the benefits therein.

### The simple & naive approach...
A simplistic approach to this would be to have a set of tables in your database, and a .NET applicaiton that runs in the background on a schedule or event based trigger
that refreshed the data in the tables.  You would then have the local data in your database for which you could join into, filter, etc. 
And your performance would be greatly improved! But as your data grows, there are very notable drawbacks to this simple approach of updating the live tables directly.

For small data sets of a couple dozen to a couple hundred records this likely will work without much issue. But if the data set to be materialized is larger
thousands, tens-of-thousands, or millions then you will quickly encounter several problems:
- Clearing the tables via `DELETE FROM [TableName]` is slow as all records are processed individually and the transaction log will grow commensurately -- this is SLOW!!! 
  - The alternative is to use `TRUNCATE TABLE [TableName]` but this will **not work** if you have data integrity constraints such as `Foreign Keys` on the table.
- At the point of initially clearing the table in your Transaction (*you absolutely should be using a Transaction!*) the table is Locked and any/all queries that need this data will be blocked -- this makes them SLOW!!!
- Your not yet done, you still need to re-populate the table.
- Even if you do a Bulk Update --instead of a delete/truncate and re-load -- the Updates will still lock the table blocking other queries, bog down the transaction log, and result in a process that is very SLOW!!!.

### The robust, performant, & resilient approach...
Soo, what's the solution? 
1. Don't load directly into your Live table(s) at all. 
2. Instead load into an offline staging table(s)...
   - Now it doesn't matter if the staging tables are blocked at all as they are offline (copies of live tables) so there is no direct impact to the Live tables other than general database server utilization concerns.
4. Then publish (aka switch in _SQL Server_ terminology) the staging table out wholistically/atomically, and nearly instantly, with the Live table.
   - The performance impact is now reduced to two factors: 1) the time it takes to do the switch (nearly instant), and 2) Any additional time needed to validate data integrity of Foreign Key relationships.

Now, there are multiple ways to do this in SQL Server but only one is really recommended: [Partition Switching](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver16#switch--partition-source_partition_number_expression--to--schema_name--target_table--partition-target_partition_number_expression-)
- The legacy approach was to swap staging/live tables out via a renaming process using `sp_rename()` but this encouters a table lock that cannot be circumvented.
- Then the `SCHEMA TRANSFER` approach became an option but it has even worse blocking issues as it takes a SCHEMA LOCK blocking EVERRRRRYTHING.
- As of SQL Server 2016 though we now have `SWITCH PARTITION` which provides control over the blocking/locking and gives flexible implementation.
  - The main benefit we gain with table switching is that now we can reduce the switchign lock priority to allow other queries to attain higher priority and finish 
running before the SWITCH executes -- thereby allowing the SWITCH to occur as a secondard event rather than the other way around (which would block the applications queries).

However the implementatino of this often requires tremebdiys complexity, duplication of table schemas, etc. This is where the .NET library comes into play
by automating the orchestration of the table shell game we have to play in SQL Server -- making it massively easier to implement from our applications.

The way this works is that the .NET code dynamically loads the complete/full table Schema via `GetTableSchemaDefinitionAsync()` and
then uses the schema to clone the table. The cloned table includes all `Primary Keys`, `Foreign Keys`, `Indexes`, `Computed Columns`, `Identity Columns`, `Column Constraints`, and even `Full Text Indexes`.

With this exact clone the api returns the metadata you need in the `MaterializeDataContext`, that is used to process the load into the staging table via the dynamically generated table name exposed by `MaterializationTableInfo.LoadingTable`.

The `MaterializeDataContext` also provides other information about the process. This allows you to handle many tables in a single batch because you will likely have 
multiple related data-sets that need to be loaded at the same time for data integrity and relational integrity. However, it is 100% your responsibility to ensure that
you sanitize and validate the data integrity of all tables being loaded in the materialization context. You need to ensure the data is valid or the load may fail with exceptions; but your Live table data will remain unaffeced as the entire process is fully transactional (with the exception of _Full Text Indexes_; see below).

_**NOTE:** During this process normal referential integrity constraints such as FKeys are disabled because they may
refer to other live tables, and vice versa, making it impossible to load your tables. These elements will be re-enabled and validated when
the final switch occurs. This is the most significant impact to switching the live table out -- the data integrity must be validated which may take some time (seconds) depending on your data._

_That's why it's recommended to keep materialized data somewhat separated from your own data, and possibly minimize the FKey references where possible --
but this is a design decision for your schema and use case. There is no magic way to eliminate the need to validate referential integrity in the 
world of relational databases... unless you just don't care; in which case just delete your FKeys._ 😜

_**WARNING:** **Full Text Indexes** cannot be disabled, dropped, or re-created within a SQL Transaction and therefore must be managed outside of the Transaction
that contains all other schema changes, data loading, and table switching process. The library cant automate this for you but this is disabled by default. If enabled (see below) we implement error handling and then attempt to drop the Full Text Index, as late as possible but, immediately before the table switch (on a separate isolated Connection to the DB), and then restore it immediately after. The error handling ensures that, upon any exception, the Full Text Index will be re-created if it was dropped; but there is a non-zero chance that the attempt to re-create could fail if there are connectivity or other impacting issues with the database server._

_Since this functionality is disabled by default it must be enabled via `SqlBulkHelpersConfig.EnableConcurrentSqlConnectionProcessing(...,enableFullTextIndexHandling: true)`._

_To minimize the risk of issues dropping/re-creating the FullTextIndex, it is done on a separate connection so that it can be recovered in the case of
any issues, therefore it requires the use of Concurrent Sql Connections via a `Func<SqlConnection>` connection factory or `ISqlBulkHelpersConnectionProvider` implementation._


## Example Usage for Materializing Data:
NOTE: Use the [Configuration](#example-configuration-of-defaults) above to improve performance -- particularly when initially loading table schemas for multiple tables (which are cached after initial load).

The easiest implementation (for most use cases) is to call the `SqlConnection.ExecuteMaterializeDataProcessAsync()` extension method providing a lambda function
that process all data; likely using the [SqlBulkHelpers](#SqlBulkHelpers) to bulk insert the data into the loading tables. This process will create and provide the SQL Transaction for you to use in your lambda function; the internal management of the transaction is what allows the framework to leverage multiple connections, and handle edge cases such as Full Text Indexes (when enabled).

Once the Lambda function is finished then the Materialization data process will automatically complete the following:
  - Switching out all loading tables for live tables.
  - Executing all necessary data integrity checks and enabling FKey constraints, etc.
  - Clean up all tables leaving only the Live tables ready to be used.
  - Committing the Transaction (_NOTE: you cannot do this in your Lambda or the process will fail; see below_).

_**IMPORTANT NOTE:** In the data processing lambda function the SqlTransaction is provided to you (not created by you), so it's critical
that you **DO NOT** Commit the Transaction yourself or else the rest of the Materialization Process cannot complete; this would be similar 
to pre-disposing of a resource that higher level code expectes to remain valid._

```csharp
public class TestDataService
{
    private ISqlBulkHelpersConnectionProvider _sqlConnectionProvider
    public TestDataService(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
    {
        _sqlConnectionProvider = sqlConnectionProvider 
            ?? throw new ArgumentNullException(nameof(sqlConnectionProvider))
    }

    public Task ExecuteMaterializeDataProcessAsync(IList<TestDataModel> parentTestData, IList<TestChildDataModel> childTestData)
    {
        var tableNames = new[]
        {
            "TestDataModelsTable",
            "TestChildRelatedDataModelsTable"
        };

        //We need a valid SqlConnection
        await using (var sqlConn = await _sqlConnectionProvider.NewConnectionAsync())
        {
            //Now we can initialize the Materialized Data context with clones of all tables ready for loading...
            //This takes in an async Lambda Func<IMaterializeDataContext, SqlTransaction, Task> that handles all data processing..
            await sqlConn.ExecuteMaterializeDataProcessAsync(tableNames, async (materializeDataContext, sqlTransaction) =>
            {
                //Note: We must override the table name to sure we Bulk Insert/Update into our Loading table and not the Live table...
                //Note: The cloned loading tables will be empty, so we only need to Bulk Insert our new data...
                var parentLoadingTableName = materializeDataContext.GetLoadingTableName("TestDataModelsTable");
                var parentResults = await sqlTransaction.BulkInsertAsync(parentTestData, tableName: parentLoadingTableName);

                var childLoadingTableName = materializeDataContext.GetLoadingTableName("TestChildRelatedDataModelsTable");
                var childResults = await sqlTransaction.BulkInsertAsync(childTestData, tableName: childLoadingTableName);

                //IMPORTANT NOTE: DO NOT Commit the Transaction here or else the rest of the Materialization Process cannot complete...
                //  Since it was provided to you (not created by you) it will automatically be committed for you at the conclusion 
                //  of the Materialized Data process!
            });

            //Once the Lambda function is finished then the Materialization data process will automatically complete 
            //  the table switching, re-enabling of referential integrity, and cleanup...
        }
    }
}

```


## Nuget Package
To use in your project, add the [SqlBulkHelpers NuGet package](https://www.nuget.org/packages/SqlBulkHelpers/) to your project.

### v2.4.2 Release Notes:
- Add Support to manually control if Materialized Loading tables are cleaned-up/removed when using `SchemaCopyMode.OutsideTransactionAvoidSchemaLocks` via `materializeDataContext.DisableMaterializedStagingTableCleanup()`;
 always enabled by default and throws an `InvalidOperationException` if if SchemaCopyMode.InsideTransactionAllowSchemaLocks is used. This provides support for advanced debugging and control flow support.
- Improved SqlBulkHelpers Configuration API to now provide Clone() and Configure() methods to more easily copy/clone existing configuration and change values is specific instances;
 including copy/clone of the Defaults for unique exeuctions.

### v2.4.1 Release Notes:
- Added support to load Table Schema for Temp Tables (basic Schema details needed for BulkInsert or Update, etc. to allow Bulk Loading Temp Tables!
- Improved Error message for when custom SQL Merge Match qualifiers are specified but DB Schema may have changed making them invalid or missing from Cached schema.

### v2.4.0 Release Notes:
- Added new explicit CopyTableDataAsync() APIs which enable explicit copying of data between two tables on matching columns (automatically detected by column Name and Data Type).
- Added new Materialized Data Configuration value MaterializedDataLoadingTableDataCopyMode to control whether the materialized data process automatically copies data into the Loading Tables after cloning. 
 This helps to greatly simplify new use cases where data must be merged (and preserved) during the materialization process.

## v2.3.1 Release Notes:
- Fixed bug with Sql Bulk Insert/Update processing with Model Properties that have mapped database names via mapping attribute (e.g. [SqlBulkColumn("")], [Map("")], [Column("")], etc.).

## v2.3 Release Notes:
- Changed default behaviour to no longer clone tables/schema inside a Transaction which creates a full Schema Lock -- as this greatly impacts Schema aware ORMs such as SqlBulkHelpers, RepoDb, etc.
  - Note: If you are manually orchestrating your process using StartMaterializedDataProcessAsync() and FinishMaterializeDataProcessAsync() then you now need to handle this by explicitly calling CleanupMaterializeDataProcessAsync() in a Try/Finally.
- Added new configuration value to control if Schema copying/cloning (for Loading Tables) is inside or outide the Transaction (e.g. SchemaCopyMode.InsideTransactionAllowSchemaLocks vs OutsideTransactionAvoidSchemaLocks).
- Fix bug in ReSeedTableIdentityValueWithMaxIdAsync() when the Table is Empty so that it now defaults to value of 1.


## v2.2.2 Release Notes:
- Improved namespace for SqlBulkHelpers.CustomExtensions to reduce risk of conflicts with similar existing extensions.

## v2.2.1 Release Notes:
- Restored support for SqlConnection Factory (simplified now as a Func&lt;SqlConnection&gt; when manually using the SqlDbSchemaLoader to dynamically retrieve Table Schema definitions for performance.

## v2.2 Release Notes:
- Added support for other Identity column data types including (INT, BIGINT, SMALLINT, & TINYINT); per [feature request here](https://github.com/cajuncoding/SqlBulkHelpers/issues/10).
- Added support to explicitly set Identity Values (aka SET IDENTITY_INSERT ON) via new `enableIdentityInsert` api parameter. 
- Added support to retreive and re-seed (aka set) the current Identity Value on a given table via new apis in the MaterializedData helpers.
- Additional small bug fixes and optimiaztions.


## v2.1 Release Notes:
- Added additional convenience methods to the `MaterializationContext` to retreive loading table info for models mapped via annotations (ModelType; vs only ordinal or string name).
- Added support to cancel the materialization process via new `MaterializationContext.CancelMaterializationProcess()` method; allows passive cancelling without the need to throw an exception to safely stop the process.
- Fixed small configuration initialization bugs when manually setting the `IsFullTextIndexHandlingEnabled` flag.
- Fixed small bug where default configuration was not being used as the fallback.


## v2.0 Release Notes:
- v2.0 release includes the NEW `MaterializeData` Helpers to make it significantly easier to implement highly efficient loading and publishing of materialized data with SQL Server.
  - The concept of Materializing data (design pattern) here is to provide easy aysnc (background) bulk loading of data with, no impact to Live tables, until the data is switched out extremely quickly accomplishing a refresh of Live data in milliseconds.
  - The all new APIs include (but are not limited to): `ExecuteMaterializeDataProcessAsync()`, `GetTableSchemaDefinition()`, `ClearTablesAsync()`, `CloneTablesAsync()`, `DropTablesAsync()`, etc.
- v2.0 provides a simplified and easier to access API via extension methods of the `SqlTransaction` class
  - This is a breaking change for Sql Bulk Insert/Update/etc, but should be very easy to migrate to!
- v2.0 Now includes support for Model mapping attributes for Table/Columns with support for RepoDB (`[Map]`), Linq2Sql (`[Table]/[Column]`), Dapper (`[Table]/[Column])`, or the built in `[SqlBulkTable]/[SqlBulkColumn]`.
  - This means you don't have to change existing model annotations if your using Dapper/Linq/RepoDB, but otherwise have the flexibility to add using hte built in attributes.
- Many performance improvements to further mitigate any reflection performance impacts, as well as minimized memory footprint.
- Improved configuration capabilities and support for Timeouts and add support for Default DB Schema Loader timeout setting.
- v2.0 now works with all tables, regardless if `Identity` Column is used or not. 
    - If no Identity Column is used then the configured PKey columns are dynamically resolved and used for uniqueness unless overridden by providing your own `SqlMergeMatchQualifierExpression` to the method.

NOTE: [Prior Release Notes are below...](#prior-release-notes)

### Additional Examples:

### Simple Example for Sql Bulk Insert or Update :
Usage is very simple if you use a lightweigth Model (e.g. ORM model via Dapper) to load data from your tables...

```csharp
    //Initialize the Sql Connection Provider from the Sql Connection string -- Usually provided by Dependency Injection...
    //NOTE: The ISqlBulkHelpersConnectionProvider interface provides a great abstraction that most projects don't
    //          take the time to do, so it is provided here for convenience (e.g. extremely helpful with DI).
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    //Initialize large list of Data to Insert or Update in a Table
    List<TestDataModel> testData = SqlBulkHelpersSample.CreateTestData(1000);

    //Bulk Inserting is now as easy as:
    //  1) Initialize the DB Connection & Transaction (IDisposable)
    //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
    using (SqlConnection sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await conn.BeginTransactionAsync())
    {
        //Uses the class name or it's annotated/attribute mapped name...
        var results = await sqlTransaction.BulkInsertOrUpdateAsync(testData);

        //Or insert into a different table by overriding the table name...
        var secondResults = await sqlTransaction.BulkInsertOrUpdateAsync(testData, "SecondTestTableName");
        
        //Don't forget to commit the changes...
        await sqlTransaction.CommitAsync();
    }

```

### Example Data Model Table/Column name mapping via Annotations
NOTE: The fully qualified format isn't strictly required, but is encouraged; the `[dbo]` schema will be used as default if not specified.

*WARNING: Do NOT use `.` in your Schema or Table Names... this is not only a really bad sql code smell, it will break the parsing logic.*

```csharp
    [SqlBulkTable("[dbo].[TestDataModelMappedTableName]")] //Built-in
    //[Table("[dbo].[TestDataModelMappedTableName]")] -- Dapper/Linq2Sql
    //[Map("[dbo].[TestDataModelMappedTableName]")] -- RepoDB
    public class TestDataModel
    {
        [SqlBulkColumn("MyColumnMappedName")] //Built-in
        //[Column(""MyColumnMappedName"")] -- Dapper/Linq2Sql
        //[Map("MyColumnMappedName")] -- RepoDB
        public string Column1 { get; set; }
        
        public int Column2 { get; set; }
    }

```


### Configuration of Defaults:

You can pass in the SqlBulkHelpersConfig to all api methods, however to simplify the applicaiton you can also configure the defaults
globally in your application startup.

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    //Update the Default configuration for SqlBulkHelpers
    SqlBulkHelpersConfig.ConfigureDefaults(config =>  {
        
        //Configure the default behavior so that SqlBulkHelpersConfig doesn't have to be passed to every method call...
        config.MaterializeDataStructureProcessingTimeoutSeconds = 60;
        config.DbSchemaLoaderQueryTimeoutSeconds = 100;
        config.IsSqlBulkTableLockEnabled = true;
        config.SqlBulkCopyOptions = SqlBulkCopyOptions.Default;
        config.SqlBulkBatchSize = 5000;
        // ...etc...
        
        //One new optimization (mainly for Materialized Data) is to enable support for concurrent connections...
        //  via a Connection Factory which Enables Concurrent Connection Processing for Performance...
        config.EnableConcurrentSqlConnectionProcessing(sqlConnectionProvider, maxConcurrentConnections: 5);
    });
```

### Retrieve Table Schema Definitions:

The Table Schema definition can be used directly and is extremely helpful for for sanitizing Table Names, mitigating Sql injection, etc.
It offers ability to retrieve basic or extended details; both of which are internally cached.
 - Basic schema details includes table name, columns & their data types, etc. 
 - Extended details includes FKey constraintes, Indexes, relationship details, etc.

*NOTE: The internal schema caching can be invalidated using the `forceCacheReload` method parameter.*

NOTE: You man use an existing SqlConnection and/or SqlTransaction with this api, however for maximum performance it's recommended to 
use a SqlConnection Factory Func so connections are not created at all if the results are already cached...
```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    private readonly ISqlBulkHelpersConnectionProvider _sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    public async Task<string> GetSanitizedTableName(string tableNameToValidate)
    {
        //We can get the basic or extended (slower query) schema details for the table (both types are cached)...
        //NOTE: Basic details includes table name, columns, data types, etc. while Extended details includes FKey constraintes, 
        //      Indexes, relationship details, etc.
        //NOTE: This is cached, so no DB call is made if it's already been loaded and the forceCacheReload flag is not set to true.
        var tableDefinition = await sqlConnection.GetTableSchemaDefinitionAsync(
            tablNameToValidate, 
            TableSchemaDetailLevel.BasicDetails
            async () => await _sqlConnectionProvider.NewConnectionAsync()
        );

        if (tableDefinition == null)
            throw new NullReferenceException($"The Table Definition is null and could not be found for the table name specified [{tableNameToValidate}].");

        return tableDefinition.TableFullyQualifiedName;
    }
```

### Explicitly controlling Match Qualifiers for the internal Merge Action:

The default behavior is to use the PKey column(s) (often an Identity column) as the default fields for identifying/resolving a unique matches
during the execution of the internal SQL Server merge query. However, there are advanced use cases where explicit  control over the matching may be needed.

Custom field qualifiers can be specified for explicit control over the record matching during the execution of the internal merge query.
This helps address some very advanced use cases such as when data is being synchronized from multiple sources and an Identity Column is used
in the destination, but is not the column by which unique matches should occur since data is coming from different systems and unique fields 
from the source system(s) needs to be used instead (e.g. the Identity ID value from the source database not the target).  
In this case a different field (or set of fields can be manually specified.

**Warnging:**  _If the custom specified fields do not result in unique matches then multiple rows may be updated resulting in unexpected data changes. 
This also means that the retrieval of Identity values to populate in the Data models may not work as expected. Therefore as a default behavior,
an exception will be thrown if non-unique matches occur (which will allow a transaction to be rolled back) to maintain data integrity._

_However, in cases where this is an intentional/expected (and understood) result then this behaviour may be controlled and
disabled by setting the `SqlMergeMatchQualifierExpression.ThrowExceptionIfNonUniqueMatchesOccur = false` flag as noted in commented code._

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    using (var sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync())
    {     
        //Initialize a Match Qualifier Expression field set with one or more Column names that identify how a match 
        //  between Model and Table data should be resolved during the internal merge query for Inserts, updates, or both...
        var explicitMatchQualifiers = new SqlMergeMatchQualifierExpression("ColumnName1", "ColumnName2");
        //{
        //    ThrowExceptionIfNonUniqueMatchesOccur = false
        //};

        var results = await sqlTransaction.BulkInsertOrUpdateAsync(testData, matchQualifierExpressionParam: explicitMatchQualifiers);
        await sqlTransaction.CommitAsync();
    }

```

### Explicitly setting Identity Values (aka SET IDENTITY_INSERT ON):
The normal process is for Identity values to be incrmented & set by SQL Server. However there are edge cases where you may need
to explicitly specify the Identity values and have those be set. An example of this may be if data was archived/backed-up elsewhere and
now needs to be restored; it's original Identity value may still be valid and need to be used for referential integrity.

This can now be done by simply specifying `enableIdentityInsert = true` parameter on the Bulk API calls as shown below...

**Warnging:**  _It is your responsibility to test and validate your Identity values are valid on the Model; SQL Server may enforce
uniqueness if they are the PKey, etc. however bad data like default int value of Zero, or negative values may be saved with this feature._

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    using (var sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync())
    {     
        //Will send the actual value of Identity ID property to be stored in the Database because enableIdentityInsert is true!
        var results = await sqlTransaction.BulkInsertOrUpdateAsync(testData, enableIdentityInsert: true);

        //Don't forget to commit the changes...
        await sqlTransaction.CommitAsync();
    }

```

### Clearing Tables (Truncate even when you have FKey constraints!)
Normally if your table has FKey constraints you cannot Truncate it... by leveraging the materialized data api we can efficiently clear
the table even with these constraints by simply switching it out for an empty table! And, this is still fully transactionally safe!

Data integrity will still be enforced after the tables are cleared so it's the responsibility of the caller to ensure that all appropriate tables
get cleared in the same transactional batch so that data integrity is maintained; otherwise FKey failures could occcur when the materialization process completes. 
The value here is that you don't have to remove and re-add your FKeys (manually or otherwise), the process is fully automated and simplified and no tables outside of
the batch are altered in any way.

NOTE: These methods can also be used with data models that have mapped table names via annotations using the Generic overloads `ClearTableAsync<TestDataModel>()`.

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    using (var sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync())
    {     
        //Clear the table and force bypassing of constraints...
        var clearTableResult = await sqlTransaction.ClearTableAsync("Table1", forceOverrideOfConstraints: true);

        //OR We can clear a series of table in one batch with ability to force override of table constraints...
        var clearTableResults = await sqlTransaction.ClearTablesAsync(
            new string[] { "Table1", "Table2", "Table3" }, 
            forceOverrideOfConstraints: true
        );
        
        //Don't forgot to commit the changes!
        await sqlTransaction.CommitAsync();
    }
```

### Cloning Tables
As a core feature of the materialized data process we must be able to clone tables, and you can leverage this api directly for
other advanced functionality.

NOTE: These methods can also be used with data models that have mapped table names via annotations using the Generic overloads `CloneTableAsync<TestDataModel>()`.

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    using (var sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync())
    {     
        //Clone any table you like... 
        //NOTE: TargetTableName is optional and a unique name will be dynamically used if not specified.
        var resultCloneInfo = await sqlTrans.CloneTableAsync(
            sourceTableName: "TableName", 
            targetTableName: "NewTableName", 
            recreateIfExists: true
        )

        //OR we can clone many tables at once in a single batch...
        //NOTE: The TargetTableName params are optional and a unique names will be dynamically used if not specified.
        var batchCloneResults = await sqlTrans.CloneTableAsync(
            new[] { 
                CloneTableInfo.From("TableName1", "NewTableName1"),
                CloneTableInfo.From("TableName2", "NewTableName2") 
            }, 
            recreateIfExists: true
        )
        
        //Don't forgot to commit the changes!
        await sqlTransaction.CommitAsync();
    }
```

_**NOTE: More Sample code is provided in the Sample App and in the Tests Project Integration Tests...**__

### Retrieve & Set the Current Identity ID Valud (Seed value)...
For edge cases it may be very helpful to both retrieve and/or set/re-seed the current Identity Value of a table. This can be done
easily with the helper apis as shown:

```csharp
    //Normally would be provided by Dependency Injection...
    //This is a DI friendly connection factory/provider pattern that can be used...
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    using (var sqlConnection = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync())
    {     
        //Clear the table and force bypassing of constraints...
        var currentIdentityValue = await sqlTrans.GetTableCurrentIdentityValueAsync("Table1");

        //OR using Model with Table Mapping Annotations...
        var currentIdentityValueForModel = await sqlTrans.GetTableCurrentIdentityValueAsync<Table1Model>();
        
        //Now we can explicitly re-seed the value in Sql Server similarly...
        int newIdentitySeedValue = 12345;
        await sqlTrans.ReSeedTableIdentityValueAsync("Table1", newIdentitySeedValue);

        //OR similarly using a Model with Table Mapping Annotations...
        await sqlTrans.ReSeedTableIdentityValueAsync<Table1Model>(newIdentitySeedValue);

        //Don't forgot to commit the changes!
        await sqlTransaction.CommitAsync();
    }
```

## Prior Release Notes

### v1.4 Release Notes:
- Add improved reliability now with use of [LazyCacheHelpers](https://github.com/cajuncoding/LazyCacheHelpers) library for the in-memory caching of DB Schema Loaders.
  - This now fixes an edge case issue where an Exception could be cached, re-thrown constantly, and re-initialization was ever attempted; exceptions are no longer cached.
  - Due to a documented issue in dependency resolution for netstandard20 projects, such as this, there may now be a dependency breaking issue for some projects that are using the older packages.config approach. You will need to either explicitly reference `LazyCacheHelpers` or update to use the go-forward approach of PackageReferences.
    - See this GitHub Issue from Microsoft for more details: https://github.com/dotnet/standard/issues/481
- Added support to now clear the DB Schema cache via SqlBulkHelpersSchemaLoaderCache.ClearCache() to enable dynamic re-initialization when needed (vs applicaiton restart).

### v1.3 Release Notes:
- Add improved support for use of `SqlBulkHelpersDbSchemaCache` with new SqlConnection factory func to greatly simplifying its use with optimized deferral of Sql Connection creation (if and only when needed) without having to implement the full Interface.

### v1.2 Release Notes:
- [Merge PR](https://github.com/cajuncoding/SqlBulkHelpers/pull/5) to enable support Fully Qualified Table Names - Thanks to @simelis: 

### v1.1 Release Notes:
- Migrated the library to use `Microsoft.Data.SqlClient` vs legacy `System.Data.SqlClient` which is no longer being 
updated with most improvements, especially performance and edge case bugs. From v1.1 onward we will only use `Microsoft.Data.SqlClient`.

### v1.0.7 Release Notes:
- Added support to optimize Identity value updates with native performance (no reflection) simply by implementing 
`ISqlBulkHelperIdentitySetter` on the model classes.

### v1.0.6.2 Release Notes:
- Fix to correctly support fully qualified table names with schema.
- Ensure BulkCopy also uses timeout parameter.
- Add Overloads for easier initialization of SqlMergeMatchQualifierExpression class.
- Breaking change; removed internal Default static references so that I can eliminate any dependency on Configuration framework for better support in .Net Core; New Caching helper and simple constructor with Sql Connection String replace this.
- Added support to specify SqlCommand Timeout Seconds when initializing the Sql Bulk Helper class, instead of relying on the connection wide setting from the Connection string.
- Added .Net Core Console Sample App for validation/testing.

### v1.0.5 Release Notes:
- Added support for custom match qualifiers to be specified even if bulk inserting/updating data with Identity columns.
  - This addresses some edge use cases such as data synchronization logic which may merge data from multiple sources, 
and Identity Values are used to differentiate data from multiple sources, but the actual merge matches needs to occur on 
other unique fields of the source system (not the Identity column of the Target table).
- Simplified initialization and constructors to provide easier use -- especially if SqlConnection/SqlTransaction already exists 
and ConnectionString is not available.  
  - It's still recommended to use ISqlBulkHelpersConnectionProvider however, this may not be congruent with existing code bases 
so now the use of existing Sql Connection & Transaction is encapsulated and can much more conveniently be used 
(this was primarily based on user feedback from others 😉). 
- Provided in-memory cache implementation to help manage caching of Schema DB Loaders for performance.
  - Previously it was possible that the DB Schema Loader was being re-loaded multiple times unnecessarily due to relying on internal behavior.
  - This was missing from initial versions because I assumed that the Static DB Loader would be managed by users as as a static/singleton
but this may not be the case, therefore this is now greatly simplified with an (encapsulated) caching implementation that is now provided out-of-the-box.
- Added more Integration Tests for Constructors and Connections, as well as the new DB Schema Loader caching implementation.

### Older Release Notes:
 - Fixed bug in dynamic initialization of SqlBulkHelpersConnectionProvider and SqlBulkHelpersDBSchemaLoader when not using the Default instances 
that automtically load the connection string from the application configuration setting.
 - Fixed bug in SqlBulk copy and OUTPUT query whereby Sql Server does not return results in the same order of the data
inserted/updated. This will result in erroneous Identity Values being populated after Merge queries are completed.
The OUTPUTS are now intentionally and always sorted in the same order as the input data for data integrity.


```
  
MIT License

Copyright (c) 2019 - Brandon Bernard

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

```
