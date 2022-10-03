# SqlBulkHelpers
A library for efficient and high performance bulk insert and update of data, into a Sql Database, from C# applications. 
This library leverages the power of the C# SqlBulkCopy classes, while augmenting with the following key benefits:

- Provides a simplified facade for interacting with the SqlBulkCopy API.
  - The Performance improvements have been DRAMATIC!
- Provides enhanced support for ORM (e.g. Dapper) based inserts/updates with the SqlBulkCopy API.
  - It's obviously easy to bulk load data from the DB into model/objects via optimized queries (e.g. Dapper), but inserting and updating is a whole different story).
- Provides support for Database Tables that utilize an Identity Id column as the Primary Key.
  - It dynamically retrieves the new Identity column value and populates them back into the Objects provided so your update will result in your models `automagically` having their PKey (Identity) populated!

The SqlBulkCopy API provides **fantastic performance benefits**, but retrieving the Identity values for Primary Keys that are auto-generated is not a default capability.  And as it turns out, this is not a trivial
task despite the significant benefits it provides for developers.  

Providing support for a table with an Identity Column as the Primary Key is the critical reason for developing this library.  There is alot of good information on Stack Overflow and other web resources that provide
various levels of help for this kind of functionality, but there are few (if any) fully developed solutions to really help others find an efficient way to do this end-to-end.

**So, that was the goal of this library, to provide and end-to-end solution to either a) leverage directly if it fits your use-cases, or b) use as a model to adapt to your use case!**

**I hope that it helps others on their projects as much as it has helped ours.**


## Nuget Package
To use in your project, add the [SqlBulkHelpers NuGet package](https://www.nuget.org/packages/SqlBulkHelpers/) to your project.

## v1.4 Release Notes:
- Due to a documented issue in dependency resolution for netstandard20 projects, such as this, there may now be a dependency breaking issue for some projects that are using the older packages.config approach. You will need to either explicitly reference `LazyCacheLoaders` or update to use the go-forward approach of PackageReferences.
  - See this GitHub Issue from Microsoft for more details: https://github.com/dotnet/standard/issues/481
- Add improved reliability now with use of (LazyCacheHelpers)[https://github.com/cajuncoding/LazyCacheHelpers] library for the in-memory caching of DB Schema Loaders.
  - This now fixes an edge case issue where an Exception could be cached, re-thrown constantly, and re-initialization was ever attempted; exceptions are no longer cached.
- Added support to now clear the DB Schema cache via SqlBulkHelpersSchemaLoaderCache.ClearCache() to enable dynamic re-initialization when needed (vs applicaiton restart).

## v1.3 Release Notes:
- Add improved support for use of `SqlBulkHelpersDbSchemaCache` with new SqlConnection factory func to greatly simplifying its use with optimized deferral of Sql Connection creation (if and only when needed) without having to implement the full Interface.

## v1.2 Release Notes:
- [Merge PR](https://github.com/cajuncoding/SqlBulkHelpers/pull/5) to enable support Fully Qualified Table Names - Thanks to @simelis: 

## v1.1 Release Notes:
- Migrated the library to use `Microsoft.Data.SqlClient` vs legacy `System.Data.SqlClient` which is no longer being 
updated with most improvements, especially performance and edge case bugs. From v1.1 onward we will only use `Microsoft.Data.SqlClient`.

## v1.0.7 Release Notes:
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

### Prior Release Notes:
 - Fixed bug in dynamic initialization of SqlBulkHelpersConnectionProvider and SqlBulkHelpersDBSchemaLoader when not using the Default instances 
that automtically load the connection string from the application configuration setting.
 - Fixed bug in SqlBulk copy and OUTPUT query whereby Sql Server does not return results in the same order of the data
inserted/updated. This will result in erroneous Identity Values being populated after Merge queries are completed.
The OUTPUTS are now intentionally and always sorted in the same order as the input data for data integrity.



### Usage:

### Simple Example:
Usage is very simple if you use a lightweigth Model (e.g. ORM model via Dapper) to load data from your tables...

```csharp
    //Initialize the Sql Connection Provider from the Sql Connection string
    //NOTE: The ISqlBulkHelpersConnectionProvider interface provides a great abstraction that most projects don't
    //          take the time to do, so it is provided here for convenience (e.g. extremely helpful with DI).
    var sqlConnectionString = ConfigurationManager.AppSettings[SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey];
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

    //Initialize large list of Data to Insert or Update in a Table
    List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1000);

    //Bulk Inserting is now as easy as:
    //  1) Initialize the DB Connection & Transaction (IDisposable)
    //  2) Instantiate the SqlBulkIdentityHelper class with ORM Model Type...
    //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
    using (SqlConnection conn = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction transaction = conn.BeginTransaction())
    {
        //The SqlBulkIdentityHelper may be initialized in multiple ways for convenience, though the recommended
        //  way is to provide the ISqlBulkHelpersConnectionProvider for deferred/lazy initialization of the DB Schema Loader.
        //However, if needed an existing Connection (and optional Transaction) may also be used as shown in the 
        //  commented line; in which case if a sql transaction exists then it must be provided to avoid possible 
        //  errors on initial run while initializing the DB Schema Loader internally (which is cached and will occur only once).
        //ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);
        //ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkHelpersDbSchemaLoader);
        ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlConnectionProvider);

        await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(testData, "TestTableName", transaction);

        transaction.Commit();
    }

```

### Initialize the DB Schema Loader and manually manage caching of it:

Alternatively the SqlBulkIdentityHelper may be initalized directly with the DB Schema Loader (which may be managed as a static reference):

```csharp
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

    var sqlBulkDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider);

    ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkHelpersDbSchemaLoader);
```

### Leverage the cached DB Schema info. (e.g. Helpful for Sanitizing Table Names, mitigating Sql injection, etc.)

The `SqlBulkHelpersSchemaLoaderCache` may be utilized directly for advanced processing with the provided DB Schema
with internal static caching already provided

*NOTE: Currently the internal DB Schema caching assumes that there are no dynamic DB Schema changes without an application restart.*

```csharp
    public string GetSanitizedTableName(string tableNameToValidate)
    {
        //Your own implementation to retrieve your connection string or other mechanism for creating new Sql DB Connections...
        //NOTE: The ConnectionString or DB Name make good Unique Identifiers to use.
        var sqlConnectionString = this.GetConnectionStringFromConfiguration();
    
        //The use of the Sql Connection factory func constructor will ensure that there is no Sql Connection created
        //  unless it is required; which is only for the initial load, and once loaded and cached then there is 
        //  no overhead because no connection is needed.
        var dbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(
            sqlConnectionString, 
            () => new SqlConnection(sqlConnectionString)
        );

        var tableDefinition = dbSchemaLoader.GetTableSchemaDefinition(tableNameToValidate);
        if (tableDefinition == null)
            throw new NullReferenceException($"The Table Definition is null and could not be found for the table name specified [{tableNameToValidate}].");

        return tableDefinition.TableFullyQualifiedName;
    }
```


### Explicitly controlling Match Qualifiers for the internal Merge Action:

The default behavior is to use the Identity column as the default field for identifying/resolving a match
during the execution of the internal Sql Server merge query. HOwever, there are advanced use cases where explicit 
control over the matching is needed.

Now custom field qualifiers can be specified for explicit control over the field matching during the execution of the internal merge query.
This helps address some very advanced use cases such as when data is being synchronized from multiple sources and an Identity Column is used
but is not the column by which unique matches should occur, because unique fields from the source system needs to be used instead 
(e.g. the Identity ID value from the source database).  In this case a different field (or set of fields can be manually specified.

Warnging:  If the custom specified fields do not result in unique matches then many rows may be updated as a result.  This also means
that the population of Identity values on the entity models may no longer be as expected. Therefore as a default behavior,
an exception will be thrown if non-unique matches occur (which will allow a transaction to be rolled back).

However, in cases where this is an intentional/expectec (and understood) result then this behaviour may be controlled and
disabled by setting the flag on the `SqlMergeMatchQualifierExpression` class as noted in commented code.

```csharp
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;


    using (var conn = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction transaction = conn.BeginTransaction())
    {     
        ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);

        //Initialize a Match Qualifier Expression field set with one or more Field names that identify
        //  how a match between Model and Table data should be resolved.
        var explicitMatchQualifiers = new SqlMergeMatchQualifierExpression(nameof(TestElement.Key), nameof(TestElement.Value))
        //{
        //    ThrowExceptionIfNonUniqueMatchesOccur = false
        //}

        var results = await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
            testData, 
            TestHelpers.TestTableName, 
            transaction,
            explicitMatchQualifiers
        )
    }

```

_**NOTE: More Sample code is provided in the Sample App and in the Tests Project (as Integration Tests)...**__

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
