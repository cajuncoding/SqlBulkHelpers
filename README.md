# SqlBulkHelpers
A library for efficient and high performance bulk insert and update of data from C# applications. 
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


### Usage:

Usage is very simple if you use a lightweigth Model (e.g. ORM model via Dapper) to load data from your tables...

```
    //Initialize the Sql Connection Provider (or manually create your own Sql DB Connection...)
    //NOTE: This interface provides a great abstraction that most projects don't take the time to do, 
    //          so it is provided here for convenience (e.g. extremely helpful with DI).
    ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

    //Initialize large list of Data to Insert or Update in a Table
    List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1000);

    //Bulk Inserting is now as easy as:
    //  1) Initialize the DB Connection & Transaction (IDisposable)
    //  2) Instantiate the SqlBulkIdentityHelper class with ORM Model Type...
    //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
    using (SqlConnection conn = await sqlConnectionProvider.NewConnectionAsync())
    using (SqlTransaction transaction = conn.BeginTransaction())
    {
        ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>();

        await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(testData, "TestTableName", transaction);

        transaction.Commit();
    }

```

_**NOTE: More Sample code is provided in the Sample App...**__

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
