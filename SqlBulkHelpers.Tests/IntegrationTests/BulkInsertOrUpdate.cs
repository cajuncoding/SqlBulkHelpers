using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class BulkInsertOrUpdateTests
    {
        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithCustomMatchQualifiersAsync()
        {
            List<TestElement> testData = TestHelpers.CreateTestData(10);
            foreach (var t in testData)
            {
                t.Key = $"CUSTOM_QUALIFIER_BY_VALUE-{t.Key}";
            }

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlConnectionProvider);
                
                var results = await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
                    testData, 
                    TestHelpers.TestTableName, 
                    transaction,
                    new SqlMergeMatchQualifierExpression(nameof(TestElement.Value))
                    {
                        ThrowExceptionIfNonUniqueMatchesOccur = false
                    }
                );

                transaction.Commit();

                //ASSERT Results are Valid...
                Assert.IsNotNull(results);

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  order with incrementing ID values as specified in the original Data!
                //This validates that data is inserted as expected for Identity columns so that it can 
                //  be correctly sorted by Incrementing Identity value when Queried (e.g. ORDER BY Id)
                var resultsSorted = results.OrderBy(r => r.Id).ToList();
                Assert.AreEqual(resultsSorted.Count(), testData.Count);

                var i = 0;
                foreach (var result in resultsSorted)
                {
                    Assert.IsNotNull(result);
                    Assert.IsTrue(result.Id > 0);
                    Assert.AreEqual(result.Key, testData[i].Key);
                    Assert.AreEqual(result.Value, testData[i].Value);
                    i++;
                }

            }
        }

        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithMultipleCustomMatchQualifiersAsync()
        {
            List<TestElement> testData = TestHelpers.CreateTestData(10);
            int count = 1;
            foreach (var t in testData)
            {
                t.Id = count++;
                t.Key = $"MULTIPLE_QUALIFIER_TEST-{t.Key}";
            }

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlConnectionProvider);

                var results = await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
                    testData,
                    TestHelpers.TestTableName,
                    transaction,
                    new SqlMergeMatchQualifierExpression(
                        nameof(TestElement.Id), 
                        nameof(TestElement.Value), 
                        nameof(TestElement.Key)
                    )
                );

                transaction.Commit();

                //ASSERT Results are Valid...
                Assert.IsNotNull(results);

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  order with incrementing ID values as specified in the original Data!
                //This validates that data is inserted as expected for Identity columns so that it can 
                //  be correctly sorted by Incrementing Identity value when Queried (e.g. ORDER BY Id)
                var resultsSorted = results.OrderBy(r => r.Id).ToList();
                Assert.AreEqual(resultsSorted.Count(), testData.Count);

                var i = 0;
                foreach (var result in resultsSorted)
                {
                    Assert.IsNotNull(result);
                    Assert.IsTrue(result.Id > 0);
                    Assert.AreEqual(result.Key, testData[i].Key);
                    Assert.AreEqual(result.Value, testData[i].Value);
                    i++;
                }

            }
        }
    }
}
