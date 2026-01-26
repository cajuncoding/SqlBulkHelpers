using System;
using RepoDb;

namespace SqlBulkHelpers.Tests
{
    [TestClass]
    public class GlobalTestInitialization
    {
        [AssemblyInitialize]
        public static void Initialize(TestContext testContext)
        {
            RepoDb.GlobalConfiguration.Setup().UseSqlServer();
        }

        [AssemblyCleanup]
        public static void TearDown()
        {
        }
    }
}
