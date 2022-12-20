using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RepoDb;
using RepoDb.Options;

namespace SqlBulkHelpers.Tests
{
    [TestClass]
    internal class GlobalTestInitialization
    {
        [AssemblyInitialize]
        public static void Initialize(TestContext testContext)
        {
            GlobalConfiguration.Setup().UseSqlServer();
        }

        [AssemblyCleanup]
        public static void TearDown()
        {
        }
    }
}
