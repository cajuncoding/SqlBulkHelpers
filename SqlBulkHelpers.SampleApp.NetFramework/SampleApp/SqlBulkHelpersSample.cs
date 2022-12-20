using System;
using System.Collections.Generic;
using SqlBulkHelpers.Tests;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public static class SqlBulkHelpersSample
    {
        public static List<TestElement> CreateTestData(int dataSize)
            => TestHelpers.CreateTestData(dataSize, "TEST_CSHARP_NetFramework");

        public static List<ChildTestElement> CreateChildTestData(List<TestElement> testData)
            => TestHelpers.CreateChildTestData(testData);
    }
}
