using System;
using System.Collections.Generic;
using SqlBulkHelpers.Tests;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public static class SqlBulkHelpersSample
    {
        public static List<TestElement> CreateTestData(int dataSize)
            => TestHelpers.CreateTestData(dataSize, prefix: "TEST_CSHARP_DotNet6");
        public static List<ChildTestElement> CreateChildTestData(List<TestElement> testData)
            => TestHelpers.CreateChildTestData(testData);

    }
}
