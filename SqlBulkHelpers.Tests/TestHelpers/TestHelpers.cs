using System;
using System.Collections.Generic;

namespace SqlBulkHelpers.Tests
{
    public static class TestHelpers
    {
        public const string TestTableName = "SqlBulkHelpersTestElements";
        public const string TestTableNameFullyQualified = "[dbo].[SqlBulkHelpersTestElements]";

        public const int SqlTimeoutSeconds = 150;

        public static List<TestElement> CreateTestData(int dataSize)
        {

            var list = new List<TestElement>();
            for (var x = 1; x <= dataSize; x++)
            {
                list.Add(new TestElement()
                {
                    Id = default,
                    Key = $"TEST_CSHARP_ORDINAL[{x}]_GUID[{Guid.NewGuid().ToString().ToUpper()}]",
                    Value = $"VALUE_{x}"
                });
            }

            return list;
        }
    }

    public class TestElement
    {
        public int Id { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }

        public override string ToString()
        {
            return $"Id=[{Id}], Key=[{Key}]";
        }
    }
}
