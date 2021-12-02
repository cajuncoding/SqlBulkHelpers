using System;
using System.Collections.Generic;
using System.Linq;
using SqlBulkHelpers.SqlBulkHelpers.Interfaces;

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

        public static List<TestElementWithIdentitySetter> CreateTestDataWithIdentitySetter(int dataSize)
        {
            var testData = CreateTestData(dataSize);
            var list = testData.Select(t => new TestElementWithIdentitySetter()
            {
                Id = t.Id,
                Key = t.Key,
                Value = t.Value
            }).ToList();

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

    public class TestElementWithIdentitySetter : TestElement, ISqlBulkHelperIdentitySetter
    {
        public void SetIdentityId(int id)
        {
            this.Id = id;
        }
    }
}
