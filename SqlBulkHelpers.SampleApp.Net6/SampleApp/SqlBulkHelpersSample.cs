using System;
using System.Collections.Generic;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public static class SqlBulkHelpersSample
    {
        public static List<TestElement> CreateTestData(int dataSize)
        {
            var list = new List<TestElement>();
            for (var x = 1; x <= dataSize; x++)
            {
                list.Add(new TestElement()
                {
                    Id = default,
                    Key = $"TEST_CSHARP_.NetCore3.1_{Guid.NewGuid()}_{x}",
                    Value = $"VALUE_{x}"
                });
            }

            return list;
        }
    }

    public class TestElement
    {
        public int Id { get; set; }
        public String Key { get; set; }
        public String Value { get; set; }
    }
}
