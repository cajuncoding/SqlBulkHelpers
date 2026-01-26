using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using RepoDb.Attributes;
using SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers.Tests
{
    public static class TestHelpers
    {
        public const string TestTableName = "SqlBulkHelpersTestElements";
        public const string TestTableNameFullyQualified = "[dbo].[SqlBulkHelpersTestElements]";
        public const string TestChildTableNameFullyQualified = "[dbo].[SqlBulkHelpersTestElements_Child_NoIdentity]";
        public const string TestTableWithFullTextIndexFullyQualified = "[dbo].[SqlBulkHelpersTestElements_WithFullTextIndex]";

        public const int SqlTimeoutSeconds = 150;

        public static SqlBulkHelpersConfig BulkHelpersConfig { get; } = SqlBulkHelpersConfig.Create(config =>
        {
            config.SqlBulkPerBatchTimeoutSeconds = SqlTimeoutSeconds;
        });

        public static List<TestElement> CreateTestData(int dataSize, string prefix = "TEST_CSHARP_DotNet8")
        {
            var list = new List<TestElement>();
            var childList = new List<ChildTestElement>();
            for (var x = 1; x <= dataSize; x++)
            {
                var testElement = new TestElement()
                {
                    Id = default,
                    Key = $"{prefix}[{x:0000}]_GUID[{Guid.NewGuid().ToString().ToUpper()}]",
                    Value = $"VALUE_{x:0000}",
                    ComputedValue = $"SHOULD_NOT_BE_SAVED::{TokenIdGenerator.NewTokenId(50)}", //NOTE: SHOULD NEVER BE SAVED!
                    IgnoredTransientValue = $"SHOULD_BE_IGNORED::{TokenIdGenerator.NewTokenId(10)}"
                };

                list.Add(testElement);

                for (var c = 1; c <= 3; c++)
                {
                    childList.Add(new ChildTestElement()
                    {
                        ParentId = testElement.Id,
                        ChildKey = $"CHILD #{c} Of: {testElement.Key}",
                        ChildValue = testElement.Value
                    });
                }
            }

            return list;
        }

        public static List<ChildTestElement> CreateChildTestData(List<TestElement> testData)
        {
            var childList = new List<ChildTestElement>();
            foreach (var testElement in testData)
            {
                for (var c = 1; c <= 3; c++)
                {
                    childList.Add(new ChildTestElement()
                    {
                        ParentId = testElement.Id,
                        ChildKey = $"CHILD #{c:0000} Of: {testElement.Key}",
                        ChildValue = testElement.Value
                    });
                }
            }

            return childList;
        }

        public static List<TestElementWithIdentitySetter> CreateTestDataWithIdentitySetter(int dataSize)
        {
            var testData = CreateTestData(dataSize);
            var list = testData.Select(t => new TestElementWithIdentitySetter()
            {
                Id = t.Id,
                Key = t.Key,
                Value = t.Value,
                ComputedValue = t.ComputedValue
            }).ToList();

            return list;
        }
    }

    public class TestElement
    {
        public int Id { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
        public string ComputedValue { get; set; }
        [SqlBulkIgnore]
        public string IgnoredTransientValue { get; set; }
        public override string ToString() => $"Id=[{Id}], Key=[{Key}]";
    }

    //Class for testing the PropertyHandlers implementaitons...
    [SqlBulkTable("SqlBulkHelpersPropertyHandlerTest")]
    public class TestElementConvertedToJson
    {
        public TestElementConvertedToJson(TestElement testElement)
        {
            this.TestElementSqlBulkConvertedJson = testElement;
            this.TestElementRepoDbConvertedJson = testElement;
        }

        public int Id { get; set; }

        [SqlBulkConvertToJson]
        public TestElement TestElementSqlBulkConvertedJson { get; set; }

        [PropertyHandler(typeof(RepoDbJsonPropertyHandler))]
        public TestElement TestElementRepoDbConvertedJson { get; set; }
    }

    [SqlBulkTable(TestHelpers.TestChildTableNameFullyQualified)]
    public class ChildTestElement
    {
        public string ChildKey { get; set; }
        public int ParentId { get; set; }
        public string ChildValue { get; set; }
        public override string ToString() => $"ParentId=[{ParentId}], ChildKey=[{ChildKey}]";
    }

    [SqlBulkTable(TestHelpers.TestTableName, uniqueMatchMergeValidationEnabled: false)]
    public class TestElementWithMappedNames
    {
        public TestElementWithMappedNames()
        {
        }

        public TestElementWithMappedNames(TestElement testElement)
        {
            MyId = testElement.Id;
            MyKey = testElement.Key;
            MyValue = testElement.Value;
            UnMappedProperty = -1;
        }

        [SqlBulkMatchQualifier]
        [Map("Id")]
        public int MyId { get; set; }
        
        [Column("Key")]
        [SqlBulkMatchQualifier]
        public string MyKey { get; set; }

        //TEST case where Name is not Specified in the Linq2Db Column Attribute
        //  since it is actually Optional: https://github.com/cajuncoding/SqlBulkHelpers/issues/20
        [Column()]
        public string MyColWithNullName { get; set; }

        //Regardless of attribute order the SqlBulkColumn should take precedent!
        [Column("INCORRECT_NAME_SHOULD_NOT_RESOLVE")]
        [SqlBulkColumn("Value")]
        [Map("INCORRECT_NAME_SHOULD_NOT_RESOLVE")]
        public string MyValue { get; set; }

        public int UnMappedProperty { get; set; }

        public override string ToString()
        {
            return $"Id=[{MyId}], Key=[{MyKey}]";
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
