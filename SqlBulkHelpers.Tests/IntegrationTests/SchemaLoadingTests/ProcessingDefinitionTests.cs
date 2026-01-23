using System;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class ProcessingDefinitionTests : BaseTest
    {
        [TestMethod]
        public void TestProcessDefinitionLoading()
        {
            //Test retrieving for a specific class type...
            var processingDef = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<TestElementWithMappedNames>();
            
            Assert.IsNotNull(processingDef);
            Assert.AreEqual(TableNameTerm.From(TestHelpers.TestTableName).FullyQualifiedTableName, processingDef.MappedDbTableName);
            Assert.IsFalse(processingDef.UniqueMatchMergeValidationEnabled);
            Assert.IsTrue(processingDef.IsRowNumberColumnNameEnabled);
            Assert.IsTrue(processingDef.IsMappingLookupEnabled);

            const string unmappedPropertyName = nameof(TestElementWithMappedNames.UnMappedProperty);

            foreach (var propDef in processingDef.PropertyDefinitions)
            {
                var expectedMappedName = propDef.PropertyName switch
                {
                    nameof(TestElementWithMappedNames.MyId) => "Id",
                    nameof(TestElementWithMappedNames.MyKey) => "Key",
                    nameof(TestElementWithMappedNames.MyValue) => "Value",
                    //For Test case clarity we explicitly Test Linq2Db Column Attr. with no Name specified: https://github.com/cajuncoding/SqlBulkHelpers/issues/20
                    nameof(TestElementWithMappedNames.MyColWithNullName) => nameof(TestElementWithMappedNames.MyColWithNullName),
                    unmappedPropertyName => unmappedPropertyName,
                    _ => null
                };

                Assert.AreEqual(expectedMappedName, propDef.MappedDbColumnName);
                //NONE of these should be an Identity Property since no Identity Column Table Definition was provided when Initializing!
                Assert.IsFalse(propDef.IsIdentityProperty);
            }

            var matchQualifierExpression = processingDef.MergeMatchQualifierExpressionFromEntityModel;
            Assert.IsNotNull(matchQualifierExpression);
            //We intentionally set this to False to validate/test the setting (though Default is True)
            Assert.IsFalse(matchQualifierExpression.ThrowExceptionIfNonUniqueMatchesOccur);

            Assert.IsNotNull(matchQualifierExpression.MatchQualifierFields);
            Assert.HasCount(2, matchQualifierExpression.MatchQualifierFields);
            //Match Qualifiers should always use their Mapped DB Name!
            Assert.AreEqual("Id", matchQualifierExpression.MatchQualifierFields[0].SanitizedName);
            Assert.AreEqual("Key", matchQualifierExpression.MatchQualifierFields[1].SanitizedName);

            
            //Test new PropertyGetter Delegate implementation (using Fasterflect)...
            var testElementWithMappedNames = new TestElementWithMappedNames()
            {
                MyId = Random.Shared.Next(),
                MyColWithNullName = TokenIdGenerator.NewTokenId(50),
                MyKey = TokenIdGenerator.NewTokenId(25),
                MyValue = Guid.NewGuid().ToString(),
                UnMappedProperty = Random.Shared.Next(),
            };

            foreach (var propDef in processingDef.PropertyDefinitions)
            {
                var propValue = propDef.InvokePropertyValueGetter(testElementWithMappedNames);
                switch (propDef.PropertyName)
                {
                    case nameof(TestElementWithMappedNames.MyId): Assert.AreEqual(testElementWithMappedNames.MyId, propValue); break;
                    case nameof(TestElementWithMappedNames.MyKey): Assert.AreEqual(testElementWithMappedNames.MyKey, propValue); break;
                    case nameof(TestElementWithMappedNames.MyValue): Assert.AreEqual(testElementWithMappedNames.MyValue, propValue); break;
                    case nameof(TestElementWithMappedNames.MyColWithNullName): Assert.AreEqual(testElementWithMappedNames.MyColWithNullName, propValue); break;
                    case nameof(TestElementWithMappedNames.UnMappedProperty): Assert.AreEqual(testElementWithMappedNames.UnMappedProperty, propValue); break;
                    default: throw new InvalidOperationException("Unexpected test elemetn property encountered but not accounted or in the switch case tests!");
                };
            }
        }
    }
}
