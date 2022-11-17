﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class ProcessingDefinitionTests
    {

        [TestMethod]
        public void TestProcessDefinitionLoading()
        {
            var processingDef = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<TestElementWithMappedNames>();
            
            Assert.IsNotNull(processingDef);
            Assert.AreEqual(TableNameTerm.From("TestElement").FullyQualifiedTableName, processingDef.MappedDbTableName);
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
            Assert.AreEqual(2, matchQualifierExpression.MatchQualifierFields.Count);
            //Match Qualifiers should always use their Mapped DB Name!
            Assert.AreEqual("Id", matchQualifierExpression.MatchQualifierFields[0].SanitizedName);
            Assert.AreEqual("Key", matchQualifierExpression.MatchQualifierFields[1].SanitizedName);
        }


    }
}
