using System;
using System.Collections.Generic;
using System.Linq;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers
{
    public class SqlMatchQualifierField
    {
        public SqlMatchQualifierField(string fieldName)
        {
            this.Name = fieldName;
            this.SanitizedName = fieldName.TrimTableNameTerm();
        }

        public string Name { get; }
        public string SanitizedName { get; }

        public override string ToString()
        {
            return this.Name.QualifySqlTerm();
        }
    }

    public class SqlMergeMatchQualifierExpression
    {
        public SqlMergeMatchQualifierExpression()
        {
        }

        public SqlMergeMatchQualifierExpression(params string[] fieldNames)
            : this(fieldNames.ToList())
        {
        }

        public SqlMergeMatchQualifierExpression(params SqlMatchQualifierField[] matchQualifierFields)
            : this(matchQualifierFields.ToList())
        {
        }

        public SqlMergeMatchQualifierExpression(IEnumerable<string> fieldNames)
            : this(fieldNames?.Select(n => new SqlMatchQualifierField(n)).ToList())
        {
        }

        public SqlMergeMatchQualifierExpression(IEnumerable<SqlMatchQualifierField> fields)
        {
            var fieldsList = fields?.ToList();
            if (fieldsList.IsNullOrEmpty())
                throw new ArgumentException(nameof(fieldsList));

            MatchQualifierFields = fieldsList;
        }

        public List<SqlMatchQualifierField> MatchQualifierFields { get; protected set; }

        /// <summary>
        /// BBernard - 12/08/2020
        /// When non-identity field match qualifiers are specified it's possible that multiple
        /// records may match if the fields are non-unique. This will result in potentially erroneous
        /// postprocessing for results, therefore we will throw an Exception when this is detected by Default!
        /// </summary>
        public bool ThrowExceptionIfNonUniqueMatchesOccur { get; set; } = true;

        //public QualifierLogicalOperator LogicalOperator { get; } = QualifierLogicalOperator.And;

        public override string ToString()
        {
            return MatchQualifierFields.Select(f => f.ToString()).ToCsv();
        }
    }
}
