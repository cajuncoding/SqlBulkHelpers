using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers
{
    public class SqlMatchQualifierField
    {
        public SqlMatchQualifierField(string fieldName)
        {
            this.Name = fieldName;
            this.SanitizedName = fieldName.Trim('[', ']');
        }

        public string Name { get; }
        public string SanitizedName { get; }
    }

    public class SqlMergeMatchQualifierExpression
    {
        public SqlMergeMatchQualifierExpression(params string[] fieldNames)
        {
            if(fieldNames == null || !fieldNames.Any())
                throw new ArgumentException(nameof(fieldNames));

            MatchQualifierFields = fieldNames.Select(n => new SqlMatchQualifierField(n)).ToList();
        }

        public SqlMergeMatchQualifierExpression(params SqlMatchQualifierField[] matchQualifierFields)
        {
            if (matchQualifierFields == null || !matchQualifierFields.Any())
                throw new ArgumentException(nameof(matchQualifierFields));

            MatchQualifierFields = matchQualifierFields.ToList();
        }

        public List<SqlMatchQualifierField> MatchQualifierFields { get; }

        //public QualifierLogicalOperator LogicalOperator { get; } = QualifierLogicalOperator.And;
    }
}
