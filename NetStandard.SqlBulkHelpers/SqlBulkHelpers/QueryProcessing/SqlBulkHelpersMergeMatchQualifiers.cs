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

        public override string ToString()
        {
            return $"[{this.Name}]";
        }
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
            return MatchQualifierFields.Select(f => f.ToString()).ToCSV();
        }
    }
}
