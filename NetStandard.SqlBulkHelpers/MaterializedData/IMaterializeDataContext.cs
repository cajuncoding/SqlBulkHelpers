namespace SqlBulkHelpers.MaterializedData
{
    public interface IMaterializeDataContext
    {
        MaterializationTableInfo[] Tables { get; }

        /// <summary>
        /// Allows disabling of data validation during materialization, but may put data integrity at risk.
        /// This will improve performance for large data loads, but if disabled then the implementor is responsible
        /// for ensuring all data integrity of the data populated into the tables!
        /// NOTE: The downside of this is that larger tables will take longer to Switch over but Data Integrity is maintained therefore this
        ///         is the default and normal behavior that should be used.
        /// NOTE: In addition, Disabling this poses other implications in SQL Server as the Constraints then become Untrusted which affects
        ///         the Query Optimizer and may may adversely impact Query performance.
        /// </summary>
        bool EnableDataConstraintChecksOnCompletion { get; set; }

        MaterializationTableInfo this[int index] { get; }
        MaterializationTableInfo this[string fullyQualifiedTableName] { get; }
        MaterializationTableInfo FindMaterializationTableInfoCaseInsensitive(string fullyQualifiedTableName);
    }
}