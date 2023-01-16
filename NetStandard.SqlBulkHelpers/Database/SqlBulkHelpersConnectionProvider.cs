using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard
    /// Connection string provider class to keep the responsibility for Loading the connection string in on only one class;
    /// but also supports custom implementations that would initialize the Connection string in any other custom way.
    ///
    /// Supports initializing SqlConnection directly from the Connection string, or from an SqlConnection factory Func provided.
    /// </summary>
    public class SqlBulkHelpersConnectionProvider : ISqlBulkHelpersConnectionProvider
    {
        public const string SqlConnectionStringConfigKey = "SqlConnectionString";

        protected string SqlDbConnectionUniqueIdentifier { get; set; }

        protected Func<SqlConnection> NewSqlConnectionFactory { get; set; }

        public SqlBulkHelpersConnectionProvider(string sqlConnectionString)
            : this(sqlConnectionString, () => new SqlConnection(sqlConnectionString))
        {
        }

        /// <summary>
        /// Uses the specified unique identifier parameter for caching (e.g. DB Schema caching) elements unique to this DB Connection.
        /// Initializes connections using hte provided SqlConnection Factory specified.
        /// </summary>
        /// <param name="sqlDbConnectionUniqueIdentifier">Most likely the Connection String!</param>
        /// <param name="sqlConnectionFactory"></param>
        /// <exception cref="ArgumentException"></exception>
        public SqlBulkHelpersConnectionProvider(string sqlDbConnectionUniqueIdentifier, Func<SqlConnection> sqlConnectionFactory)
        {
            if (string.IsNullOrWhiteSpace(sqlDbConnectionUniqueIdentifier))
                throw new ArgumentException($"The Unique DB Connection Identifier specified is null/empty; a valid identifier must be specified.");

            SqlDbConnectionUniqueIdentifier = sqlDbConnectionUniqueIdentifier;
            NewSqlConnectionFactory = sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));
        }

        /// <summary>
        /// Uses the specified unique identifier parameter for caching (e.g. DB Schema caching) elements unique to this DB Connection.
        /// Initializes connections using hte provided SqlConnection Factory specified.
        /// </summary>
        /// <param name="sqlConnectionFactory"></param>
        /// <exception cref="ArgumentException"></exception>
        public SqlBulkHelpersConnectionProvider(Func<SqlConnection> sqlConnectionFactory)
        {
            NewSqlConnectionFactory = sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));
            using (var sqlTempConnection = sqlConnectionFactory.Invoke())
            {
                SqlDbConnectionUniqueIdentifier = sqlTempConnection.ConnectionString;
            }
        }

        /// <summary>
        /// Provide Internal access to the Connection String to help uniquely identify the Connections from this Provider.
        /// </summary>
        /// <returns>Unique string representing connections provided by this provider</returns>
        public virtual string GetDbConnectionUniqueIdentifier()
        {
            return SqlDbConnectionUniqueIdentifier;
        }

        public virtual SqlConnection NewConnection()
        {
            var sqlConn = NewSqlConnectionFactory.Invoke();
            if (sqlConn.State != ConnectionState.Open)
                sqlConn.Open();

            return sqlConn;
        }

        public virtual async Task<SqlConnection> NewConnectionAsync()
        {
            var sqlConn = await NewSqlConnectionFactory
                .Invoke()
                .EnsureSqlConnectionIsOpenAsync()
                .ConfigureAwait(false);

            return sqlConn;
        }
    }
}
