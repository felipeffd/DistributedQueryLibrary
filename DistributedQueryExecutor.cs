using System;
using System.Linq;
using System.Data;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace DistributedQueryLibrary
{
    public class DistributedQueryExecutor
    {     
        private readonly object _locker;

        private readonly string _noServerFoundMessage = "Não há servidores selecionados.";
        private readonly string _userCancelledMessage = "Cancelada pelo usuário.";
        private readonly string _queryErrorMessage = "Falha ao executar a consulta: ";
        private readonly string _linesAffectedMessage = " linhas afetada(s) em: ";
        public string Credentials { private get; set; }
        public int TotalLinesAffected { get; private set; }
        public List<QueryLog> Messages { get; private set; }
        public int MaxDegreeOfParallelism { get; private set; }
        public static int Timeout { get; private set; }

        public DistributedQueryExecutor(int timeout, int maxDegreeOfParallelism)
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism;
            Credentials = "Integrated Security = true";
            Timeout = timeout;
            _locker = new object();
        }

        /// <summary>
        /// Executes a query to a list of servers.
        /// The string connection will use the Integrated Security by default.
        /// A list of result messages will be stored in the Messages property. 
        /// </summary>
        /// <param name="query">The query to be processed.</param>
        /// <param name="servers">A list of servers to connect.</param>
        /// <param name="queryWorker">A nullable Backgroundworker for cancellation commands and reporting results.</param>
        /// <param name="addServerName">A flag to add or not a column showing the server name.</param>
        /// <returns>The results in a DataTable.</returns>
        public DataTable DistributeQuery(string query, List<string> servers, BackgroundWorker queryWorker = null, bool addServerName = false)
        {
            int step = 0;
            TotalLinesAffected = 0;

            Messages = new List<QueryLog>();
            var tableResults = new DataTable();

            if (servers.Count == 0)
                Messages.Add(new QueryLog(_noServerFoundMessage, String.Empty, DateTime.Now));

            Parallel.ForEach(servers, new ParallelOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism }, server =>
            {
                bool isSuccessful = true;
                string localMessage = default;
                (DataTable table, int linesAffected) localTable = default;

                try
                {
                    if (queryWorker != null && queryWorker.CancellationPending)
                        throw new OperationCanceledException(_userCancelledMessage);

                    localTable = ExecuteQuery(query, server);

                    if (addServerName && localTable.table.Rows.Count > 0)
                    {
                        DataColumn serverColumn = new DataColumn
                        {
                            ColumnName = "SERVIDOR",
                            DataType = server.GetType(),
                            DefaultValue = server
                        };
                        localTable.table?.Columns.Add(serverColumn);
                    }
                    localMessage = String.Concat(localTable.linesAffected, _linesAffectedMessage);
                }
                catch (Exception exception)
                {
                    localMessage = String.Concat(_queryErrorMessage, exception.Message);
                    isSuccessful = false;
                }
                finally
                {
                    lock (_locker)
                    { 
                        step++;
                        TotalLinesAffected += localTable.linesAffected;
                        Messages.Add(new QueryLog(localMessage, server, DateTime.Now, isSuccessful));
                        tableResults.Merge(localTable.table ?? new DataTable());
                        queryWorker?.ReportProgress(step * 100 / servers.Count());
                    }
                }
            });

            return tableResults;
        }
        
        /// <summary>
        /// Executes a query to a single server.
        /// </summary>
        /// <param name="query">The SQL command to be processed.</param>
        /// <param name="server">The target server to receive the query.</param>
        /// <returns>A tuple with the results in a DataTable, and the number of lines affected.</returns>
        public (DataTable table, int linesAffected) ExecuteQuery(string query, string server)
        {
            DataTable table = new DataTable { Locale = System.Globalization.CultureInfo.InvariantCulture };
            string _connectionString = $"{Credentials};Data Source={server};Connect Timeout={Timeout}";

            SqlConnection connection = new SqlConnection(_connectionString);
            SqlCommand command = new SqlCommand(query, connection) { CommandTimeout = Timeout };

            connection.Open();
            SqlDataReader dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            table.Load(dataReader);
            dataReader.Close();

            int linesRetrieved = table.Rows.Count;
            int linesModified = dataReader.RecordsAffected;

            int linesAffected = Math.Max(0, linesModified) + Math.Max(0, linesRetrieved);

            return (table, linesAffected);
        }
    }
}
