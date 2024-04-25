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
        private object _locker;
        private string _connectionString;

        private readonly string _noServerFoundMessage = "Não há servidores selecionados.";
        private readonly string _userCancelledMessage = "Cancelada pelo usuário.";
        private readonly string _queryErrorMessage = "Falha ao executar a consulta.";

        private readonly string _messageSeparator = " ";
        
        public string Credentials { private get; set; }

        public int TotalLinesAffected { get; private set; }
        public string ErrorMessage { get; set; }
        public static int Timeout { get; set; }

        public int MaxDegreeOfParallelism { get; private set; }

        public DistributedQueryExecutor(int timeout, int maxDegreeOfParallelism)
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism;
            Credentials = "Integrated Security = true";
            Timeout = timeout;
            _locker = new object();
        }

        public DataTable DistributeQuery(BackgroundWorker queryWorker, string query, List<string> servers, bool addServerName = false)
        {
            ErrorMessage = String.Empty;
            var tableResults = new DataTable();

            int step = 1;
            TotalLinesAffected = 0;
            int totalServers = servers.Count();

            if (servers.Count == 0)
                ErrorMessage = _noServerFoundMessage;

            Parallel.ForEach(servers, new ParallelOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism }, server =>
            {
                DataTable table = null;
                string errorMessage = default;

                try
                {
                    if (queryWorker.CancellationPending)
                        throw new OperationCanceledException();

                    table = ExecuteQuery(query, server);
 
                    if (addServerName)
                    {
                        DataColumn serverColumn = new DataColumn
                        {
                            ColumnName = "SERVIDOR",
                            DataType = server.GetType(),
                            DefaultValue = server
                        };
                        table?.Columns.Add(serverColumn);
                    }
                    
                }
                catch (Exception exception)
                {
                    errorMessage = String.Concat(_queryErrorMessage, _messageSeparator,
                                                  exception.Message, _messageSeparator,
                                                  server, Environment.NewLine);
                }
                finally
                {
                    lock (_locker) 
                    { 
                        step++;
                        tableResults.Merge(table ?? new DataTable());
                        ErrorMessage += errorMessage;
                    }
                    queryWorker.ReportProgress(step * 100 / totalServers);
                }
            });

            return tableResults;
        }

        public DataTable ExecuteQuery(string query, string server)
        {
            DataTable table = new DataTable { Locale = System.Globalization.CultureInfo.InvariantCulture };
            SqlDataReader dataReader;
            int linesCount;

            _connectionString = $"{Credentials};Data Source={server};Connect Timeout={Timeout}";

            SqlConnection connection = new SqlConnection(_connectionString);
            SqlCommand command = new SqlCommand(query, connection) { CommandTimeout = Timeout };

            connection.Open();
            dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            table.Load(dataReader);
            dataReader.Close();

            int linesRetrieved = table.Rows.Count;
            int linesModified = dataReader.RecordsAffected;

            linesCount = Math.Max(0, linesModified) + Math.Max(0, linesRetrieved);

            TotalLinesAffected += linesCount;

            if (table.Rows.Count < 0)
                table.Rows.Add();
            
            return table;
        }
    }
}
