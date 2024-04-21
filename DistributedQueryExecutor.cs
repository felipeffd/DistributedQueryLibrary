﻿using System;
using System.Linq;
using System.Data;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Reflection.Emit;

namespace DistributedQueryLibrary
{
    public class DistributedQueryExecutor
    {     
        private string _connectionString;
        private readonly string _credentials;
        public int TotalLinesAffected { get; private set; }
        public static int Timeout { get; set; }

        public string ErrorMessage { get; set; }

        private string _noServerFoundMessage = "Não há servidores selecionados.";
        private string _userCancelledMessage = "Cancelada pelo usuário.";
        private string _queryErrorMessage = "Falha ao executar a consulta.";

        private string _messageSeparator = "-";

        public DistributedQueryExecutor(int timeout)
        {
            _credentials = "Integrated Security = true";
            Timeout = timeout;
        }

        public DataTable DistributeQuery(BackgroundWorker queryWorker, string query, List<string> servers, bool addServerName = false)
        {
            var tableResults = new DataTable();
            List<DataTable> resultsTableList = new List<DataTable>();

            int step = 0;
            TotalLinesAffected = 0;
            int totalServers = servers.Count();

            if (servers.Count == 0)
                ErrorMessage = _noServerFoundMessage;

            Parallel.ForEach(servers, new ParallelOptions { MaxDegreeOfParallelism = 10 }, server =>
            {
                DataTable table = null;

                step++;
                try
                {
                    if (queryWorker.CancellationPending)
                        throw new Exception(_userCancelledMessage);

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
                    resultsTableList.Add(table);
                    
                }
                catch (Exception exception)
                {
                    table = null;
                    ErrorMessage += String.Concat(_queryErrorMessage, _messageSeparator,
                                                  exception.Message, _messageSeparator,
                                                  server, Environment.NewLine);
                }
                finally
                {
                    queryWorker.ReportProgress(step * 100 / totalServers);
                }
            });

            resultsTableList.RemoveAll(table => table == null);

            if (resultsTableList.Count < servers.Count)
                return DistributeQuery(queryWorker, query, servers);

            resultsTableList.ForEach(table => tableResults.Merge(table));

            return tableResults;
        }

        private DataTable ExecuteQuery(string query, string server)
        {
            DataTable table = new DataTable { Locale = System.Globalization.CultureInfo.InvariantCulture };
            SqlDataReader dataReader;
            int linesCount = -1;

            _connectionString = $"{_credentials};Data Source={server};Connect Timeout={Timeout}";

            SqlConnection connection = new SqlConnection(_connectionString);
            SqlCommand command = new SqlCommand(query, connection)
            {
                CommandTimeout = Timeout
            };

            connection.Open();
            dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            table.Load(dataReader);
            dataReader.Close();

            int linesRetrieved = table.Rows.Count;
            int linesModified = dataReader.RecordsAffected;

            linesCount = Math.Max(0, linesModified) + Math.Max(0, linesRetrieved);

            TotalLinesAffected += linesCount;

            if (table?.Rows?.Count < 0)
                table?.Rows?.Add();
            
            if (linesCount <= 0)
                table = null;
            
            return table;
        }
    }
}
