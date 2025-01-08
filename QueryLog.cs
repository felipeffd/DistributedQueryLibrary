using System;

namespace DistributedQueryLibrary
{
    public class QueryLog
    {
        public string Message { get; private set; }
        public string Server {  get; private set; }
        public DateTime Time { get; private set; }
        public bool IsSuccessfull { get; private set; }
        public QueryLog(string message, string server, DateTime time)
        {
            Message = message;
            Server = server;
            Time = time;
        }

        public QueryLog(string message, string server, DateTime time, bool isSuccessfull) : this(message, server, time)
        {
            IsSuccessfull = isSuccessfull;
        }        
    }
}
