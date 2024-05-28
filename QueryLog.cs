using System;

namespace DistributedQueryLibrary
{
    public class QueryLog
    {
        public string Message { get; private set; }
        public string Server {  get; private set; }
        public DateTime Time { get; private set; }
        public QueryLog(string message, string server, DateTime time)
        {
            Message = message;
            Server = server;
            Time = time;
        }
    }
}
