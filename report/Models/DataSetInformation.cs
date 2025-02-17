using System;
using System.Data;

namespace report
{
    internal class DataSetInformation
    {
        public string TableName { get; set; }
        public DataTable DataTable { get; set; }
        public string Sh { get; set; }
        public string Start { get; set; }
        public string Finish { get; set; }
        public DateTime LastUpdate { get; set; }

        public DataSetInformation(string tableName, DataTable dataTable, string sh, string start, string finish, DateTime lastUpdate)
        {
            TableName = tableName;
            DataTable = dataTable;
            Sh = sh;
            Start = start;
            Finish = finish;
            LastUpdate = lastUpdate;
        }
    }
}
