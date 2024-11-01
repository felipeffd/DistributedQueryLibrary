using System;
using System.Data;
using System.IO;
using System.Text;

namespace DistributedQueryLibrary
{
    public static class ResultExporter
    {
        public static void ExportToCSV(DataTable table, string filename) 
        {
            try
            {
                using (StreamWriter writer = new StreamWriter(filename, false, Encoding.UTF8))
                {
                    WriteHeader(writer, table);
                    WriteDataRows(writer, table);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao exportar arquivo CSV: " + ex.Message);
            }
        }

        private static void WriteHeader(StreamWriter writer, DataTable table)
        {
            for (int i = 0; i < table.Columns.Count; i++)
            {
                writer.Write(table.Columns[i].ColumnName);
                if (i < table.Columns.Count - 1)
                {
                    writer.Write(";");
                }
            }
            writer.WriteLine(); 
        }

        private static void WriteDataRows(StreamWriter writer, DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    // Handle any potential commas in the data by enclosing each field in quotes
                    writer.Write($"\"{row[i].ToString().Replace("\"", "\"\"")}\"");

                    if (i < table.Columns.Count - 1)
                    {
                        writer.Write(";");
                    }
                }
                writer.WriteLine();
            }
        }
    }
}
