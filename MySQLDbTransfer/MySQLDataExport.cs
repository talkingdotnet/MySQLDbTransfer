using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;

namespace MySQLDbTransfer
{
    public class MySQLDataExport
    {
        private const int cBatchCount = 1000;
        private const string FILEEXTENSION = ".xml.gz";

        private string _connectionString = "";
        private string _directoryPath = "";

        public MySQLDataExport(IConfigurationRoot config)
        {
            _connectionString = config["AppSettings:MySQLConnectionString"];
            _directoryPath = config["AppSettings:DestinationDirectoryPath"];
        }

        public bool ExportData(string url = "")
        {
            WriteToConsole("** MySql Data Export Started**");
            bool bValue = true;
            string newFileName = "Dbdata" + FILEEXTENSION;
            string filePath = Path.Combine(_directoryPath, newFileName);
            if(url != "")
                bValue = DownloadFile(url, filePath);
            if (bValue)
            {
                string unZippedFileName = UnZipFile(filePath);
                if (unZippedFileName == "")
                {
                    WriteToConsole("Some error occured while unzipping the file. DB export is stopped.");
                    return false;
                }
                DataSet ds = DeserializeDataset(unZippedFileName);
                if (ds != null)
                {
                    TransferData(ds);
                    WriteToConsole("** MySql Data Export Finished**");
                }
                else
                    bValue = false;
            }
            return bValue;
        }

        private void TransferData(DataSet ds)
        {
            try
            {
                EnableDisableForeignKey(false);
                if (ds != null)
                {
                    foreach (DataTable dt in ds.Tables)
                    {
                        InsertData(dt, dt.TableName);
                    }
                }
                EnableDisableForeignKey(true);
            }
            catch (Exception ex)
            {
                WriteToConsole("Data Export Failed: " + ex.ToString());
            }
        }

        private object Transform(object col)
        {
            if (col == null)
                return "NULL";

            switch (col.GetType().FullName)
            {
                case "System.DBNull":
                    return "NULL";
                case "System.Guid":
                case "System.String":
                    return $"\"{col.ToString().Replace("\"", "\\\"").Replace("'", "\\'")}\"";
                case "System.DateTime":
                    return $"\"{((DateTime)col).ToString("yyyy-MM-dd HH:mm:ss")}\"";
                case "System.Byte[]":
                    return "0x" + BitConverter.ToString((byte[])col).Replace("-", "");
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Byte":
                case "System.SByte":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                case "System.Boolean":
                    return col;
            }
            return col;
        }

        private void InsertData(DataTable dt, string tableName)
        {
            if (dt.Rows.Count == 0) return;
            try
            {
                string truncateQuery = "Truncate Table `" + tableName + "`;";
                using (MySqlConnection connection = new MySqlConnection(_connectionString))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        connection.Open();
                        command.Connection = connection;
                        command.CommandText = truncateQuery.ToString();
                        command.ExecuteNonQuery();
                    }
                }

                StringBuilder sb = new StringBuilder($"INSERT INTO `" + tableName + "` (");
                StringBuilder sbRows = new StringBuilder();
                StringBuilder sbQuery = new StringBuilder();
                foreach (DataColumn column in dt.Columns)
                {
                    sb.Append("`").Append(column.ColumnName).Append("`,");
                }
                sb.Remove(sb.Length - 1, 1);
                sb.Append(") VALUES ");
                int nCount = 0;
                foreach (DataRow row in dt.Rows)
                {
                    nCount++;
                    sbRows.Append("(");
                    foreach (var col in row.ItemArray)
                    {
                        sbRows.Append(Transform(col)).Append(",");
                    }
                    sbRows.Remove(sbRows.Length - 1, 1);
                    sbRows.Append("),");
                    if (nCount == cBatchCount)
                    {
                        if (sbRows.Length > 0)
                        {
                            sbRows.Remove(sbRows.Length - 1, 1);
                            sbQuery.Append(sb);
                            sbQuery.Append(sbRows);
                            using (MySqlConnection connection = new MySqlConnection(_connectionString))
                            {
                                using (MySqlCommand command = new MySqlCommand())
                                {
                                    connection.Open();
                                    command.Connection = connection;
                                    command.CommandText = sbQuery.ToString();
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        sbRows.Clear();
                        sbQuery.Clear();
                        nCount = 0;
                    }
                }
                if (sbRows.Length > 0)
                {
                    sbRows.Remove(sbRows.Length - 1, 1);
                    sb.Append(sbRows);
                }
                if (sbRows.Length > 0)
                {
                    using (MySqlConnection connection = new MySqlConnection(_connectionString))
                    {
                        using (MySqlCommand command = new MySqlCommand())
                        {
                            connection.Open();
                            command.Connection = connection;
                            command.CommandText = sb.ToString();
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToConsole("Error: InsertData()" + "\n" + ex.ToString());
                throw ex;
            }
        }

        private void EnableDisableForeignKey(bool bEnable)
        {
            string query = "";
            if (bEnable)
                query = "SET FOREIGN_KEY_CHECKS=1;";
            else
                query = "SET FOREIGN_KEY_CHECKS=0;";
            try
            {
                using (MySqlConnection connection = new MySqlConnection(_connectionString))
                {
                    using (MySqlCommand command = new MySqlCommand())
                    {
                        connection.Open();
                        command.Connection = connection;
                        command.CommandText = query;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToConsole("Error: EnableDisableForeignKey()" + "\n" + ex.ToString());
                throw ex;
            }
        }

        private string UnZipFile(string filePath)
        {
            string unZippedFileName = "";
            FileInfo fileToDecompress = new FileInfo(filePath);
            try
            {
                using (FileStream originalFileStream = fileToDecompress.OpenRead())
                {
                    string currentFileName = fileToDecompress.FullName;
                    unZippedFileName = currentFileName.Remove(currentFileName.Length - fileToDecompress.Extension.Length);
                    using (FileStream decompressedFileStream = File.Create(unZippedFileName))
                    {
                        using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                        {
                            decompressionStream.CopyTo(decompressedFileStream);
                        }
                    }
                }
                WriteToConsole("File unzipped successfully.");
            }
            catch (Exception ex)
            {
                WriteToConsole("Error: DecompressFile()" + "\n" + ex.ToString());
                unZippedFileName = "";
            }
            return unZippedFileName;
        }

        private DataSet DeserializeDataset(string filePath)
        {
            DataSet ds = new DataSet();
            try
            {
                ds.ReadXmlSchema(filePath);
                ds.ReadXml(filePath, XmlReadMode.IgnoreSchema);
                WriteToConsole("Dataset deserialized successfully.");
            }
            catch (Exception ex)
            {
                WriteToConsole("Error: DeserializeDataset()" + "\n" + ex.ToString());
            }
            return ds;
        }

        private bool DownloadFile(string url, string filePath)
        {
            bool bResult = true;
            try
            {
                WebClient Client = new WebClient();
                Client.DownloadFile(url, filePath);
                WriteToConsole("File Downloaded successfully");
            }
            catch (Exception ex)
            {
                WriteToConsole("Error: DownloadFile()" + "\n" + ex.ToString());
                bResult = false;
            }
            return bResult;
        }

        private void WriteToConsole(string msg)
        {
            Console.WriteLine(msg);
        }
    }
}
