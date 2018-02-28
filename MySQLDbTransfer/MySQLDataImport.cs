using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;

namespace MySQLDbTransfer
{
    public class MySQLDataImport
    {
        #region Fields
        private const int cBatchCount = 2000;

        private string _connectionString = "";
        private string _directoryPath = "";

        #endregion

        public MySQLDataImport(IConfigurationRoot config)
        {
            _connectionString = config["AppSettings:MySQLConnectionString"];
            _directoryPath = config["AppSettings:DestinationDirectoryPath"];
        }

        #region Methods

        public bool Import(string url = "")
        {
            WriteToConsole("** MySql Data Export Started**");
            bool bValue = true;
            string newFileName = "Dbdata.xml.gz";
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
                DisableForeignKey(true);
                if (ds != null)
                {
                    foreach (DataTable dt in ds.Tables)
                    {
                        Populate(dt, dt.TableName);
                    }
                }
                DisableForeignKey(false);
            }
            catch (Exception ex)
            {
                WriteToConsole("Data Export Failed: " + ex.ToString());
            }
        }

        private object ConvertDataTypes(object col)
        {
            if (col == null)
                return "NULL";

            switch (col.GetType().FullName)
            {
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
                case "System.DBNull":
                    return "NULL";
                case "System.Guid":
                case "System.String":
                    return $"\"{col.ToString().Replace("\"", "\\\"").Replace("'", "\\'")}\"";
                case "System.DateTime":
                    return $"\"{((DateTime)col).ToString("yyyy-MM-dd HH:mm:ss")}\"";
                case "System.Byte[]":
                    return "0x" + BitConverter.ToString((byte[])col).Replace("-", "");
            }
            return col;
        }

        private void Populate(DataTable dt, string tableName)
        {
            if (dt.Rows.Count == 0) return;
            try
            {
                string truncateQuery = "Truncate Table `" + tableName + "`;"; //First truncate the table
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
                StringBuilder sbValues = new StringBuilder();
                StringBuilder sbQuery = new StringBuilder();
                foreach (DataColumn column in dt.Columns)
                {
                    sb.Append("`").Append(column.ColumnName).Append("`,");
                }
                sb.Remove(sb.Length - 1, 1);
                sb.Append(") VALUES ");
                int cnt = 0;
                foreach (DataRow row in dt.Rows)
                {
                    cnt++;
                    sbValues.Append("(");
                    foreach (var col in row.ItemArray)
                    {
                        sbValues.Append(ConvertDataTypes(col)).Append(",");
                    }
                    sbValues.Remove(sbValues.Length - 1, 1);
                    sbValues.Append("),");
                    if (cnt == cBatchCount)
                    {
                        if (sbValues.Length > 0)
                        {
                            sbValues.Remove(sbValues.Length - 1, 1);
                            sbQuery.Append(sb);
                            sbQuery.Append(sbValues);
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
                        sbQuery.Clear();
                        sbValues.Clear();
                        cnt = 0;
                    }
                }
                if (sbValues.Length > 0)
                {
                    sbValues.Remove(sbValues.Length - 1, 1);
                    sb.Append(sbValues);
                }
                if (sbValues.Length > 0)
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

        private void DisableForeignKey(bool flag)
        {
            string query = "";
            if (flag)
                query = "SET FOREIGN_KEY_CHECKS=0;";
            else
                query = "SET FOREIGN_KEY_CHECKS=1;";
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
                WriteToConsole("Error: DisableForeignKey()" + "\n" + ex.ToString());
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

        #endregion
    }
}
