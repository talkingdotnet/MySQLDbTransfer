using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Xml.Serialization;

namespace MySQLDbTransfer
{
    public class DataExtractor
    {
        #region Fields
        private string _connectionString = "";
        private string _directoryPath = "";
        #endregion

        public DataExtractor(IConfigurationRoot config)
        {
            _connectionString = config["AppSettings:SQLConnectionString"];
            _directoryPath = config["AppSettings:SourceDirectoryPath"];
        }

        #region Methods
        
        public void ExtractData()
        {
            try
            {
                DataTable dtList = GetTableList();
                DataSet ds = new DataSet();
                foreach (DataRow row in dtList.Rows)
                {
                    string tableName = row[0].ToString();
                    DataTable dtData = FetchData(tableName);
                    ds.Tables.Add(dtData);
                }
                if (ds.Tables.Count > 0)
                {
                    XmlSerializer ser = new XmlSerializer(typeof(DataSet));
                    string xmlFileName = "Dbdata.xml";
                    string fullPath = Path.Combine(_directoryPath, xmlFileName);
                    using (TextWriter writer = new StreamWriter(fullPath))
                    {
                        ser.Serialize(writer, ds);
                        writer.Close();
                    }
                    PrepareZip(fullPath);
                    File.Delete(fullPath);
                }
            }
            catch (Exception ex)  { throw ex; }
        }

        private DataTable FetchData(string tableName)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                string sQuery = "SELECT * FROM [" + tableName + "]";
                using (SqlCommand command = new SqlCommand(sQuery, conn))
                {
                    conn.Open();
                    command.CommandTimeout = 180; 
                    DataAdapter da = new SqlDataAdapter(command);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    dt.TableName = tableName;
                    ds.Tables.Remove(dt);
                    return dt;
                }
            }
        }

        private DataTable GetTableList()
        {
            DataTable dt;
            string sQuery = @"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                using (SqlCommand command = new SqlCommand(sQuery, conn))
                {
                    conn.Open();
                    DataAdapter da = new SqlDataAdapter(command);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    dt = ds.Tables[0];
                }
            }
            return dt;
        }

        private void PrepareZip(string filePath)
        {
            FileInfo gZipfile = new FileInfo(filePath);
            FileInfo gzipFileName = new FileInfo(string.Concat(gZipfile.FullName, ".gz"));
            using (FileStream fileToBeZippedAsStream = gZipfile.OpenRead())
            {
                using (FileStream gzipTargetAsStream = gzipFileName.Create())
                {
                    using (GZipStream gzipStream = new GZipStream(gzipTargetAsStream, CompressionMode.Compress))
                    {
                        try
                        {
                            fileToBeZippedAsStream.CopyTo(gzipStream);
                        }
                        catch (Exception ex) { throw ex; }
                    }
                }
            }
        }

        #endregion
    }
}
