using System;
using CryptographyGCM;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Net.Http;
using Newtonsoft.Json;

namespace DeleteNodeInWorskspace
{
    internal class Program
    {
        private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            NLog.LogManager.Configuration = new NLog.Config.XmlLoggingConfiguration("Nlog.config");
            if (!Directory.Exists("./log"))
            {
                Directory.CreateDirectory("./log");
                logger.Info("Log folder created.");
            }
            logger.Info("Application Started");
            try
            {
                var dayBefore = ConfigurationManager.AppSettings["dayBefore"];
                var reportLocation = ConfigurationManager.AppSettings["reportLocation"];

                DataTable data = GetNodeListInWorkspace(dayBefore);
                if (data != null || data.Rows.Count > 0)
                {
                    var ticket = GetSessionTicket();
                    logger.Info("Total Item : "+ data.Rows.Count);

                    data.Columns.Add("Status", typeof(string));
                    data.Columns.Add("DeletedAt", typeof(string));
                    for (var i=0; i < data.Rows.Count; i++)
                    {

                        var res = DeleteNode(Convert.ToInt64(data.Rows[i]["DataID"]), ticket);
                        data.Rows[i]["DeletedAt"] = DateTime.Now;
                        data.Rows[i]["Status"] = res;

                    }
                }
                else
                {
                    if (data == null)
                    {
                        logger.Info("There is no data in workspace created below " + dayBefore + " days." );
                    }
                    else
                    {
                        logger.Info("Total Item : " + data.Rows.Count);
                    }
                }
                var dateTime = DateTime.Now.ToString("MMddyyhhmm");
                var reportName = "report_" + dateTime + ".csv";
                WriteToCsvFile(data, reportLocation, reportName);
                logger.Info("Application End");
                NLog.LogManager.Shutdown();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                logger.Error(ex);
                NLog.LogManager.Shutdown();
                throw ex;
            }
        }

        public static DataTable GetNodeListInWorkspace(string day)
        {
            DataTable table = new DataTable();
            using (var conn = new SqlConnection(GetConnectionString()))
            {
                using (var cmd = conn.CreateCommand())
                {
                    String SQL =
                        $@" SELECT distinct dtc.DataID, dtc.Name, (select Name from KUAF where ID = dtp.CreatedBy) as 'Username', dtc.CreateDate FROM DTreeCore dtc 
                            left join DTreeCore dtp on dtp.DataID = dtc.ParentID  
                            where dtp.SubType = 142 and dtc.Deleted = 0 and dtc.CreateDate < DATEADD(day, -{day}, GETDATE())
                        ";
                    SqlDataAdapter adapter = new SqlDataAdapter();
                    cmd.CommandText = SQL;

                   adapter.SelectCommand = cmd;
                   adapter.Fill(table);
                }
                conn.Close();
            }

            return table;
        }

        internal static string GetSessionTicket()
        {
           using (var client = new HttpClient())
            {
                var CS_URL = ConfigurationManager.AppSettings["CS_REST_URL"];
                var url = CS_URL + "v1/auth";
                var otcs_username =  ConfigurationManager.AppSettings["OTCS_Username"];
                var otcs_password = ConfigurationManager.AppSettings["OTCS_Password"];

                var formContent = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("username", otcs_username),
                    new KeyValuePair<string, string>("password", otcs_password)
                });

                var response = client.PostAsync(url, formContent).GetAwaiter().GetResult();
                if (response.IsSuccessStatusCode)
                {
                    var resString = response.Content.ReadAsStringAsync().Result;
                    var resJson = JsonConvert.DeserializeObject<dynamic>(resString);
                    var ticket = resJson["ticket"];
                    logger.Info("Authenticated with Ticket: " + ticket);
                    return ticket;
                }
                else
                {
                    return null;
                }
            }
        }

        internal static string DeleteNode(long nodeID, string ticket)
        {
            using (var client = new HttpClient())
            {
                var CS_URL = ConfigurationManager.AppSettings["CS_REST_URL"];
                var url = CS_URL + "v1/nodes/" + nodeID;
                client.DefaultRequestHeaders.Add("otcsticket", ticket);
                var response = client.DeleteAsync(url).GetAwaiter().GetResult();
                if (response.IsSuccessStatusCode)
                {
                    var responseContent = "success deleting " + nodeID;
                    Console.WriteLine(responseContent);
                    logger.Info(responseContent);
                    return "Deleted";
                }
                else
                {
                    logger.Error("Error Deleting " + nodeID);
                    logger.Error(response.Content.ReadAsStringAsync().Result);
                    return "";
                }
            }
        }

        private static void WriteToCsvFile(DataTable dataTable, string filePath, string reportName)
        {
            StringBuilder fileContent = new StringBuilder();

            foreach (var col in dataTable.Columns)
            {
                fileContent.Append(col.ToString() + ",");
            }

            fileContent.Replace(",", Environment.NewLine, fileContent.Length - 1, 1);

            foreach (DataRow dr in dataTable.Rows)
            {
                foreach (var column in dr.ItemArray)
                {
                    fileContent.Append("\"" + column.ToString() + "\",");
                }

                fileContent.Replace(",", Environment.NewLine, fileContent.Length - 1, 1);
            }
            if (!Directory.Exists(filePath))
            {
                Directory.CreateDirectory(filePath);
                logger.Info("report folder created.");
            }
            File.WriteAllText(filePath + reportName, fileContent.ToString());
        }


        internal static string GetConnectionString()
        {
            string connectionString = ConfigurationManager.AppSettings["DB_ConnectionString_WOCredentials"];
            bool UseSecureCredentials = bool.Parse(ConfigurationManager.AppSettings["UseSecureCredentials"]);

                string dbLoginId;
                string dbPassword;
                if (UseSecureCredentials)
                {
                    dbLoginId = SecureInfo.getSensitiveInfo(ConfigurationManager.AppSettings["SecureCSDBUsername_Filename"]);
                    dbPassword = SecureInfo.getSensitiveInfo(ConfigurationManager.AppSettings["SecureCSDBPassword_Filename"]);
                }
                else
                {
                    dbLoginId = ConfigurationManager.AppSettings["DB_Username"];
                    dbPassword = ConfigurationManager.AppSettings["DB_Password"];
                }

                //"DATA SOURCE=;PERSIST SECURITY INFO=True;Pooling = False;"//ORACLE connectionString
                connectionString += " USER ID =" + dbLoginId + "; password =" + dbPassword + ";";
            return connectionString;
        }
    }

    public class SecureInfo
    {
        public static string getSensitiveInfo(string secureFileName)
        {
            string credentialsDirectory = ConfigurationManager.AppSettings["SecureCredentialsPath"];
            string AESKeyFilePath = Path.Combine(credentialsDirectory, ConfigurationManager.AppSettings["SecureAESKey_Filename"]);
            string secureFilePath = Path.Combine(credentialsDirectory, secureFileName);
            return CryptographyAES_GCM.ReadEncryptedData(secureFilePath, AESKeyFilePath);
        }
    }
}
