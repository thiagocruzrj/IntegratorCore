using System.Collections.Generic;
using System.Data;
using IntegratorCore.Domain.Repository;
using IntegratorNet.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorCore.Infrastructure.Repository
{
    public class test : IGenerateData<SibelCategoriaAbrasce>
    {
        IConfiguration _configuration;

        public test(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelCategoriaAbrasce> generic)
        {
            DataTable table = new DataTable();
            // Getting datatable layout from database
            table = GetDataTableLayout("clog_crm_categoria_abrasce");

        // Pupulate datatable
        foreach (SibelCategoriaAbrasce link in generic)
        {
            DataRow row = table.NewRow();                
            //row["LinkURL"] = link.LinkURL;
            //row["CreateDate"] = link.CreateDate;
            //row["Titel"] = link.Titel;
            table.Rows.Add(row);
        }

        BulkInsertMySQL(table, "clog_crm_categoria_abrasce");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"select cd_categoria_abrasce, nm_categoria_abrasce from BI_STG.STG_CRM_CATEGORIA_ABRASCE";
                using (OracleDataAdapter adapter = new OracleDataAdapter(query, connection))
                {
                    adapter.Fill(table);
                };
            }
            return table;
        }

        public void BulkInsertMySQL(DataTable table, string tableName)
        {
            var connectionString = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel; Uid=myUsername; Integrated Security=True;";
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();

                using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.Transaction = tran;
                        cmd.CommandText = $"INSERT clog_crm_categoria_abrasce SET " + 
                        "NM_CATEGORIA_ABRASCE = item.NmCategoriaAbrasce, " + 
                        "WHERE CD_CATEGORIA_ABRASCE = item.CdCategoriaAbrasce from BI_STG.STG_CRM_CATEGORIA_ABRASCE";

                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            adapter.UpdateBatchSize = 10000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                            {
                                cb.SetAllValues = true;
                                adapter.Update(table);
                                tran.Commit();
                            }
                        };
                    }
                }
            }
        }
    }
}