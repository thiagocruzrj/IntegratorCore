using System;
using System.Collections.Generic;
using System.Data;
using IntegratorCore.Domain.Repository;
using IntegratorNet.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelSegmtoAbrasceImpl : IGenerateData<SibelSegmtoAbrasce>
    {
        IConfiguration _configuration;

        public SibelSegmtoAbrasceImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelSegmtoAbrasce> generic)
        {
            DataTable table = new DataTable();
            table = GetDataTableLayout("clog_crm_segmto_abrasce");

            // Pupulate datatable
            foreach (SibelSegmtoAbrasce item in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = item.Id;
                row["EVENTO"] = item.Evento;
                row["DT_EVENTO"] = item.DtEvento;
                row["Nm_Segmto_Abrasce"] = item.NmSegmtoAbrasce;
                row["Cd_Segmto_Abrasce"] = item.CdSegmtoAbrasce;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_segmto_abrasce");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"select ID, EVENTO, DT_EVENTO, Cd_Segmto_Abrasce, Nm_Segmto_Abrasce from BI_STG.STG_CRM_SEGMTO_ABRASCE";
                using (OracleDataAdapter adapter = new OracleDataAdapter(query, connection))
                {
                    adapter.Fill(table);
                };
                connection.Close();
            }
            return table;
        }

        public void BulkInsertMySQL(DataTable table, string tableName)
        {
            var connectionString = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;
            Uid=myUsername; Integrated Security=True;";
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();

                using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.Transaction = tran;
                        cmd.CommandText = $"INSERT INTO clog_crm_segmto_abrasce values (ID, EVENTO, DT_EVENTO, Cd_Segmto_Abrasce, Nm_Segmto_Abrasce) from BI_STG.STG_CRM_SEGMTO_ABRASCE" ;

                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            adapter.UpdateBatchSize = 1000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                            {
                                cb.SetAllValues = true;
                                adapter.Update(table);
                                tran.Commit();
                            }
                        };
                        connection.Close();
                    }
                }
            }
        }
    }
}