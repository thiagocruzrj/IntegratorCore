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
    public class SibelGrpEconomicoImpl : IGenerateData<SibelGrpEconomico>
    {
        IConfiguration _configuration;

        public SibelGrpEconomicoImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelGrpEconomico> generic)
        {
            DataTable table = new DataTable();
            // Getting datatable layout from database
            table = GetDataTableLayout("clog_crm_grp_economico");

            // Pupulate datatable
            foreach (SibelGrpEconomico link in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = link.Id;
                row["EVENTO"] = link.Evento;
                row["DT_EVENTO"] = link.DtEvento;                
                row["CD_GRP_ECONOMICO"] = link.CdGrpEconomico;
                row["NM_GRP_ECONOMICO"] = link.NmGrpEconimico;
                row["RAZAO_SOCIAL"] = link.RazaoSocial;
                row["TIPO_DOCUMENTO"] = link.TipoDocumento;
                row["NUM_DOCUMENTO"] = link.NumDocumento;
                row["NM_WEBSITE"] = link.NmWebsite;
                row["CD_ATUACAO_MARCA"] = link.CdAtuacaoMarca;
                row["ATENDIMENTO_LOJA"] = link.AtendimentoLoja;
                row["ATENDIMENTO_MALL"] = link.AtendimentoMall;
                row["ATENDIMENTO_MIDIA"] = link.AtendimentoMidia;
                row["KA_RESP_LOJA"] = link.KaRespLoja;
                row["KA_RESP_MALL_MIDIA"] = link.KaRespMallMidia;
                row["DT_INSERT"] = link.DtInsert;
                row["DT_UPDATE"] = link.DtUpdate;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_grp_economico");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT ID, EVENTO, DT_EVENTO, CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                                "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA, " +
                                "KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO";
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
                        cmd.CommandText = $"INSERT INTO clog_crm_grp_economico " + 
                            "(ID, EVENTO, DT_EVENTO, CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                            "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA, " +
                            "KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO)" + 
                            "VALUES (ID, EVENTO, DT_EVENTO, CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                            "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA, " +
                            "KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO" +
                            "FROM BI_STG.STG_CRM_CLIENTE";

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