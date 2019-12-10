using System.Collections.Generic;
using System.Data;
using IntegratorCore.Domain.Repository;
using IntegratorNet.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelClienteImpl : IGenerateData<SibelCliente>
    {
        IConfiguration _configuration;

        public SibelClienteImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelCliente> generic)
        {
            DataTable table = new DataTable();
            // Getting datatable layout from database
            table = GetDataTableLayout("clog_crm_cliente");

            // Pupulate datatable
            foreach (SibelCliente link in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = link.Id;
                row["EVENTO"] = link.Evento;
                row["DT_EVENTO"] = link.DtEvento;                
                row["CD_CLIENTE"] = link.CdCliente;
                row["TIPO_CLIENTE"] = link.TipoCliente;
                row["NM_RAZAO_SOCIAL"] = link.NmRazaoSocial;
                row["CD_GRUPO_ECONOMICO"] = link.CdGrupoEconomico;
                row["CD_TIPO_DOCUMENTO"] = link.CdTipoDocumento;
                row["TIPO_AGENCIA"] = link.TipoAgencia;
                row["FLG_BV"] = link.FlgBv;
                row["FLG_COMISSAO"] = link.FlgComissao;
                row["FLG_ATND_MIDIA"] = link.FlgAtndMidia;
                row["DT_INSERT"] = link.DtInsert;
                row["DT_UPDATE"] = link.DtUpdate;
                row["DS_SUBTIPO_CLIENTE"] = link.DsSubtipoCliente;
                row["CD_DDD"] = link.CdDdd;
                row["NM_BAIRRO"] = link.NmBairro;
                row["NM_CIDADE"] = link.NmCidade;
                row["SG_ESTADO"] = link.SgEstado;
                row["NM_PAIS"] = link.NmPais;
                row["CD_CEP"] = link.CdCep;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_cliente");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT ID, EVENTO, DT_EVENTO CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA," + 
                                "FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO," +
                                "NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP FROM BI_STG.STG_CRM_CLIENTE";
                using (OracleDataAdapter adapter = new OracleDataAdapter(query, connection))
                {
                    adapter.Fill(table);
                };
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
                        cmd.CommandText = $"INSERT INTO clog_crm_cliente " + 
                            "(ID, EVENTO, DT_EVENTO, CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA," + 
                            "FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO," +
                            "NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP FROM BI_STG.STG_CRM_CLIENTE)" + 
                            "VALUES (ID, EVENTO, DT_EVENTO, CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA," + 
                            "FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO," +
                            "NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP FROM BI_STG.STG_CRM_CLIENTE)" +
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
                    }
                }
            }
        }
    }
}