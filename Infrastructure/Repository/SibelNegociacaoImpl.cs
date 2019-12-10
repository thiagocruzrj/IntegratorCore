using System.Collections.Generic;
using System.Data;
using IntegratorCore.Domain.Repository;
using IntegratorNet.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelNegociacaoImpl : IGenerateData<SibelNegociacao>
    {
        IConfiguration _configuration;

        public SibelNegociacaoImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelNegociacao> generic)
        {
            DataTable table = new DataTable();
            // Getting datatable layout from database
            table = GetDataTableLayout("clog_crm_negociacao");

            // Pupulate datatable
            foreach (SibelNegociacao item in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = item.Id;
                row["EVENTO"] = item.Evento;
                row["DT_EVENTO"] = item.DtEvento;                
                row["CD_NEGOCIACAO"] = item.Negociacao;                
                row["CD_NEGOCIACAO_PN"] = item.NegociacaoPn;                
                row["CD_NEGOCIACAO_ASSOC"] = item.NegociacaoAssoc;                
                row["FL_NEGOCIACAO_CERTIFICADA"] = item.NegociacaoAssoc;                
                row["CD_MARCA"] = item.DtEvento;                 
                row["CD_CLIENTE"] = item.Cliente;
                row["CD_SHOPPING"] = item.Shopping;
                row["TIPO_OPORTUNIDADE"] = item.TipoOportunidade;
                row["ESTAGIO"] = item.Estagio;
                row["RECEITA_TOTAL"] = item.ReceitaTotal;
                row["DT_EMISSAO"] = item.DtEmissao;
                row["TIPO_PAGAMENTO"] = item.TipoPagamento;
                row["ALUGUEL_TOTAL"] = item.AluguelTotal;
                row["ENERGIA_TOTAL"] = item.EnergiaTotal;
                row["WISHLIST"] = item.Wishlist;
                row["STATUS_FATURAMENTO"] = item.StatusFaturamento;
                row["VL_DESCONTO"] = item.VlDesconto;
                row["DT_INI_VIGENCIA"] = item.DtIniVigencia;
                row["DT_FIM_VIGENCIA"] = item.DtFimVigencia;
                row["CATEGORIA_MIDIA"] = item.CategoriaMidia;
                row["RAMO"] = item.Ramo;
                row["CATEGORIA"] = item.Categoria;
                row["DT_VENCIMENTO"] = item.DtVencimento;
                row["DT_ASSINATURA"] = item.DtAssinatura;
                row["MOTIVO_REPROV"] = item.MotivoReprov;
                row["MOTIVO_REPROV_DOC"] = item.MotivoReprovDoc;
                row["MOTIVO_PERDA"] = item.MotivoPerde;
                row["DT_INSERT"] = item.DtInsert;
                row["DT_UPDATE"] = item.DtUpdate;
                row["DS_TP_ACAO_JURIDICA"] = item.DsTpAcaoJuridica;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_negociacao");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT ID, EVENTO, DT_EVENTO, CD_NEGOCIACAO ,CD_NEGOCIACAO_PN ,CD_NEGOCIACAO_ASSOC, " +
                "FL_NEGOCIACAO_CERTIFICADA,CD_MARCA,CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL, " +
                "DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL,ENERGIA_TOTAL,WISHLIST,STATUS_FATURAMENTO,VL_DESCONTO,DT_INI_VIGENCIA, " +
                "DT_FIM_VIGENCIA,CATEGORIA_MIDIA,RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC, " +
                "MOTIVO_PERDA,DT_INSERT,DT_UPDATE,DS_TP_ACAO_JURIDICA from BI_STG.STG_CRM_NEGOCIACAO";
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
                        cmd.CommandText = $"INSERT INTO clog_crm_negociacao " + 
                            "(ID, EVENTO, DT_EVENTO, CD_NEGOCIACAO ,CD_NEGOCIACAO_PN ,CD_NEGOCIACAO_ASSOC, " +
                            "FL_NEGOCIACAO_CERTIFICADA,CD_MARCA,CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL, " +
                            "DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL,ENERGIA_TOTAL,WISHLIST,STATUS_FATURAMENTO,VL_DESCONTO,DT_INI_VIGENCIA, " +
                            "DT_FIM_VIGENCIA,CATEGORIA_MIDIA,RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC, " +
                            "MOTIVO_PERDA,DT_INSERT,DT_UPDATE,DS_TP_ACAO_JURIDICA )" + 
                            "VALUES (ID, EVENTO, DT_EVENTO, CD_NEGOCIACAO ,CD_NEGOCIACAO_PN ,CD_NEGOCIACAO_ASSOC, " +
                            "FL_NEGOCIACAO_CERTIFICADA,CD_MARCA,CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL, " +
                            "DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL,ENERGIA_TOTAL,WISHLIST,STATUS_FATURAMENTO,VL_DESCONTO,DT_INI_VIGENCIA, " +
                            "DT_FIM_VIGENCIA,CATEGORIA_MIDIA,RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC, " +
                            "MOTIVO_PERDA,DT_INSERT,DT_UPDATE,DS_TP_ACAO_JURIDICA) FROM BI_STG.STG_CRM_NEGOCIACAO";

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