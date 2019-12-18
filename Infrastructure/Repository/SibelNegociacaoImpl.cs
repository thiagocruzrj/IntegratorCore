using System.Collections.Generic;
using System.Data;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Domain.Entities;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Linq;

namespace IntegratorCore.Cmd.Infrastructure.Repository
{
    public class SibelNegociacaoImpl : IGenerateData<SibelNegociacaoOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";     
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";      
        private readonly string _dmlSelect = @"SELECT CD_NEGOCIACAO ,CD_NEGOCIACAO_PN ,CD_NEGOCIACAO_ASSOC, " +
                            "FL_NEGOCIACAO_CERTIFICADA,CD_MARCA,CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL, " +
                            "DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL,ENERGIA_TOTAL,WISHLIST,STATUS_FATURAMENTO,VL_DESCONTO,DT_INI_VIGENCIA, " +
                            "DT_FIM_VIGENCIA,CATEGORIA_MIDIA,RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC, " +
                            "MOTIVO_PERDA,DT_INSERT,DT_UPDATE,DS_TP_ACAO_JURIDICA from BI_STG.STG_CRM_NEGOCIACAO";
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_negociacao " + 
                            "(EVENTO, DT_EVENTO,CD_NEGOCIACAO ,CD_NEGOCIACAO_PN ,CD_NEGOCIACAO_ASSOC, " +
                            "FL_NEGOCIACAO_CERTIFICADA,CD_MARCA,CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL, " +
                            "DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL,ENERGIA_TOTAL,WISHLIST,STATUS_FATURAMENTO,VL_DESCONTO,DT_INI_VIGENCIA, " +
                            "DT_FIM_VIGENCIA,CATEGORIA_MIDIA,RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC, " +
                            "MOTIVO_PERDA,DT_INSERT,DT_UPDATE,DS_TP_ACAO_JURIDICA)";

        public void GetResult()
        {
            //TODO: Alterar ADO pelo Dapper hidratando uma Entidade
            DataTable table = new DataTable();
            using (OracleConnection connection = new OracleConnection(_connectionStringOracle))
            {
                connection.Open();                                
                using (OracleDataAdapter adapter = new OracleDataAdapter(_dmlSelect, connection))
                {
                    adapter.Fill(table);
                };
                connection.Close();
            }                
            // Getting datatable layout from database
            IList<SibelNegociacaoOracle> _sibelNegociacao = new List<SibelNegociacaoOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelNegociacao.Add(
                        new SibelNegociacaoOracle(
                            (string)row["CD_NEGOCIACAO"].ToString(),
                            (string)row["CD_NEGOCIACAO_PN"].ToString(),
                            (string)row["CD_NEGOCIACAO_ASSOC"].ToString(),
                            (string)row["FL_NEGOCIACAO_CERTIFICADA"].ToString(),
                            (string)row["CD_MARCA"].ToString(),
                            (string)row["CD_CLIENTE"].ToString(),
                            (string)row["CD_SHOPPING"].ToString(),
                            (string)row["TIPO_OPORTUNIDADE"].ToString(),
                            (string)row["ESTAGIO"].ToString(),
                            (string)row["RECEITA_TOTAL"].ToString(),
                            (string)row["DT_EMISSAO"].ToString(),
                            (string)row["TIPO_PAGAMENTO"].ToString(),
                            (string)row["ALUGUEL_TOTAL"].ToString(),
                            (string)row["ENERGIA_TOTAL"].ToString(),
                            (string)row["WISHLIST"].ToString(),
                            (string)row["STATUS_FATURAMENTO"].ToString(),
                            (string)row["VL_DESCONTO"].ToString(),
                            (string)row["DT_INI_VIGENCIA"].ToString(),
                            (string)row["DT_FIM_VIGENCIA"].ToString(),
                            (string)row["CATEGORIA_MIDIA"].ToString(),
                            (string)row["RAMO"].ToString(),
                            (string)row["CATEGORIA"].ToString(),
                            (string)row["DT_VENCIMENTO"].ToString(),
                            (string)row["DT_ASSINATURA"].ToString(),
                            (string)row["MOTIVO_REPROV"].ToString(),
                            (string)row["MOTIVO_REPROV_DOC"].ToString(),
                            (string)row["MOTIVO_PERDA"].ToString(),
                            (string)row["DT_INSERT"].ToString(),
                            (string)row["DT_UPDATE"].ToString(),
                            (string)row["DS_TP_ACAO_JURIDICA"].ToString()
                        )
                    );                    
                }
                BulkInsertMySQL(_sibelNegociacao);
            }
        }

        public void GetResult(string sql, List<SibelNegociacaoOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelNegociacaoOracle> results)
        { //TODO: Alterar o ADO pelo Dapper.
          //TODO: Alterar para respeitar mesmo a quantidade de dados. Aqui ele está fazendo um por um.          
            try
            {
                
                if(results != null && results.Count > 0)
                {
                    //Configura a Paginação
                    int total = results.Count; //Pega o total de resultados na lista
                    int pageSize = 1000; //Quantidade de registros que vamos processar                    
                    int totalPages = total / pageSize; //Baseado no total e nos registros, qual o total de páginas para iterar
                    using (MySqlConnection connection = new MySqlConnection(_connectionStringMySQL)) 
                    {                    
                        connection.Open();

                        //Pega a página de processamento atual e começa a iteração
                        for(int page = 0; page <= totalPages; page++)
                        {
                            //Lista Paginada
                            var paginatedResults = results
                            .Skip(pageSize * (page))
                            .Take(pageSize)
                            .ToList(); 

                            //Abre a Transação para a quantidade atual paginada
                            using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
                            {
                                //Executa a operação com o total de linhas desta página
                                foreach(var linha in paginatedResults)
                                {                                    
                                    using (MySqlCommand cmd = new MySqlCommand())
                                    {
                                        cmd.Connection = connection;
                                        cmd.Transaction = tran;
                                        cmd.CommandText = _dmlInsert + "values " +
                                        $" ('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.Negociacao}','{linha.NegociacaoPn}','{linha.NegociacaoAssoc}','{linha.NegociacaoCertificada}','{linha.Marca}','{linha.Cliente}','{linha.Shopping}','{linha.TipoOportunidade}','{linha.Estagio}','{linha.ReceitaTotal}','{linha.DtEmissao}','{linha.TipoPagamento}','{linha.AluguelTotal}','{linha.EnergiaTotal}','{linha.Wishlist}','{linha.StatusFaturamento}','{linha.VlDesconto}','{linha.DtIniVigencia}','{linha.DtFimVigencia}','{linha.CategoriaMidia}','{linha.Ramo}','{linha.Categoria}','{linha.DtVencimento}','{linha.DtAssinatura}','{linha.MotivoReprov}','{linha.MotivoReprovDoc}','{linha.MotivoPerde}','{linha.DtInsert}','{linha.DtUpdate}','{linha.DsTpAcaoJuridica}')";
                                        cmd.ExecuteNonQuery();
                                    }                                
                                }                                
                                tran.Commit();
                            }                                                    
                        } 
                        connection.Close();                   
                    }                                                                           
                }                             
            }
            catch (Exception)
            {                
                throw;
            }            
        }
    }
}