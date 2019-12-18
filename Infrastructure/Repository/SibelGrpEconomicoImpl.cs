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
    public class SibelGrpEconomicoImpl : IGenerateData<SibelGrpEconomicoOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";      
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";     
        private readonly string _dmlSelect = @"SELECT CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                                "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA, " +
                                "KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO";
        private readonly string _dmlInsert = $"INSERT INTO clog_crm_grp_economico " + 
                            "(EVENTO, DT_EVENTO,CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                            "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO) VALUES";
        
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
            IList<SibelGrpEconomicoOracle> _sibelGrpEconomico = new List<SibelGrpEconomicoOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelGrpEconomico.Add(
                        new SibelGrpEconomicoOracle(
                            (string)row["CD_GRP_ECONOMICO"].ToString(),
                            (string)row["NM_GRP_ECONOMICO"].ToString(),
                            (string)row["RAZAO_SOCIAL"].ToString(),
                            (string)row["TIPO_DOCUMENTO"].ToString(),
                            (string)row["NUM_DOCUMENTO"].ToString(),
                            (string)row["NM_WEBSITE"].ToString(),
                            (string)row["CD_ATUACAO_MARCA"].ToString(),
                            (string)row["ATENDIMENTO_LOJA"].ToString(),
                            (string)row["ATENDIMENTO_MALL"].ToString(),
                            (string)row["ATENDIMENTO_MIDIA"].ToString(),
                            (string)row["KA_RESP_LOJA"].ToString(),
                            (string)row["KA_RESP_MALL_MIDIA"].ToString(),
                            (string)row["DT_INSERT"].ToString(),
                            (string)row["DT_UPDATE"].ToString()

                        )
                    );                    
                }
                BulkInsertMySQL(_sibelGrpEconomico);
            }
        }

        public void GetResult(string sql, List<SibelGrpEconomicoOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelGrpEconomicoOracle> results)
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
                                        $" ('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdGrpEconomico}','{linha.NmGrpEconimico}','{linha.RazaoSocial}','{linha.TipoDocumento}','{linha.NumDocumento}','{linha.NmWebsite}','{linha.CdAtuacaoMarca}','{linha.AtendimentoLoja}','{linha.AtendimentoMall}','{linha.AtendimentoMidia}','{linha.KaRespLoja}','{linha.KaRespMallMidia}','{linha.DtInsert}','{linha.DtUpdate}')";
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