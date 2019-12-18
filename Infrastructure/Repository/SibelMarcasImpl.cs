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
    public class SibelMarcasImpl : IGenerateData<SibelMarcasOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";        
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";    
        private readonly string _dmlSelect = @"SELECT CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                                "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                                "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                                "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                                "VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                                "KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_MARCAS";
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_marcas " + 
                            "(EVENTO, DT_EVENTO,CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                            "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                            "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                            "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                            "VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                            "KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE)";

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
            IList<SibelMarcasOracle> _sibelMarcas = new List<SibelMarcasOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelMarcas.Add(
                        new SibelMarcasOracle(
                            (string)row["CD_MARCA"].ToString(),
                            (string)row["NM_MARCA"].ToString(),
                            (string)row["RAZAO_SOCIAL"].ToString(),
                            (string)row["CD_GRUPO_ECONOMICO"].ToString(),
                            (string)row["TIPO_DOCUMENTO"].ToString(),
                            (string)row["NUM_DOCUMENTO"].ToString(),
                            (string)row["NM_WEBSITE"].ToString(),
                            (string)row["CD_ATUACAO_MARCA"].ToString(),
                            (string)row["CD_PERFIL_SOCIAL"].ToString(),
                            (string)row["CD_SEGMTO_ABRASCE"].ToString(),
                            (string)row["CD_CATEGORIA_ABRASCE"].ToString(),
                            (string)row["FLG_ANTENA"].ToString(),
                            (string)row["FLG_CAIXA_ELETRONICO"].ToString(),
                            (string)row["FLG_MIDIA"].ToString(),
                            (string)row["FLG_QUIOSQUE"].ToString(),
                            (string)row["FLG_LOJA"].ToString(),
                            (string)row["FLG_EVENTO"].ToString(),
                            (string)row["VL_FAIXA_METRAGEM_DE"].ToString(),
                            (string)row["VL_FAIXA_METRAGEM_ATE"].ToString(),
                            (string)row["VL_METRAGEM_MINIMA_EVENTO"].ToString(),
                            (string)row["VL_METRAGEM_MINIMA_QUIOSQUE"].ToString(),
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
                BulkInsertMySQL(_sibelMarcas);
            }
        }

        public void GetResult(string sql, List<SibelMarcasOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelMarcasOracle> results)
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
                                        $" ('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdMarca}','{linha.NmMarca}','{linha.RazaoSocial}','{linha.CdGrupoEconomico}','{linha.TipoDocumento}','{linha.NumDocumento}','{linha.NnWwebsite}','{linha.CdAtuacaoMarca}','{linha.CdPerfilSocial}','{linha.CdSegmentoAbrasce}','{linha.CdCategoriaAbrasce}','{linha.FlgAntena}','{linha.FlgCaixaEletronico}','{linha.FlgMedia}','{linha.FlgQuiosque}','{linha.FlgLoja}','{linha.FlgEvento}','{linha.VlFaixaMetragemDe}','{linha.VlFaixaMetragemAte}','{linha.VlMetragemMinimaEvento}','{linha.VlMetragemMinimaQuiosque}','{linha.AtendimentoLoja}','{linha.AtendimentoMall}','{linha.AtendimentoMidia}','{linha.KaRespLoja}','{linha.KaRespMallMidia}','{linha.DtInsert}','{linha.DtUpdate}')";
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