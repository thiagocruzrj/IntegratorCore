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
    public class SibelPropriedadeImpl : IGenerateData<SibelPropriedadeOracle>
    {
        private readonly string _connectionStringMySQL = @"";         
        private readonly string _connectionStringOracle = @"";    
        private readonly string _dmlSelect = @"SELECT CD_PROPRIEDADE_SIEBEL,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "DT_FIM_REFORMA, " +
                                "DT_FIM_REFORMA from BI_STG.STG_CRM_PROPRIEDADE"; 
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_propriedade (EVENTO, DT_EVENTO,CD_PROPRIEDADE_SIEBEL,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "DT_FIM_REFORMA)";

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
            IList<SibelPropriedadeOracle> _sibelPropriedade = new List<SibelPropriedadeOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelPropriedade.Add(
                        new SibelPropriedadeOracle(
                            (string)row["CD_PROPRIEDADE_SIEBEL"].ToString(),
                            (string)row["TIPO_PROPRIEDADE"].ToString(),
                            (string)row["ORGID_CPI_ASSOCIADA"].ToString(),
                            (string)row["IND_STATUS"].ToString(),
                            (string)row["IND_SITUACAO"].ToString(),
                            (string)row["IND_DISPONIB_LOCACAO"].ToString(),
                            (string)row["CATEGORIA_PROPRIEDADE"].ToString(),
                            (string)row["IND_ALERTA"].ToString(),
                            (string)row["CLASSIFICACAO_ABC"].ToString(),
                            (string)row["ABL"].ToString(),
                            (string)row["VL_CRD"].ToString(),
                            (string)row["TIPO_MATERIAL"].ToString(),
                            (string)row["TIPO_ESTRUTURA"].ToString(),
                            (string)row["PERIODO_VENDA"].ToString(),
                            (string)row["TIPO_VENDA"].ToString(),
                            (string)row["QTD_COMERCIALIZADA"].ToString(),
                            (string)row["DT_ULT_UPD_VALORES"].ToString(),
                            (string)row["ULTIMO_ALUGUEL_FATUR"].ToString(),
                            (string)row["ULTIMO_ALUGUEL_PERCENT"].ToString(),
                            (string)row["ULTIMO_ALUGUEL_MINIMO"].ToString(),
                            (string)row["ULTIMO_ALUGUEL_CONTRATO"].ToString(),
                            (string)row["ULTIMO_CONDOMINIO"].ToString(),
                            (string)row["ULTIMO_FUNDO_PROMO"].ToString(),
                            (string)row["ULTIMA_ATIVIDADE"].ToString(),
                            (string)row["ULTIMO_LOJISTA"].ToString(),
                            (string)row["DT_SAIDA_LOJISTA"].ToString(),
                            (string)row["AMM_TOTAL_PRIMEIRO_ANO_VIAB"].ToString(),
                            (string)row["AMM_TOTAL_VIAB"].ToString(),
                            (string)row["ALUGUEL_M2_PRIMEIRO_ANO_VIAB"].ToString(),
                            (string)row["PRIMEIRO_ANO_VIAB"].ToString(),
                            (string)row["CDU_M2_VIAB"].ToString(),
                            (string)row["CDU_VIAB"].ToString(),
                            (string)row["NUM_ALUGUEIS_ANO_VIAB"].ToString(),
                            (string)row["CONDOMINIO_VIAB"].ToString(),
                            (string)row["VGL_VIAB"].ToString(),
                            (string)row["PREVISAO_SEGMTO_VIAB"].ToString(),
                            (string)row["ALUGUEL_M2_PRIMEIRO_ANO_TAB"].ToString(),
                            (string)row["CDU_M2_TAB"].ToString(),
                            (string)row["CDU_TAB"].ToString(),
                            (string)row["VGL_TAB"].ToString(),
                            (string)row["SETOR_LOCALIZACAO"].ToString(),
                            (string)row["DT_FIM_REFORMA"].ToString()
                        )
                    );                    
                }
                BulkInsertMySQL(_sibelPropriedade);
            }
        }

        public void GetResult(string sql, List<SibelPropriedadeOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelPropriedadeOracle> results)
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
                                        $" ('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdPropriedadeSiebel}','{linha.TipoPropriedade}','{linha.OrigdCpiAssociada}','{linha.IndStatus}','{linha.IndSituacao}','{linha.IndDisponibLocacao}','{linha.CategoriaPropriedade}','{linha.IndAlerta}','{linha.ClassificacaoAbc}','{linha.Abl}','{linha.VlCrd}','{linha.TipoMaterial}','{linha.TipoEstrutura}','{linha.PeriodoVenda}','{linha.TipoVenda}','{linha.QtdComercializada}','{linha.DlUltUpdValores}','{linha.UltimoAluguelFatur}','{linha.UltimoAluguelPercent}','{linha.UltimoAluguelMinimo}','{linha.UltimoAluguelContrato}','{linha.UltimoCondominio}','{linha.UltimoFundoPromo}','{linha.UltimoAtividade}','{linha.UltimoLogista}','{linha.DtSaidaLogista}','{linha.AnmTotalPrimeiroAnoViab}','{linha.AnmTotalViab}','{linha.AluguelM2PrimeiroAnoViab}','{linha.PrimeiroAnoViaB}','{linha.CduM2ViaB}','{linha.CduViaB}','{linha.NumAlugueisAnoViaB}','{linha.CondominioViaB}','{linha.VglViaB}','{linha.PrevisaoSegmtoViaB}','{linha.AluguelM2PrimeiroAnoTab}','{linha.CduM2Tab}','{linha.CduTab}','{linha.VglTab}','{linha.SetorLocalizacao}','{linha.DtFimReforma}')";
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