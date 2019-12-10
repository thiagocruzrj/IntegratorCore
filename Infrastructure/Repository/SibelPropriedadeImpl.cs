using System.Collections.Generic;
using System.Data;
using IntegratorCore.Domain.Repository;
using IntegratorNet.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelPropriedadeImpl : IGenerateData<SibelPropriedade>
    {
        IConfiguration _configuration;

        public SibelPropriedadeImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelPropriedade> generic)
        {
            DataTable table = new DataTable();
            // Getting datatable layout from database
            table = GetDataTableLayout("clog_crm_propriedade");

            // Pupulate datatable
            foreach (SibelPropriedade item in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = item.Id;
                row["EVENTO"] = item.Evento;
                row["DT_EVENTO"] = item.DtEvento;                
                row["CD_PROPRIEDADE_SIEBEL"] = item.CdPropriedade;                
                row["TIPO_PROPRIEDADE"] = item.TipoPropriedade;                
                row["ORGID_CPI_ASSOCIADA"] = item.OrigdCpiAssociada;                
                row["IND_STATUS"] = item.IndStatus;                
                row["IND_SITUACAO"] = item.IndSituacao;                 
                row["IND_DISPONIB_LOCACAO"] = item.IndDisponibLocacao;
                row["CATEGORIA_PROPRIEDADE"] = item.CategoriaPropriedade;
                row["IND_ALERTA"] = item.IndAlerta;
                row["CLASSIFICACAO_ABC"] = item.ClassificacaoAbc;
                row["ABL"] = item.Abl;
                row["VL_CRD"] = item.VlCrd;
                row["TIPO_MATERIAL"] = item.TipoEstrutura;
                row["TIPO_ESTRUTURA"] = item.TipoEstrutura;
                row["TIPO_VENDA"] = item.TipoVenda;
                row["QTD_COMERCIALIZADA"] = item.QtdComercializada;
                row["DT_ULT_UPD_VALORES"] = item.DlUltUpdValores;
                row["ULTIMO_ALUGUEL_FATUR"] = item.UltimoAluguelPercent;
                row["ULTIMO_ALUGUEL_PERCENT"] = item.UltimoAluguelPercent;
                row["ULTIMO_ALUGUEL_MINIMO"] = item.UltimoAluguelMinimo;
                row["ULTIMO_ALUGUEL_CONTRATO"] = item.UltimoAluguelContrato;
                row["ULTIMO_CONDOMINIO"] = item.UltimoCondominio;
                row["ULTIMO_FUNDO_PROMO"] = item.UltimoFundoPromo;
                row["ULTIMA_ATIVIDADE"] = item.UltimoAtividade;
                row["ULTIMO_LOJISTA"] = item.UltimoLogista;
                row["DT_SAIDA_LOJISTA"] = item.DtSaidaLogista;
                row["AMM_TOTAL_PRIMEIRO_ANO_VIAB"] = item.AnmTotalPrimeiroAnoViab;
                row["ALUGUEL_M2_PRIMEIRO_ANO_VIAB"] = item.AluguelM2PrimeiroAnoViab;
                row["PRIMEIRO_ANO_VIAB"] = item.PrimeiroAnoViaB;
                row["CDU_M2_VIAB"] = item.CduM2ViaB;
                row["CDU_VIAB"] = item.CduViaB;
                row["NUM_ALUGUEIS_ANO_VIAB"] = item.NumAlugueisAnoViaB;
                row["CONDOMINIO_VIAB"] = item.CondominioViaB;
                row["PREVISAO_SEGMTO_VIAB"] = item.PrevisaoSegmtoViaB;
                row["ALUGUEL_M2_PRIMEIRO_ANO_TAB"] = item.AluguelM2PrimeiroAnoTab;
                row["CDU_M2_TAB"] = item.CduM2Tab;
                row["CDU_TAB"] = item.CduTab;
                row["VGL_TAB"] = item.VglViaB;
                row["SETOR_LOCALIZACAO"] = item.SetorLocalizacao;
                row["DT_FIM_REFORMA"] = item.DtFimReforma;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_propriedade");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT (ID, EVENTO,DT_EVENTO, CD_PROPRIEDADE_SIEBEL,CD_PROPRIEDADE,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,ALUGUEL_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "PAVIMENTO_LOCALIZACAO,CD_CPI_ASSOCIADA,DT_INSERT,DT_UPDATE,IND_RESERVA_TECNICA,DT_INI_REFORMA, " +
                                "DT_FIM_REFORMA from BI_STG.STG_CRM_PROPRIEDADE) FROM BI_STG.STG_CRM_PROPRIEDADE"; 
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
                        cmd.CommandText = $"INSERT INTO clog_crm_propriedade (ID, EVENTO,DT_EVENTO, CD_PROPRIEDADE_SIEBEL,CD_PROPRIEDADE,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,ALUGUEL_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "PAVIMENTO_LOCALIZACAO,CD_CPI_ASSOCIADA,DT_INSERT,DT_UPDATE,IND_RESERVA_TECNICA,DT_INI_REFORMA, " +
                                "DT_FIM_REFORMA from BI_STG.STG_CRM_PROPRIEDADE)" + 
                                "VALUES (ID, EVENTO,DT_EVENTO, CD_PROPRIEDADE_SIEBEL,CD_PROPRIEDADE,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,ALUGUEL_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "PAVIMENTO_LOCALIZACAO,CD_CPI_ASSOCIADA,DT_INSERT,DT_UPDATE,IND_RESERVA_TECNICA,DT_INI_REFORMA, " +
                                "DT_FIM_REFORMA from BI_STG.STG_CRM_PROPRIEDADE) FROM BI_STG.STG_CRM_PROPRIEDADE";

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