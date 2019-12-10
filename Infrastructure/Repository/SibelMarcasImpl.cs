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
    public class SibelMarcasImpl : IGenerateData<SibelMarcas>
    {
        IConfiguration _configuration;

        public SibelMarcasImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void GetResult(string sql, List<SibelMarcas> generic)
        {
            DataTable table = new DataTable();
            table = GetDataTableLayout("clog_crm_marcas");

            // Pupulate datatable
            foreach (SibelMarcas item in generic)
            {
                DataRow row = table.NewRow();
                row["ID"] = item.Id;
                row["EVENTO"] = item.Evento;
                row["DT_EVENTO"] = item.DtEvento;                
                row["CD_MARCA"] = item.CdMarca;
                row["NM_MARCA"] = item.NmMarca;
                row["RAZAO_SOCIAL"] = item.RazaoSocial;
                row["CD_GRUPO_ECONOMICO"] = item.CdGrupoEconomico;
                row["TIPO_DOCUMENTO"] = item.TipoDocumento;
                row["NM_WEBSITE"] = item.NnWwebsite;
                row["CD_ATUACAO_MARCA"] = item.CdAtuacaoMarca;
                row["CD_PERFIL_SOCIAL"] = item.AtendimentoLoja;
                row["CD_SEGMTO_ABRASCE"] = item.AtendimentoMall;
                row["CD_CATEGORIA_ABRASCE"] = item.AtendimentoMidia;
                row["FLG_ANTENA"] = item.KaRespLoja;
                row["FLG_CAIXA_ELETRONICO"] = item.KaRespMallMidia;
                row["FLG_MIDIA"] = item.FlgMedia;
                row["FLG_QUIOSQUE"] = item.FlgQuiosque;
                row["FLG_LOJA"] = item.FlgLoja;
                row["FLG_EVENTO"] = item.FlgEvento;
                row["VL_FAIXA_METRAGEM_DE"] = item.VlFaixaMetragemDe;
                row["VL_FAIXA_METRAGEM_ATE"] = item.VlFaixaMetragemAte;
                row["VL_METRAGEM_MINIMA_EVENTO"] = item.VlMetragemMinimaEvento;
                row["VL_METRAGEM_MINIMA_QUIOSQUE"] = item.VlMetragemMinimaQuiosque;
                row["ATENDIMENTO_LOJA"] = item.AtendimentoLoja;
                row["ATENDIMENTO_MALL"] = item.AtendimentoMall;
                row["ATENDIMENTO_MIDIA"] = item.AtendimentoMidia;
                row["KA_RESP_LOJA"] = item.KaRespLoja;
                row["KA_RESP_MALL_MIDIA"] = item.KaRespMallMidia;
                row["DT_INSERT"] = item.DtInsert;
                row["DT_UPDATE"] = item.DtUpdate;
                table.Rows.Add(row);
            }
            BulkInsertMySQL(table, "clog_crm_marcas");
        }

        public DataTable GetDataTableLayout(string tableName)
        {
            DataTable table = new DataTable();
            var connectionString = @"jdbc:oracle:thin:bi_read/bi#brmalls#26@192.168.100.170:1521/BRMBIPRD";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT ID, EVENTO, DT_EVENTO, Negociacao CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                                "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                                "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                                "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                                ",VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                                ",KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_MARCAS";
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
                            "(ID, EVENTO, DT_EVENTO, CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                            "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                            "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                            "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                            ",VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                            ",KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE)" + 
                            "VALUES (ID, EVENTO, DT_EVENTO, CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                            "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                            "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                            "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                            ",VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                            ",KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_MARCAS)";

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