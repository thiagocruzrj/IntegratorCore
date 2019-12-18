namespace IntegratorCore.Cmd.Domain.Entities
{
    public class SibelClienteOracle
    {
        public SibelClienteOracle(string cdCliente, string tipoCliente, string nmRazaoSocial, string cdGrupoEconomico, string cdTipoDocumento, string tipoAgencia, string flgBv, string flgComissao, string flgAtndMidia, string dtInsert, string dtUpdate, string dsSubtipoCliente, string cdDdd, string nmBairro, string nmCidade, string sgEstado, string nmPais, string cdCep)
        {
            this.CdCliente = cdCliente;
            this.TipoCliente = tipoCliente;
            this.NmRazaoSocial = nmRazaoSocial;
            this.CdGrupoEconomico = cdGrupoEconomico;
            this.CdTipoDocumento = cdTipoDocumento;
            this.TipoAgencia = tipoAgencia;
            this.FlgBv = flgBv;
            this.FlgComissao = flgComissao;
            this.FlgAtndMidia = flgAtndMidia;
            this.DtInsert = dtInsert;
            this.DtUpdate = dtUpdate;
            this.DsSubtipoCliente = dsSubtipoCliente;
            this.CdDdd = cdDdd;
            this.NmBairro = nmBairro;
            this.NmCidade = nmCidade;
            this.SgEstado = sgEstado;
            this.NmPais = nmPais;
            this.CdCep = cdCep;
        }
        public string CdCliente { get; private set; }
        public string TipoCliente { get; private set; }
        public string NmRazaoSocial { get; private set; }
        public string CdGrupoEconomico { get; private set; }
        public string CdTipoDocumento { get; private set; }
        public string TipoAgencia { get; private set; }
        public string FlgBv { get; private set; }
        public string FlgComissao { get; private set; }
        public string FlgAtndMidia { get; private set; }
        public string DtInsert { get; private set; }
        public string DtUpdate { get; private set; }
        public string DsSubtipoCliente { get; private set; }
        public string CdDdd { get; private set; }
        public string NmBairro { get; private set; }
        public string NmCidade { get; private set; }
        public string SgEstado { get; private set; }
        public string NmPais { get; private set; }
        public string CdCep { get; private set; }
    }
}