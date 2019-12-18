namespace IntegratorCore.Cmd.Domain.Entities
{
    public class SibelCategoriaAbrasceOracle {
        public SibelCategoriaAbrasceOracle (string cdCategoriaAbrasce, string nmCategoriaAbrasce) {

            this.CdCategoriaAbrasce = cdCategoriaAbrasce;
            this.NmCategoriaAbrasce = nmCategoriaAbrasce;
        }
        public string CdCategoriaAbrasce { get; private set; }
        public string NmCategoriaAbrasce { get; private set; }
    }
}