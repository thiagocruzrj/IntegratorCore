namespace IntegratorCore.Cmd.Domain.Entities
{
    public class SibelSegmtoAbrasceOracle {
        public SibelSegmtoAbrasceOracle (string cdSegmtoAbrasce, string nmSegmtoAbrasce) {
            this.CdSegmtoAbrasce = cdSegmtoAbrasce;
            this.NmSegmtoAbrasce = nmSegmtoAbrasce;

        }
        public string CdSegmtoAbrasce { get; private set; }
        public string NmSegmtoAbrasce { get; private set; }
    }
}