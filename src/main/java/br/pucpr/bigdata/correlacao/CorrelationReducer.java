package br.pucpr.bigdata.correlacao;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * REDUCE: recebe todas as estatisticas parciais de UMA coluna (o shuffle
 * garante que a mesma chave cai no mesmo reducer), soma tudo e so entao
 * aplica a formula de Pearson uma unica vez, com os totais globais.
 *
 * Entrada : (coluna, ["n,sx,sy,sxy,sxx,syy", ...])  (vindas de Mappers/Combiners)
 * Saida   : (coluna, "r=0.xxxxxx  n=517")
 */
public class CorrelationReducer extends Reducer<Text, Text, Text, Text> {

    private final Text saida = new Text();

    @Override
    protected void reduce(Text coluna, Iterable<Text> valores, Context context)
            throws IOException, InterruptedException {

        CorrelationStats total = new CorrelationStats();
        for (Text v : valores) {
            total.add(CorrelationStats.parse(v.toString()));
        }

        double r = total.pearson();
        String rTexto = Double.isNaN(r)
                ? "indefinida"
                : String.format(Locale.ROOT, "%.6f", r);

        saida.set("r=" + rTexto + "  n=" + total.n);
        context.write(coluna, saida);
    }
}
