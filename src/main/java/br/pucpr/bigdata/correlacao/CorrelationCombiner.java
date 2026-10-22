package br.pucpr.bigdata.correlacao;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * COMBINER ("mini-reducer" que roda no mesmo no do Mapper, slide 06).
 *
 * Soma as estatisticas parciais de cada coluna ANTES do shuffle, reduzindo
 * o volume que trafega pela rede. Emite o MESMO formato da entrada
 * ("n,sx,sy,sxy,sxx,syy") e NUNCA calcula r: correlacao nao e aditiva
 * (a "media das correlacoes" de cada bloco esta errada), mas as somas sao.
 *
 * Tipos de entrada = tipos de saida = tipos de saida do Mapper (Text, Text),
 * regra obrigatoria para um Combiner.
 */
public class CorrelationCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text saida = new Text();

    @Override
    protected void reduce(Text coluna, Iterable<Text> valores, Context context)
            throws IOException, InterruptedException {

        CorrelationStats total = new CorrelationStats();
        for (Text v : valores) {
            total.add(CorrelationStats.parse(v.toString()));
        }
        saida.set(total.serialize());
        context.write(coluna, saida);
    }
}
