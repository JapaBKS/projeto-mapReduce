package br.pucpr.bigdata.correlacao;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MAP: recebe UMA linha do CSV e emite um par (coluna, estatisticas) para cada
 * coluna numerica correlacionada com a temperatura.
 *
 * Entrada : (offset da linha, linha)
 * Saida   : (nome da coluna, "1,x,y,xy,xx,yy")   com y = temperatura
 *
 * INTEGRIDADE: a linha so e aproveitada se estiver COMPLETA e VALIDA
 * (13 campos, todos os campos usados numericos e finitos). Caso contrario
 * e descartada INTEIRA e contada em um Counter. Assim:
 *   - nenhum calculo parcial recebe dado quebrado;
 *   - todas as correlacoes usam exatamente as mesmas linhas (mesmo n);
 *   - x e y sempre vem da MESMA linha (par nunca fica desalinhado).
 */
public class CorrelationMapper extends Mapper<LongWritable, Text, Text, Text> {

    /** Contadores visiveis no log do job. */
    enum Linhas { VALIDAS, EM_BRANCO, COLUNAS_INCORRETAS, VALOR_INVALIDO }

    static final int TOTAL_COLUNAS = 13;
    static final int IDX_TEMP = 8;

    /** Colunas numericas correlacionadas (month=2 e day=3 sao texto: ficam de fora). */
    static final int[] IDX_COLUNAS = {0, 1, 4, 5, 6, 7, 9, 10, 11, 12};
    static final String[] NOMES = {"X", "Y", "FFMC", "DMC", "DC", "ISI", "RH", "wind", "rain", "area"};

    private final Text chave = new Text();
    private final Text valor = new Text();

    @Override
    protected void map(LongWritable offset, Text linha, Context context)
            throws IOException, InterruptedException {

        String texto = linha.toString().trim();
        if (texto.isEmpty()) {
            context.getCounter(Linhas.EM_BRANCO).increment(1);
            return;
        }

        // -1 preserva campos vazios no final ("a,b," vira 3 campos, nao 2)
        String[] campos = texto.split(",", -1);
        if (campos.length != TOTAL_COLUNAS) {
            context.getCounter(Linhas.COLUNAS_INCORRETAS).increment(1);
            return;
        }

        // 1) VALIDA tudo antes de emitir qualquer coisa
        double temp;
        double[] x = new double[IDX_COLUNAS.length];
        try {
            temp = lerNumero(campos[IDX_TEMP]);
            for (int i = 0; i < IDX_COLUNAS.length; i++) {
                x[i] = lerNumero(campos[IDX_COLUNAS[i]]);
            }
        } catch (NumberFormatException e) {
            context.getCounter(Linhas.VALOR_INVALIDO).increment(1);
            return;
        }

        // 2) So agora emite: um par por coluna
        context.getCounter(Linhas.VALIDAS).increment(1);
        for (int i = 0; i < IDX_COLUNAS.length; i++) {
            chave.set(NOMES[i]);
            valor.set(CorrelationStats.ofPair(x[i], temp).serialize());
            context.write(chave, valor);
        }
    }

    private static double lerNumero(String s) {
        double d = Double.parseDouble(s.trim());
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new NumberFormatException("nao finito: " + s);
        }
        return d;
    }
}
