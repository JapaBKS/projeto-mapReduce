package br.pucpr.bigdata.correlacao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver (main): configura e submete o job. Nao processa dados.
 *
 * Uso: hadoop jar correlacao-mapreduce-1.0.jar <entrada> <saida>
 */
public class CorrelationDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CorrelationDriver <entrada> <saida>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "correlacao-temperatura");

        job.setJarByClass(CorrelationDriver.class);
        job.setMapperClass(CorrelationMapper.class);
        job.setCombinerClass(CorrelationCombiner.class);
        job.setReducerClass(CorrelationReducer.class);

        // Mapper e Combiner emitem (Text, Text); o Reducer tambem.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 1 reducer: so existem 10 chaves, e a saida fica em um unico arquivo.
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        if (ok) {
            imprimirResumo(job);
        }
        System.exit(ok ? 0 : 1);
    }

    /** Mostra no console os contadores de integridade e o efeito do Combiner. */
    private static void imprimirResumo(Job job) throws java.io.IOException {
        System.out.println("---------- RESUMO ----------");
        for (CorrelationMapper.Linhas c : CorrelationMapper.Linhas.values()) {
            System.out.println(c + " = " + job.getCounters().findCounter(c).getValue());
        }
        System.out.println("Map output records     = "
                + job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        System.out.println("Combine input records  = "
                + job.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
        System.out.println("Combine output records = "
                + job.getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue());
        System.out.println("Reduce input records   = "
                + job.getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
        System.out.println("----------------------------");
    }
}
