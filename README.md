# Correlação de Pearson com MapReduce

Correlação entre a temperatura (coluna 9 do `forestfireinput.csv`) e as colunas
numéricas X, Y, FFMC, DMC, DC, ISI, RH, wind, rain e area, usando Hadoop MapReduce.

## Como funciona
- Mapper: valida a linha inteira e emite, por coluna, as somas n, x, y, xy, x2, y2.
- Combiner: soma os parciais no mesmo nó (as somas são aditivas; correlações não são).
- Reducer: soma tudo e aplica a fórmula de Pearson uma única vez.

## Executar
    mvn clean package
    rm -rf saida
    hadoop jar target/correlacao-mapreduce-1.0.jar datasets/forestfireinput.csv saida
    cat saida/part-r-00000
