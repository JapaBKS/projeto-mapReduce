package br.pucpr.bigdata.correlacao;

/**
 * Estatisticas suficientes (sufficient statistics) para o calculo de Pearson:
 * n, soma(x), soma(y), soma(xy), soma(x^2), soma(y^2).
 *
 * Todas sao ADITIVAS: somar o resultado parcial de dois blocos de dados
 * da exatamente o mesmo resultado que processar os dois blocos juntos.
 * E isso que permite usar Combiner e varios mappers/slaves sem errar a conta.
 *
 * Formato de serializacao (vai dentro de um Text): "n,sx,sy,sxy,sxx,syy"
 */
final class CorrelationStats {

    long n;
    double sx;
    double sy;
    double sxy;
    double sxx;
    double syy;

    /** Estatisticas de UM unico par (x, y). */
    static CorrelationStats ofPair(double x, double y) {
        CorrelationStats s = new CorrelationStats();
        s.n = 1;
        s.sx = x;
        s.sy = y;
        s.sxy = x * y;
        s.sxx = x * x;
        s.syy = y * y;
        return s;
    }

    /** Soma componente a componente (operacao associativa e comutativa). */
    void add(CorrelationStats o) {
        n += o.n;
        sx += o.sx;
        sy += o.sy;
        sxy += o.sxy;
        sxx += o.sxx;
        syy += o.syy;
    }

    static CorrelationStats parse(String text) {
        String[] p = text.split(",");
        if (p.length != 6) {
            throw new IllegalArgumentException("Valor intermediario invalido: " + text);
        }
        CorrelationStats s = new CorrelationStats();
        s.n = Long.parseLong(p[0]);
        s.sx = Double.parseDouble(p[1]);
        s.sy = Double.parseDouble(p[2]);
        s.sxy = Double.parseDouble(p[3]);
        s.sxx = Double.parseDouble(p[4]);
        s.syy = Double.parseDouble(p[5]);
        return s;
    }

    /** Double.toString garante ida e volta sem perda de precisao. */
    String serialize() {
        return n + "," + sx + "," + sy + "," + sxy + "," + sxx + "," + syy;
    }

    /**
     * r = (n*sxy - sx*sy) / sqrt( (n*sxx - sx^2) * (n*syy - sy^2) )
     * Retorna NaN se nao ha variancia (coluna constante) ou n < 2.
     */
    double pearson() {
        if (n < 2) {
            return Double.NaN;
        }
        double numerador = n * sxy - sx * sy;
        double varX = n * sxx - sx * sx;
        double varY = n * syy - sy * sy;
        if (varX <= 0 || varY <= 0) {
            return Double.NaN;
        }
        return numerador / Math.sqrt(varX * varY);
    }
}
