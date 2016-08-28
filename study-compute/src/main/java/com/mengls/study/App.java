package com.mengls.study;

import org.apache.commons.math3.analysis.function.Logit;
import org.apache.commons.math3.fitting.*;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SystemClock;

import java.util.Arrays;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        int count = 1000000;
        Random rnd = new Random();
        WeightedObservedPoints points = new WeightedObservedPoints();
        for (int i = 0; i < count; i++) {
            double x = rnd.nextDouble();
            double y = x * x + 2 * x + 3;
            points.add(x, y);
        }
        int degree = 2;
        PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);

        HarmonicCurveFitter harmonicCurveFitter = HarmonicCurveFitter.create();

        GaussianCurveFitter gaussianCurveFitter = GaussianCurveFitter.create();

        SimpleCurveFitter simpleCurveFitter = SimpleCurveFitter.create(new Logit.Parametric(), null);
        long t1 = System.nanoTime();
        double[] result = fitter.fit(points.toList());
        long t2 = System.nanoTime();
        System.out.println("time: " + (t2 - t1) / 1E09);
        for (int i = 0; i < result.length; i++) {
            System.out.println(result[i]);
        }

        DenseMatrix x = DenseMatrix.zeros(degree, degree + 1);
        Vector y = Vectors.dense(new double[degree]);
        x.transpose().multiply(x);
    }
}
