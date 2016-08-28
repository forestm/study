package com.mengls.study;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalog.*;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by mengls on 2016/8/21.
 */
public class StudySparkSQL {

    public static void main(String[] args) {
        testFunction();
    }

    public static void testFunction() {
        SparkSession session = Spark.getSession();

        String path1 = "D:\\study\\apache-spark\\spark-2.0.0-bin-hadoop2.7\\data\\mllib\\lr-data\\test\\123.csv";
        Dataset<Row> ds1 = session.read().csv(path1);

        String path2 = "D:\\study\\apache-spark\\spark-2.0.0-bin-hadoop2.7\\data\\mllib\\lr-data\\test\\456.csv";
        Dataset<Row> ds2 = session.read().csv(path2);

        testCombine(ds1, ds2);
        ds1.select();


        session.stop();
    }

    public static void testCombine(Dataset<Row> ds1, Dataset<Row> ds2) {
//        assert ds1.cache().count() == ds2.cache().count();
        String leftIndex = "leftIndex";
        String rightIndex = "rightIndex";
        Dataset<Row> left = addIndex(ds1, leftIndex);
        Dataset<Row> right = addIndex(ds2, rightIndex);
        left.join(right, left.col(leftIndex).equalTo(right.col(rightIndex)), "inner").drop(leftIndex, rightIndex).show();
    }

    public static Dataset<Row> addIndex(Dataset<Row> ds, String indexName) {
        SparkSession session = ds.sparkSession();
        JavaRDD<Row> rdd = ds.javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            public Row call(Tuple2<Row, Long> line) throws Exception {
                Object[] row = new Object[line._1().size() + 1];
                line._1().toSeq().copyToArray(row, 0, row.length - 1);
                row[row.length - 1] = line._2();
                return RowFactory.create(row);
            }
        });
        StructType schema = ds.schema().add(indexName, DataTypes.LongType);
        return session.createDataFrame(rdd, schema);
    }


    public static void testModel() {
        String[] featureNames = new String[0];
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureNames)
                .setOutputCol("features");

        GeneralizedLinearRegression regression = new GeneralizedLinearRegression()
                .setFitIntercept(true)
                .setLabelCol("label")
                .setFeaturesCol("features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler, regression});
        Dataset<Row> newDS = null;
        PipelineModel model = pipeline.fit(newDS);
    }
}
