package com.iakidovich;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;

import mpi.MPI;

public class Main {
    
    private static ThreadLocalRandom random = ThreadLocalRandom.current();
    
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        Point[] dataset = generateDataset("s2.txt");
        Point[] centroids = generateInitialCentroids(dataset, 100);
    
        double[] basicAllocation = new double[dataset.length];
        double[] nextAllocation = new double[dataset.length];
        
        Point[] datasetBuffer = new Point[dataset.length / size];
        
        MPI.COMM_WORLD.Scatter(dataset, 0, datasetBuffer.length, MPI.OBJECT,
                datasetBuffer, 0, datasetBuffer.length, MPI.OBJECT, 0);

//        System.out.println("rank <" + rank + "> " + "size <" + size + ">");
//        System.out.println(Arrays.toString(datasetBuffer));
        
        long start = System.nanoTime();
        int maxIterationCount = 400;
        double[] work = {0.0};
        while (maxIterationCount != 0 && work[0] == 0.0) {
            MPI.COMM_WORLD.Bcast(centroids, 0, centroids.length, MPI.OBJECT, 0);
            
            double[] nextAllocationBuffer = new double[dataset.length / size];
            for (int i = 0; i < datasetBuffer.length; i++) {
                List<Double> lengthToCentroids = new ArrayList<>();
                for (int j = 0; j < centroids.length; j++) {
                    lengthToCentroids.add(compute(datasetBuffer[i], centroids[j]));
                }
                int clusterInd = lengthToCentroids.indexOf(Collections.min(lengthToCentroids));
                nextAllocationBuffer[i] = clusterInd;
            }
            
            MPI.COMM_WORLD.Gather(nextAllocationBuffer, 0, nextAllocationBuffer.length, MPI.DOUBLE,
                    nextAllocation, 0, nextAllocationBuffer.length, MPI.DOUBLE, 0);
            
            if (rank == 0) {
            // сравниваем результаты текущей кластеризации с прошлой
                if (Arrays.equals(basicAllocation, nextAllocation)) {
                    work[0] = 1.0;
                } else {
                    basicAllocation = Arrays.copyOf(nextAllocation, nextAllocation.length);
                }
            //пересчитываем центры кластеров
                clearCentroids(centroids);
                computeNewCentroid(centroids, dataset, nextAllocation);
//                System.out.println(Arrays.toString(centroids));
            }
            maxIterationCount--;
            MPI.COMM_WORLD.Barrier();
            MPI.COMM_WORLD.Bcast(work, 0, 1, MPI.DOUBLE, 0);
        }
        long end = System.nanoTime();
        MPI.Finalize();
        System.out.println("current result in millis: " + TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));
    }
    
    private static Point[] generateDataset(String filename) {
        Point[] dataset = new Point[100000];
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String s;
            int i = 0;
            while ((s = reader.readLine()) != null) {
                String[] xY = s.split("\\s+");
                Point e = new Point(Integer.parseInt(xY[1]), Integer.parseInt(xY[2]));
                dataset[i] = e;
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataset;
    }
    
    private static Point[] generateInitialCentroids(Point[] dataset, int clusterSize) {
        Point[] centroids = new Point[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            Point e = dataset[random.nextInt(dataset.length)];
            if (contain(centroids, e)) {
                i--;
                continue;
            }
            centroids[i] = new Point(e.x, e.y);
        }
        return centroids;
    }
    
    private static double compute(Point data, Point centroid) {
        return sqrt(pow(data.x - centroid.x, 2.0) + pow(data.y - centroid.y, 2.0));
    }
    
    private static void computeNewCentroid(Point[] centroids, Point[] dataset, double[] allocation) {
        for (int i = 0; i < allocation.length; i++) {
            Point centroid = centroids[(int) allocation[i]];
            Point point = dataset[i];
            computeCentroidOffset(point, centroid);
        }
    }
    
    private static void computeCentroidOffset(Point data, Point centroid) {
        if (centroid.x == 0.0 & centroid.y == 0.0) {
            centroid.x = data.x;
            centroid.y = data.y;
        } else {
            centroid.x = (data.x + centroid.x) / 2.0;
            centroid.y = (data.y + centroid.y) / 2.0;
        }
    }
    
    private static void clearCentroids(Point[] centroids) {
        for (Point p: centroids) {
            p.x = 0.0;
            p.y = 0.0;
        }
    }
    
    private static void writeResults(List<Point> dataset, List<Point> centroids) {
        HSSFWorkbook sheets = new HSSFWorkbook();
        File file = new File("resultTwo.xls");
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            HSSFSheet sheet = sheets.createSheet("Clusters");
            int rownum = 0;
            for (Point centroid : centroids) {
                writExcel(sheet, rownum, centroid.x, centroid.y);
                rownum++;
            }
            Row row = sheet.createRow(rownum);
            Cell cell = row.createCell(0, CellType.STRING);
            cell.setCellValue("dataset");
            rownum++;
            for (Point data : dataset) {
                writExcel(sheet, rownum, data.x, data.y);
                rownum++;
            }
            sheets.write(outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void writExcel(HSSFSheet sheet, int rownum, double x, double y) {
        Row row = sheet.createRow(rownum);
        Cell cell = row.createCell(0, CellType.NUMERIC);
        cell.setCellValue(x);
        cell = row.createCell(1, CellType.NUMERIC);
        cell.setCellValue(y);
    }
    
    private static boolean contain(Point[] centroids, Point point) {
        for (int i = 0; i < centroids.length; i++) {
            if (centroids[i] == point) {
                return true;
            }
        }
        return false;
    }
}
