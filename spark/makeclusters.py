#!/usr/bin/env python
# -*- coding: utf-8 -*-

# programa que construye un RDD con los ficheros de usuarios y ejecuta el algoritmo KMEANS de clusterizacion
# las salidas son la asignacion de cluster por usuario y los k centroides

# incializacion spark context
import re
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import csv

with open ("/root/spark-ec2/cluster-url", "r") as myfile:
        cluster_url=myfile.readlines()
        myfile.close()

print "Starting Spark Context\n"
# TO DO: leer automaticamente la direccion del cluster del fichero /root/spark-ec2/cluster-url
conf = SparkConf().setMaster(cluster_url[0].rstrip('\n')).setAppName("KlusterMaker")
sc = SparkContext(conf = conf)

# lectura ficheros txt

# ficheros en local.
# 
#pairedData = sc.wholeTextFiles('file:///home/paulino.tardaguila/BitBucket/endesa/tratamientodatos/R/user_rowfiles/255*.txt').map(lambda (x,y): (long(re.findall('\d+',x)[0]),map(float, y.split(" "))))


print "Retrieving info from S3 bucket\n"
#ficheros en bucket S3
pairedData = sc.wholeTextFiles('s3n://mdcendesa/user_rowfiles/*.txt').map(lambda (x,y): (long(re.findall('\d+',x)[1]),map(float, y.split(" "))))

pairedData.persist()

matriz = pairedData.values()

print "Performing KMeans Clustering algorithm\n"
# kmeans en matriz de valores del RDD
clusters = KMeans.train(matriz, 20, maxIterations=10, runs=10, initializationMode="random")

centroides = clusters.centers

asignacion = clusters.predict(matriz)

# Suma de cuadrados intra cluster (total)

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSE = matriz.map(lambda point: error(point)).reduce(lambda x, y:x+y)

# TODO: suma de cuadrados total y suma de cuadrados por cluster

matriz_asig = matriz.map(lambda point: (clusters.predict(point), point))

def errorKnownCluster(cl, point):
	return sqrt(sum([x**2 for x in (point - cluster.centers[cl])]))

# Suma de cuadrados de cada cluster...usar la funcion error es lento, porque ha de recalcular la asignación
# de cada observacion.. Investigar la forma de meter el valor de la llave en la llamada a la función del map
CSSE = matriz_asig.mapValues(lambda point: error(point)).reduceByKey(lambda x,y:(x+y))

print "Writing output files\n"
# Guardar salidas
# asignacion
asigRDD=pairedData.keys().map(lambda k:k).zip(asignacion)

def toCSVLine(data):
  return ';'.join(str(d) for d in data)


lines = asigRDD.map(toCSVLine)
# así lo guardamos en formato hadoop, pero no nos hace falta, porque el tamaño no es excesivo
#lines.saveAsTextFile('/home/paulino/BitBucket/endesa/tratamientodatos/spark/output/asignacion_kmeans.csv')

# además, no funciona con buckets en eu-central-1
# https://issues.apache.org/jira/browse/SPARK-11353
#lines.saveAsTextFile('s3n://mdcendesa/output/asignacion_kmeans.csv')

#lo guardo en local, al máximo tendrá 100.000 lineas
# to do: ruta relativa al directorio de trabajo
#with file('/home/paulino/BitBucket/endesa/tratamientodatos/spark/output/asignacion_kmeans.csv', 'w') as outfile:
with file('output/asignacion_kmeans.csv', 'w') as outfile:
	for item in lines.collect():
  		outfile.write("%s\n" % item)
	outfile.close()


item_length = len(centroides[0])

#with file('/home/paulino/BitBucket/endesa/tratamientodatos/spark/output/centroides_kmeans.csv','w') as outfile:
with file('output/centroides_kmeans.csv','w') as outfile:
    file_writer = csv.writer(outfile)
    for i in range(item_length):
        file_writer.writerow([x[i] for x in centroides])
    outfile.close()

with file('output/sumsquares.csv','w') as outfile:
    outfile.write("WSSE: %d\n" % WSSE)
    outfile.write("CSSE: %d\n" % CSSE)
    outfile.close()


print "Closing Spark environment\n"
sc.stop()
