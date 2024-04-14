import org.apache.spark._

object Hello {
  def distance(a: Station, b: Station): Double = {
    // https://en.wikipedia.org/wiki/Haversine_formula
    val rad = 6372
    val lat1 = a.lat * math.Pi / 180
    val lat2 = b.lat * math.Pi / 180
    val long1 = a.long * math.Pi / 180
    val long2 = b.long * math.Pi / 180

    val cl1 = math.cos(lat1)
    val cl2 = math.cos(lat2)
    val sl1 = math.sin(lat1)
    val sl2 = math.sin(lat2)
    val delta = long2 - long1
    val cdelta = math.cos(delta)
    val sdelta = math.sin(delta)

    val y = math.sqrt(math.pow(cl2 * sdelta, 2) + math.pow(cl1 * sl2 - sl1 * cl2 * cdelta, 2))
    val x = sl1 * sl2 + cl1 * cl2 * cdelta
    val ad = math.atan2(y, x)
    val dist = ad * rad
    dist
  }

  def main(args: Array[String]): Unit = {
    // Инициализация Spark контекста
    val config = new SparkConf()
      .setAppName("Lab1")
      .setMaster("local[*]")
    val sparkContext = new SparkContext(config)

    val tripData = sparkContext.textFile("data/trips.csv")
    val tripsFromFile = tripData
      .map(row => row.split(",", -1))
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter) // Удаление первой строки csv файла (заголовка) из методички
    val stationData = sparkContext.textFile("data/stations.csv")
    val stationsFromFile = stationData
      .map(row => row.split(",", -1))
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)

    val trips = tripsFromFile.mapPartitions(rows => {rows.map(row => Trip.buildFromRow(row))})
    val stations = stationsFromFile.map(row => Station.buildFromRow(row))

    // 1. Найти велосипед с максимальным временем пробега.
    val bikeWithLongestDuration = trips
      .keyBy(trip => trip.bikeId)
      .mapValues(trip => trip.duration)
      .reduceByKey(_ + _)
      .sortBy(trip => trip._2, ascending = false)
      .first()
    println("Task1: BikeId %d and bikeWithLongestDuration %d".format(bikeWithLongestDuration._1, bikeWithLongestDuration._2))

    // 2.	Найти наибольшее геодезическое расстояние между станциями
    val longestDistance = stations
      .cartesian(stations)
      .map(pair => (pair._1.name, pair._2.name, distance(pair._1, pair._2)))
      .sortBy(list => list._3, ascending = false)
      .first()
    println("Task2: longestDistance %s".format(longestDistance))

    // 3.	Найти путь велосипеда с максимальным временем пробега через станции
    val paths = trips
      .filter(trip => trip.bikeId.equals(bikeWithLongestDuration._1))
      .sortBy(trip => trip.startDate)
    paths.foreach(path => println("Task3: from station %s to station %s".format(path.startStation, path.endStation)))

    // 4.	Найти количество велосипедов в системе
    val bikesCount = trips
      .map(trip => trip.bikeId)
      .distinct()
      .count()
    println("Task4: bikesCount %s".format(bikesCount))

    // 5.	Найти пользователей потративших на поездки более 3 часов
    val subscribers = trips.keyBy(trip => trip.zipCode)
      .mapValues(trip => trip.duration)
      .reduceByKey(_ + _)
      .filter(trip => trip._2 > 3 * 60 * 60)
      .take(10)
    println("Task5: subscribers %s".format(subscribers.mkString("Array(", ", ", ")")))

    sparkContext.stop()
  }
}