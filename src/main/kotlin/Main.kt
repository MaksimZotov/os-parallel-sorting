import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.concurrent.thread
import kotlin.random.Random.Default.nextInt
import kotlin.system.measureNanoTime
import kotlin.system.measureTimeMillis

const val DEFAULT_NUM_OF_THREADS_IN_THREAD_POOL = 8

fun main() {
    demonstrateExecutorService()
}

fun sort(array: IntArray, begin: Int, end: Int) {
    //mergeSort(array, begin, end + 1)
    bubbleSort(array, begin, end)
}

fun checkParallelSort() {
    val threadsMin = 1
    val threadsMax = 100
    val iterations = 20
    val arraySize = 1_000
    val maxValue = arraySize / 3
    for (threads in threadsMin..threadsMax) {
        measureTime(threads, iterations, arraySize, maxValue)
    }
}

fun measureTime(threads: Int, iterations: Int, arraySize: Int, maxValue: Int) {
    var time = 0L
    repeat(iterations) {
        val array = createArray(arraySize, maxValue)
        val expected = array.sortedArray()
        time += doWorkThreads(array, threads)
        if (!array.contentEquals(expected)) {
            throw Exception("Array is invalid")
        }
    }
    println("${time / iterations}")
}


fun doWork(array: IntArray, threads: Int, defaultNumOfThreads: Int = threads): Long {
    val executorService = Executors.newFixedThreadPool(defaultNumOfThreads)
    val futures = mutableListOf<Future<*>>()
    val step = array.size / threads + 1
    var intervals = mutableListOf<Pair<Int, Int>>()

    for (begin in array.indices step step) {
        var end = begin + step - 1
        if (end > array.lastIndex) {
            end = array.lastIndex
        }
        intervals.add(begin to end)
    }

    val time = measureTimeMillis {

        for (interval in intervals) {
            futures.add(executorService.submit {
                sort(array, interval.first, interval.second)
            })
        }
        futures.forEach { it.get() }
        futures.clear()

        if (threads == 1) {
            return@measureTimeMillis
        }

        while (intervals.size > 1) {

            val nextIntervals = mutableListOf<Pair<Int, Int>>()

            for (i in intervals.indices step 2) {

                val leftInterval = intervals[i]
                val rightInterval =
                    if (i + 1 < intervals.size) intervals[i + 1] else intervals[i]

                val begin = leftInterval.first
                val end = rightInterval.second

                nextIntervals.add(begin to end)

                if (leftInterval != rightInterval) {
                    futures.add(executorService.submit {
                        merge(
                            array,
                            leftInterval.first,
                            leftInterval.second,
                            rightInterval.first,
                            rightInterval.second
                        )
                    })
                }
            }

            futures.forEach { it.get() }
            futures.clear()
            intervals = nextIntervals
        }
    }
    executorService.shutdown()
    return time
}

fun createArray(arraySize: Int, maxValue: Int): IntArray {
    val arrayInt = IntArray(arraySize)
    for (i in arrayInt.indices) {
        arrayInt[i] = nextInt(maxValue)
    }
    return arrayInt
}

fun merge(array: IntArray, firstBegin: Int, firstEnd: Int, secondBegin: Int, secondEnd: Int) {
    val left = array.copyOfRange(firstBegin, firstEnd + 1)
    val right = array.copyOfRange(secondBegin, secondEnd + 1)
    merge(array, left, right, firstBegin, secondEnd)
}

fun merge(array: IntArray, begin: Int, middle: Int, end: Int) {
    val left = array.copyOfRange(begin, middle)
    val right = array.copyOfRange(middle, end)
    merge(array, left, right, begin, end - 1)
}

fun merge(array: IntArray, left: IntArray, right: IntArray, begin: Int, end: Int) {
    var li = 0
    var ri = 0
    for (i in begin..end) {
        if (li < left.size && (ri == right.size || left[li] <= right[ri])) {
            array[i] = left[li++]
        } else {
            array[i] = right[ri++]
        }
    }
}

fun mergeSort(array: IntArray, begin: Int, end: Int) {
    if (end - begin <= 1) {
        return
    }
    val middle = (begin + end) / 2
    mergeSort(array, begin, middle)
    mergeSort(array, middle, end)
    merge(array, begin, middle, end)
}

fun bubbleSort(array: IntArray, begin: Int, end: Int) {
    for (i in begin..end) {
        for (j in begin until end - i + begin) {
            val temp = array[j + 1]
            if (array[j] > array[j + 1]) {
                array[j + 1] = array[j]
                array[j] = temp
            }
        }
    }
}

fun doWorkThreads(array: IntArray, threads: Int): Long {
    val threadsList = mutableListOf<Thread>()
    val step = array.size / threads + 1
    var intervals = mutableListOf<Pair<Int, Int>>()

    for (begin in array.indices step step) {
        var end = begin + step - 1
        if (end > array.lastIndex) {
            end = array.lastIndex
        }
        intervals.add(begin to end)
    }

    val time = measureTimeMillis {

        for (interval in intervals) {
            threadsList.add(thread {
                sort(array, interval.first, interval.second)
            })
        }
        threadsList.forEach { it.join() }
        threadsList.clear()

        if (threads == 1) {
            return@measureTimeMillis
        }

        while (intervals.size > 1) {

            val nextIntervals = mutableListOf<Pair<Int, Int>>()

            for (i in intervals.indices step 2) {

                val leftInterval = intervals[i]
                val rightInterval =
                    if (i + 1 < intervals.size) intervals[i + 1] else intervals[i]

                val begin = leftInterval.first
                val end = rightInterval.second

                nextIntervals.add(begin to end)

                if (leftInterval != rightInterval) {
                    threadsList.add(thread {
                        merge(
                            array,
                            leftInterval.first,
                            leftInterval.second,
                            rightInterval.first,
                            rightInterval.second
                        )
                    })
                }
            }

            threadsList.forEach { it.join() }
            threadsList.clear()
            intervals = nextIntervals
        }
    }
    return time
}

fun demonstrateExecutorService() {
    val n = 20
    val executorService = Executors.newFixedThreadPool(DEFAULT_NUM_OF_THREADS_IN_THREAD_POOL)
    val futures = mutableListOf<Future<*>>()
    repeat(n) { i ->
        futures.add(executorService.submit {
            println("Start\tIndex: $i\tThread: ${Thread.currentThread().name}")
            var sum = 0
            repeat(1000_000) {
                val a = nextInt(10_000)
                val b = nextInt(10_000) + 1
                val c = a * b + a / b
                sum += c
            }
            println("End\tIndex: $i\tThread: ${Thread.currentThread().name}")
        })
    }
    futures.forEach { it.get() }
    executorService.shutdown()
}