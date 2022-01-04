import kotlin.concurrent.thread
import kotlin.random.Random.Default.nextInt
import kotlin.system.measureNanoTime

fun main() {
    checkParallelSort()
}

fun sort(array: IntArray, begin: Int, end: Int) {
    mergeSort(array, begin, end + 1)
    //bubbleSort(array, begin, end)
}

fun checkParallelSort() {
    val threadsMin = 1
    val threadsMax = 100
    val iterations = 30
    val arraySize = 15_000
    val maxValue = 100
    for (threads in threadsMin..threadsMax) {
        measureTime(threads, iterations, arraySize, maxValue)
    }
}

fun measureTime(threads: Int, iterations: Int, arraySize: Int, maxValue: Int) {
    var time = 0L
    repeat(iterations) {
        val array = createArray(arraySize, maxValue)
        val expected = array.sortedArray()
        time += doWork(array, threads)
        if (!array.contentEquals(expected)) {
            throw Exception("Array is invalid")
        }
    }
    println("[$threads; ${time / iterations}]")
}


fun doWork(array: IntArray, threads: Int): Long {
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

    val time = measureNanoTime {

        for (interval in intervals) {
            threadsList.add(thread {
                sort(array, interval.first, interval.second)
            })
        }
        wait(threadsList)
        threadsList.clear()

        if (threads == 1) {
            return@measureNanoTime
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

            wait(threadsList)
            threadsList.clear()
            intervals = nextIntervals
        }
    }
    return time
}

fun createArray(arraySize: Int, maxValue: Int): IntArray {
    val arrayInt = IntArray(arraySize)
    for (i in arrayInt.indices) {
        arrayInt[i] = nextInt(maxValue)
    }
    return arrayInt
}

fun wait(threads: MutableList<Thread>) {
    for (thread in threads) {
        thread.join()
    }
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