package dbtask

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.File
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random
import kotlin.system.measureTimeMillis


fun generateFile(file: File, amount: Int = 2000) {
    file.printWriter().use { out ->
        List(amount) { abs(Random.nextInt()).toBigInteger() }.forEach { number ->       // change nextInt to nextLong to see some real slowness
            out.println(number)
        }
    }
}

fun readFile(file: File): List<BigInteger> = file.readLines().map { it.toBigInteger() }

fun primeFactorization(number: BigInteger): List<BigInteger> {
    val result = mutableListOf<BigInteger>()
    if (number < 2.toBigInteger()) return result

    var reminder = number
    var probe = 2.toBigInteger()
    while (probe <= reminder / probe) {
        while (reminder % probe == 0.toBigInteger()) {
            result.add(probe)
            reminder /= probe
        }
        probe++
    }

    if (reminder > 1.toBigInteger())
        result.add(reminder)
    return result
}

fun simplePrimeCounter(numbers: List<BigInteger>): Long {
    var counter = 0L
    numbers.forEach { n ->
        counter += primeFactorization(n).size
    }
    return counter
}

fun threadedPrimeCounter(numbers: List<BigInteger>): Long {
    val threads = mutableListOf<Thread>()
    val counter = AtomicLong(0)
    (0..3).forEach { i ->
        threads.add(thread(start = true, isDaemon = true) {
            var localCounter = 0L
            numbers.subList(i * numbers.size / 4, (i + 1) * numbers.size / 4).forEach { n ->
                localCounter += primeFactorization(n).size.toLong()
            }
            counter.addAndGet(localCounter)
        })
    }
    threads.forEach { it.join() }
    return counter.get()
}

fun rxedPrimeCounter(numbers: List<BigInteger>): Long {
    val counter = AtomicLong(0)
    Observable.create<BigInteger> { emitter ->
        numbers.forEach { emitter.onNext(it) }
        emitter.onComplete()
    }.forEach { n ->
        counter.addAndGet(primeFactorization(n).size.toLong())
    }
//    Observable.create<BigInteger> { emitter ->
//        numbers.forEach { emitter.onNext(it) }
//        emitter.onComplete()
//    }.flatMap { n ->
//        Observable.just(n).subscribeOn(Schedulers.computation()).map {
//            primeFactorization(it).size.toLong()
//        }
//    }.subscribe {
//        counter.addAndGet(it)
//    }
    return counter.get()
}

fun main() {
    val file = File("number.txt")
    generateFile(file)
    println("File generated")

    val numbers = readFile(file)
    println("File read")

//    println(simplePrimeCounter(numbers))
//    println(threadedPrimeCounter(numbers))
//    println(rxedPrimeCounter(numbers))

    println(measureTimeMillis { simplePrimeCounter(numbers) })
    println(measureTimeMillis { threadedPrimeCounter(numbers) })
    println(measureTimeMillis { rxedPrimeCounter(numbers) })
}
