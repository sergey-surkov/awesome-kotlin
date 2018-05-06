package link.kotlin.scripts.utils

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.withContext
import java.lang.Runtime.getRuntime
import java.lang.System.currentTimeMillis
import kotlin.coroutines.experimental.CoroutineContext

typealias Task<T, R> = suspend (T) -> R
typealias Producer<T> = suspend () -> T

private val pipelineDispatcher by lazy {
    val cpuNum = getRuntime().availableProcessors().coerceAtLeast(1)
    logger<Pipeline<*>>().info("Number of CPUs in PipelineDispatcher: $cpuNum.")
    newFixedThreadPoolContext(cpuNum, "Pipeline Dispatcher")
}

/**
 * Pipeline for building static site.
 *
 * @author Ibragimov Ruslan
 */
class Pipeline<out T>(
    private val producer: Producer<T>
) {
    suspend fun <R> pipe(
        context: CoroutineContext = pipelineDispatcher,
        name: String,
        task: Task<T, R>
    ): Pipeline<R> = withContext(context) {
        Pipeline {
            measure(name) { task(producer()) }
        }
    }

    suspend fun <R1, R2> pipeAsync(
        context: CoroutineContext = pipelineDispatcher,
        task1: Pair<String, Task<T, R1>>,
        task2: Pair<String, Task<T, R2>>
    ): Pipeline<Pair<R1, R2>> {
        val input = producer()
        val d1 = async(context = context) { measure(task1.first) { task1.second(input) } }
        val d2 = async(context = context) { measure(task2.first) { task2.second(input) } }

        return Pipeline { Pair(d1.await(), d2.await()) }
    }

    suspend fun <R1, R2, R3> pipeAsync(
        context: CoroutineContext = pipelineDispatcher,
        task1: Pair<String, Task<T, R1>>,
        task2: Pair<String, Task<T, R2>>,
        task3: Pair<String, Task<T, R3>>
    ): Pipeline<Triple<R1, R2, R3>> {
        val input = producer()
        val d1 = async(context = context) { measure(task1.first) { task1.second(input) } }
        val d2 = async(context = context) { measure(task2.first) { task2.second(input) } }
        val d3 = async(context = context) { measure(task3.first) { task3.second(input) } }
        return Pipeline { Triple(d1.await(), d2.await(), d3.await()) }
    }

    suspend fun <R> map(map: suspend (T) -> R): R {
        return map(producer())
    }

    private suspend fun <R> measure(name: String, producer: Producer<R>): R {
        LOGGER.info("$name started")
        val start = currentTimeMillis()
        val result = producer()
        LOGGER.info("$name completed in ${currentTimeMillis() - start}ms")
        return result
    }

    companion object {
        private val LOGGER = logger<Pipeline<*>>()
    }
}
