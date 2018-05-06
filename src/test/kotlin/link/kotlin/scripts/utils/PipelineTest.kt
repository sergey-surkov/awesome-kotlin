package link.kotlin.scripts.utils

import kotlinx.coroutines.experimental.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * @author Ruslan Ibragimov
 */
class PipelineTest {

    @Test
    fun `test map sync`() = runBlocking {
        val result = Pipeline { Unit }
            .map { 42 }

        assertEquals(42, result)
    }

    @Test
    fun `test pipe sync`() = runBlocking {
        val result = Pipeline { Unit }
            .pipe(name = "PipeSync") {
                42
            }
            .pipe(name = "PipeSync") {
                it.toString()
            }
            .map { it }

        assertEquals("42", result)
    }

    @Test
    fun `test pipe async`() = runBlocking {
        val task1: Task<Int, Int> = { it: Int -> it * 42 }
        val task2: Task<Int, Int> = { it: Int -> it * 24 }

        val result = Pipeline { 2 }
            .pipeAsync(
                task1 = "Task1" to task1,
                task2 = "Task2" to task2
            )
            .pipe(name = "PipeSync") {
                it.first + it.second
            }
            .map { it }

        assertEquals(132, result)
    }
}
