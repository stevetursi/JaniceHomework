package com.tursi.janiceHomework

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.concurrent.thread
import kotlin.system.exitProcess


class SiteSearch constructor(private val searchTerm: String) {


    private val result:MutableList<String> = mutableListOf()
    private val errors:MutableList<Pair<String, java.lang.Exception>> = mutableListOf()

    init {
        val fullQueue: Queue<String> = getUrls()
        concur(fullQueue)
    }

    private fun concur(fullQueue: Queue<String>) {
        val totalWork = fullQueue.size

        val concurrentLimit = 20
        val workers: ConcurrentMap<String, Pair<Long, Thread>> = ConcurrentHashMap()
        while (fullQueue.peek() != null) {

            printStatus(totalWork, workers.size+fullQueue.size)

            while (workers.size < concurrentLimit && fullQueue.peek() != null) {
                val site: String = fullQueue.poll()
                workers[site] = Pair(System.currentTimeMillis(), thread(start = true) {
                    try {
                        val content = getSite(site)
                        removeFromMapAndAttemptToKillThread(workers, site)
                        match(site, content)
                    } catch (e: Exception) {
                        removeFromMapAndAttemptToKillThread(workers, site)
                        logError(site, e)
                    }
                })
            }

            findAndKillOldThreads(workers)

            Thread.sleep(100 ) // this was necessary, otherwise it hung. suspect it's environment (ide) related.
        }

        // block while we wait for work to finish
        while (workers.isNotEmpty()) {
            printStatus(totalWork, workers.size+fullQueue.size)
            findAndKillOldThreads(workers)
            Thread.sleep(100 )
        }

        File("results.txt").writeText(result.joinToString("\n"))
        File("errors.txt").writeText(errors.map { "${it.first}: ${it.second}" }.joinToString ("\n") )


        println("\nFound ${result.size} sites with term [$searchTerm]\nResults written to results.txt")
        if (!errors.isEmpty()) {
            println("Some sites couldn't be searched. A list is in errors.txt")
        }

        // sometimes there are zombie requests in orphaned threads still running despite interrupt()
        exitProcess(0)
    }

    private fun printStatus(total: Int, done: Int) {
        val percentRemaining = ((done*100)/total)
        val percentDone = 100 - percentRemaining

        print("\r progress: ${"*".repeat(percentDone)}${"-".repeat(percentRemaining)}")

    }

    private fun findAndKillOldThreads(workers: ConcurrentMap<String, Pair<Long, Thread>>) {
        // the retry/redirect logic in getSite() could potentially lead to infinite recursive loops. This
        // will catch and kill them after n seconds.
        val now = System.currentTimeMillis()
        workers.keys.filter{x -> now-workers[x]!!.first > 15000}.forEach { run {
            logError(it, Exception("Zombie thread timeout"))
            removeFromMapAndAttemptToKillThread(workers, it)
        } }
    }

    private fun removeFromMapAndAttemptToKillThread(workers: ConcurrentMap<String, Pair<Long, Thread>>, key: String) {
        if (workers.containsKey(key)) {
            val thread = workers[key]!!.second
            workers.remove(key)

            if (thread.isAlive) {
                thread.interrupt()
            }

        }

    }

    private fun match(site: String, content: String) {
        if (content.contains(searchTerm)) {
            result.add(site)
        }
    }

    private fun logError(site: String, e: Exception) {
        errors.add(Pair(site, e))
    }

    private fun getSite(site:String, secure: Boolean=true): String {
        try {
            with(sendGet(site, secure)) {
                connectTimeout = 5000
                when {
                    responseCode >= 500 -> throw Exception("Server Error")
                    responseCode >= 400 -> throw Exception("400 Error")
                }

                if (responseCode >= 300) {
                    // redirect
                    val newUrl = getHeaderField("Location")
                    return getSite(newUrl, newUrl.startsWith("https"))
                }
                return inputStream.bufferedReader().readText()

            }
        } catch (e: Exception) {
            if (secure) {
                // let's retry it non-secure first
                return getSite(site, false)
            } else {
                // in this case it would have failed in both secure and non, let's ditch it.
                throw e
            }
        }
    }

    private fun getUrls(): Queue<String> {
        val source = "s3.amazonaws.com/fieldlens-public/urls.txt"

        val rawLines = mutableListOf<String>()

        with ( sendGet(source, true) ) {
            BufferedReader(InputStreamReader(inputStream)).use {
                it.forEachLine { line -> rawLines.add(line) }
            }
        }

        val urls = rawLines
            .filterIndexed{ i, _ ->
                i !=0 // remove header
                // && i < 40 // some of my work was done on a slow connection.
            }
            .map { it
                .split(",")[1]
                .removeSurrounding("\"")
            }

        return PriorityQueue(urls)
    }


    private fun sendGet(url: String, secure: Boolean=true): HttpURLConnection {

        val fullUrl =
            if (url.startsWith("http")) {
                // it would have been added in a redirect
                url
            } else {
                url.prependIndent(if (secure) "https://" else "http://")
            }

        val obj = URL( fullUrl )

        return obj.openConnection() as HttpURLConnection
    }

}
