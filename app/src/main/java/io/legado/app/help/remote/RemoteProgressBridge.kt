package io.legado.app.help.remote

import io.legado.app.constant.AppLog
import io.legado.app.data.appDb
import io.legado.app.data.entities.Book
import io.legado.app.data.entities.BookProgress
import io.legado.app.help.AppWebDav
import io.legado.app.help.config.AppConfig
import io.legado.app.help.http.okHttpClient
import io.legado.app.utils.GSON
import io.legado.app.utils.fromJsonObject
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

private enum class RemoteSyncMode(val value: String) {
    LOCAL_ONLY("local"),
    WEBDAV("webdav"),
    QREAD("qread");

    companion object {
        fun fromValue(value: String?): RemoteSyncMode {
            return entries.firstOrNull { it.value.equals(value, true) } ?: WEBDAV
        }
    }
}

/**
 * 阶段1统一远端进度入口：默认沿用 WebDav；QRead 走最小接口骨架。
 */
object RemoteProgressBridge {

    suspend fun uploadBookProgress(
        book: Book,
        toast: Boolean = false,
        onSuccess: (() -> Unit)? = null
    ) {
        when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
            RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(book, toast, onSuccess)
            RemoteSyncMode.QREAD -> uploadBookProgressQRead(BookProgress(book), onSuccess)
        }
    }

    suspend fun uploadBookProgress(
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null
    ) {
        when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
            RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(progress, onSuccess)
            RemoteSyncMode.QREAD -> uploadBookProgressQRead(progress, onSuccess)
        }
    }

    suspend fun getBookProgress(book: Book): BookProgress? {
        return when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> null
            RemoteSyncMode.WEBDAV -> AppWebDav.getBookProgress(book)
            RemoteSyncMode.QREAD -> getBookProgressQRead(book)
        }
    }

    suspend fun downloadAllBookProgress() {
        when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> Unit
            RemoteSyncMode.WEBDAV -> AppWebDav.downloadAllBookProgress()
            RemoteSyncMode.QREAD -> downloadAllBookProgressQRead()
        }
    }

    private suspend fun uploadBookProgressQRead(
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null
    ) {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return
        try {
            val body = GSON.toJson(progress)
                .toRequestBody("application/json; charset=utf-8".toMediaType())
            val request = Request.Builder()
                .url("$baseUrl/api/v1/sync/progress")
                .header("Authorization", "Bearer $token")
                .post(body)
                .build()
            okHttpClient.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    onSuccess?.invoke()
                } else {
                    AppLog.put("QRead上传进度失败, code=${response.code}")
                }
            }
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead上传进度异常\n${e.localizedMessage}", e)
        }
    }

    private suspend fun getBookProgressQRead(book: Book): BookProgress? {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return null
        return try {
            val url = "$baseUrl/api/v1/sync/progress".toHttpUrl()
                .newBuilder()
                .addQueryParameter("name", book.name)
                .addQueryParameter("author", book.author)
                .build()
            val request = Request.Builder()
                .url(url)
                .header("Authorization", "Bearer $token")
                .get()
                .build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("QRead拉取进度失败, code=${response.code}")
                    null
                } else {
                    val body = response.body?.string().orEmpty()
                    GSON.fromJsonObject<BookProgress>(body).getOrNull()
                }
            }
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead拉取进度异常\n${e.localizedMessage}", e)
            null
        }
    }

    private suspend fun downloadAllBookProgressQRead() {
        val books = appDb.bookDao.all
        books.forEach { book ->
            val progress = getBookProgressQRead(book) ?: return@forEach
            if (progress.durChapterIndex > book.durChapterIndex ||
                (progress.durChapterIndex == book.durChapterIndex &&
                    progress.durChapterPos > book.durChapterPos)
            ) {
                book.durChapterIndex = progress.durChapterIndex
                book.durChapterPos = progress.durChapterPos
                book.durChapterTitle = progress.durChapterTitle
                book.durChapterTime = progress.durChapterTime
                appDb.bookDao.update(book)
            }
        }
    }
}
