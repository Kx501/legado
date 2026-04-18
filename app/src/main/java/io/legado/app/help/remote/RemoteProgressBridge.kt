package io.legado.app.help.remote

import io.legado.app.constant.AppLog
import io.legado.app.constant.BookType
import io.legado.app.data.appDb
import io.legado.app.data.entities.Book
import io.legado.app.data.entities.BookSource
import io.legado.app.data.entities.BookProgress
import io.legado.app.data.entities.RssSource
import io.legado.app.help.AppWebDav
import io.legado.app.help.coroutine.Coroutine
import io.legado.app.help.config.AppConfig
import io.legado.app.help.http.okHttpClient
import io.legado.app.utils.GSON
import io.legado.app.utils.fromJsonArray
import io.legado.app.utils.fromJsonObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.FormBody
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject

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
 * 统一远端进度入口：WebDav 与 QRead 共用此桥接层。
 */
object RemoteProgressBridge {
    private const val QREAD_CLIENT_VERSION = "3.1.0"
    private val QREAD_API_VERSIONS = intArrayOf(5, 1)
    private const val QREAD_PATH_GET_BOOKSHELF = "/api/%d/getBookshelf"
    private const val QREAD_PATH_SAVE_BOOK_PROGRESS = "/api/%d/saveBookProgress"
    private const val QREAD_PATH_SAVE_BOOKS = "/api/%d/saveBooks"
    /** BookController：按 bookUrl 列表批量删除云端书架 */
    private const val QREAD_PATH_DELETE_BOOKS = "/api/%d/deleteBooks"
    /** ReadController：一次返回书源摘要列表（与轻阅读一致），再 POST getbookSourcejson 拉全文 */
    private const val QREAD_PATH_GET_BOOK_SOURCES_LIST = "/api/%d/getBookSources"
    private const val QREAD_PATH_GET_BOOK_SOURCE_JSON = "/api/%d/getbookSourcejson"
    /** RssController：一次返回订阅源摘要，再逐条 getRssSources 取 json */
    private const val QREAD_PATH_GET_RSS_SOURCES_BULK = "/api/%d/getRssSourcess"
    private const val QREAD_PATH_GET_RSS_SOURCE = "/api/%d/getRssSources"
    private const val PARAM_ACCESS_TOKEN = "accessToken"
    private const val PARAM_VERSION = "version"
    private const val PARAM_NAME = "name"
    private const val PARAM_URL = "url"
    private const val PARAM_POS = "pos"
    private const val PARAM_TITLE = "title"
    private const val PARAM_INDEX = "index"
    private const val PARAM_IS_NEW = "isnew"
    private const val QREAD_IS_SUCCESS = "isSuccess"
    private const val QREAD_ERROR_MSG = "errorMsg"
    private const val QREAD_DATA = "data"
    private const val QREAD_BOOK_URL = "bookUrl"
    private const val QREAD_BOOK_SOURCE_URL = "bookSourceUrl"
    private const val QREAD_SOURCE_URL = "sourceUrl"
    private const val QREAD_SOURCE_GROUP = "sourceGroup"
    private const val QREAD_ENABLED = "enabled"
    private const val QREAD_JSON = "json"
    private const val QREAD_NAME = "name"
    private const val QREAD_AUTHOR = "author"
    private const val QREAD_DUR_CHAPTER_INDEX = "durChapterIndex"
    private const val QREAD_DUR_CHAPTER_POS = "durChapterPos"
    private const val QREAD_DUR_CHAPTER_TIME = "durChapterTime"
    private const val QREAD_DUR_CHAPTER_TITLE = "durChapterTitle"
    private const val QREAD_CONTENT_TYPE_JSON = "application/json; charset=utf-8"
    private const val LOG_QREAD_PREFIX = "QRead请求失败"
    private const val MODE_QREAD = "qread"

    suspend fun uploadBookProgress(
        book: Book,
        toast: Boolean = false,
        onSuccess: (() -> Unit)? = null
    ) {
        withContext(Dispatchers.IO) {
            when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
                RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
                RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(book, toast, onSuccess)
                RemoteSyncMode.QREAD -> uploadBookProgressQRead(book, BookProgress(book), onSuccess)
            }
        }
    }

    suspend fun uploadBookProgress(
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null
    ) {
        withContext(Dispatchers.IO) {
            when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
                RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
                RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(progress, onSuccess)
                RemoteSyncMode.QREAD -> uploadBookProgressQRead(progress, onSuccess)
            }
        }
    }

    suspend fun getBookProgress(book: Book): BookProgress? {
        return withContext(Dispatchers.IO) {
            when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
                RemoteSyncMode.LOCAL_ONLY -> null
                RemoteSyncMode.WEBDAV -> AppWebDav.getBookProgress(book)
                RemoteSyncMode.QREAD -> getBookProgressQRead(book)
            }
        }
    }

    suspend fun downloadAllBookProgress() {
        withContext(Dispatchers.IO) {
            when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
                RemoteSyncMode.LOCAL_ONLY -> Unit
                RemoteSyncMode.WEBDAV -> AppWebDav.downloadAllBookProgress()
                RemoteSyncMode.QREAD -> downloadAllBookProgressQRead()
            }
        }
    }

    /**
     * 将本地网络书逐本同步到 QRead 服务端书架（[addBookToQReadShelf] / saveBooks），
     * 服务端已有同 bookUrl 或同名作者的记录会跳过。
     * 仅建议在**备份恢复**等批量 `insert`、未走 [Book.save] 的场景调用；日常加删靠入架/删架钩子即可。
     */
    suspend fun uploadLocalShelfToQRead(): Int {
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) return 0
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return withContext(Dispatchers.IO) {
            try {
                val remote = fetchBookProgressListQRead(baseUrl, token)
                val remoteUrls =
                    remote.mapNotNull { it.bookUrl?.trim()?.takeIf { u -> u.isNotBlank() } }.toMutableSet()
                val remoteNameAuthor = remote.map { it.name to it.author }.toMutableSet()
                var added = 0
                for (book in appDb.bookDao.webBooks) {
                    currentCoroutineContext().ensureActive()
                    if (book.bookUrl.isBlank() || book.name.isBlank()) continue
                    if (book.bookUrl in remoteUrls || (book.name to book.author) in remoteNameAuthor) continue
                    if (addBookToQReadShelf(baseUrl, token, book)) {
                        added++
                        remoteUrls.add(book.bookUrl)
                        remoteNameAuthor.add(book.name to book.author)
                    }
                }
                added
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead上传书架异常\n${e.localizedMessage}", e)
                0
            }
        }
    }

    /**
     * 新加入书架的网络书：在 QRead 模式下异步尝试 [ensureBookOnQReadShelf]（不阻塞调用线程）。
     */
    fun scheduleSyncBookToQReadShelfIfEnabled(book: Book) {
        Coroutine.async {
            syncBookToQReadShelfIfEnabled(book)
            Unit
        }
    }

    suspend fun syncBookToQReadShelfIfEnabled(book: Book) {
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) return
        if (book.bookUrl.isBlank() || book.name.isBlank()) return
        if ((book.type and BookType.local) != 0) return
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return
        withContext(Dispatchers.IO) {
            try {
                ensureBookOnQReadShelf(baseUrl, token, book)
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead单本入架异常 bookUrl=${book.bookUrl}\n${e.localizedMessage}", e)
            }
        }
    }

    fun scheduleDeleteBooksFromQReadShelfIfEnabled(bookUrls: List<String>) {
        val urls = bookUrls.map { it.trim() }.filter { it.isNotBlank() }.distinct()
        if (urls.isEmpty()) return
        Coroutine.async {
            deleteBooksFromQReadShelfIfEnabled(urls)
            Unit
        }
    }

    fun scheduleDeleteBookFromQReadShelfIfEnabled(book: Book) {
        if ((book.type and BookType.local) != 0) return
        if (book.bookUrl.isBlank()) return
        scheduleDeleteBooksFromQReadShelfIfEnabled(listOf(book.bookUrl))
    }

    suspend fun deleteBooksFromQReadShelfIfEnabled(bookUrls: List<String>) {
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) return
        val urls = bookUrls.map { it.trim() }.filter { it.isNotBlank() }.distinct()
        if (urls.isEmpty()) return
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return
        withContext(Dispatchers.IO) {
            try {
                val body = JSONArray(urls).toString().toRequestBody(QREAD_CONTENT_TYPE_JSON.toMediaType())
                for (version in QREAD_API_VERSIONS) {
                    val requestUrl = "$baseUrl${QREAD_PATH_DELETE_BOOKS.format(version)}".toHttpUrl()
                        .newBuilder()
                        .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                        .build()
                    val request = Request.Builder().url(requestUrl).post(body).build()
                    val ok = okHttpClient.newCall(request).execute().use { response ->
                        if (!response.isSuccessful) {
                            AppLog.put("$LOG_QREAD_PREFIX deleteBooks HTTP ${response.code}, v=$version")
                            return@use false
                        }
                        val payload = response.body.string()
                        val root = runCatching { JSONObject(payload) }.getOrNull() ?: return@use false
                        if (root.optBoolean(QREAD_IS_SUCCESS, false)) {
                            return@use true
                        }
                        AppLog.put(
                            "$LOG_QREAD_PREFIX deleteBooks isSuccess=false, v=$version, error=${
                                root.optString(QREAD_ERROR_MSG)
                            }"
                        )
                        false
                    }
                    if (ok) break
                }
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead云端删书异常\n${e.localizedMessage}", e)
            }
        }
    }

    suspend fun syncBookSourcesFromQRead(accessToken: String? = null): Int {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = accessToken?.trim()?.takeIf { it.isNotBlank() } ?: AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return withContext(Dispatchers.IO) {
            try {
                val sourceJson = fetchBookSourceJsonQRead(baseUrl, token)
                if (sourceJson.isBlank()) return@withContext 0
                val sources = GSON.fromJsonArray<BookSource>(sourceJson).getOrNull() ?: emptyList()
                if (sources.isEmpty()) return@withContext 0
                appDb.bookSourceDao.insert(*sources.toTypedArray())
                sources.size
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead同步书源异常\n${e.localizedMessage}", e)
                0
            }
        }
    }

    suspend fun syncRssSourcesFromQRead(accessToken: String? = null): Int {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = accessToken?.trim()?.takeIf { it.isNotBlank() } ?: AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return withContext(Dispatchers.IO) {
            try {
                val summaries = fetchRssSourceSummariesQRead(baseUrl, token)
                if (summaries.isEmpty()) return@withContext 0
                val rssSources = mutableListOf<RssSource>()
                summaries.forEach { summary ->
                    val source = fetchRssSourceDetailQRead(baseUrl, token, summary.sourceUrl) ?: return@forEach
                    source.enabled = summary.enabled
                    source.sourceGroup = summary.sourceGroup
                    rssSources.add(source)
                }
                if (rssSources.isEmpty()) return@withContext 0
                appDb.rssSourceDao.insert(*rssSources.toTypedArray())
                rssSources.size
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead同步订阅源异常\n${e.localizedMessage}", e)
                0
            }
        }
    }

    suspend fun syncQReadSourcesIfEnabled(): Pair<Int, Int> {
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) {
            return 0 to 0
        }
        val bookCount = syncBookSourcesFromQRead()
        val rssCount = syncRssSourcesFromQRead()
        return bookCount to rssCount
    }

    private suspend fun uploadBookProgressQRead(
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null
    ) {
        val localBook = appDb.bookDao.getBook(progress.name, progress.author)
        uploadBookProgressQRead(localBook, progress, onSuccess)
    }

    private suspend fun uploadBookProgressQRead(
        book: Book?,
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null
    ) {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        val bookUrl = book?.bookUrl.orEmpty()
        if (baseUrl.isBlank() || token.isBlank() || bookUrl.isBlank()) return
        try {
            if (book != null && !ensureBookOnQReadShelf(baseUrl, token, book)) {
                AppLog.put("QRead书架缺书且自动入架失败, bookUrl=$bookUrl")
                return
            }
            var uploadOk = false
            QREAD_API_VERSIONS.forEach { version ->
                val form = FormBody.Builder()
                    .add(PARAM_ACCESS_TOKEN, token)
                    .add(PARAM_URL, bookUrl)
                    .add(PARAM_POS, progress.durChapterPos.toDouble().toString())
                    .add(PARAM_TITLE, progress.durChapterTitle.orEmpty())
                    .add(PARAM_INDEX, progress.durChapterIndex.toString())
                    .add(PARAM_IS_NEW, "0")
                    .build()
                val request = Request.Builder()
                    .url("$baseUrl${QREAD_PATH_SAVE_BOOK_PROGRESS.format(version)}")
                    .post(form)
                    .build()
                okHttpClient.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) {
                        AppLog.put("$LOG_QREAD_PREFIX saveBookProgress HTTP ${response.code}, v=$version")
                        return@use
                    }
                    val payload = response.body.string()
                    val json = runCatching { JSONObject(payload) }.getOrNull() ?: return@use
                    if (json.optBoolean(QREAD_IS_SUCCESS, false)) {
                        uploadOk = true
                    } else {
                        AppLog.put(
                            "$LOG_QREAD_PREFIX saveBookProgress isSuccess=false, v=$version, error=${
                                json.optString(QREAD_ERROR_MSG)
                            }"
                        )
                    }
                }
                if (uploadOk) return@forEach
            }
            if (uploadOk) {
                onSuccess?.invoke()
            } else {
                AppLog.put("QRead上传进度失败, bookUrl=$bookUrl")
            }
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead上传进度异常\n${e.localizedMessage}", e)
        }
    }

    private suspend fun ensureBookOnQReadShelf(baseUrl: String, token: String, book: Book): Boolean {
        val existed = fetchBookProgressListQRead(baseUrl, token, book.name).any {
            (!it.bookUrl.isNullOrBlank() && it.bookUrl == book.bookUrl) ||
                (it.name == book.name && it.author == book.author)
        }
        if (existed) return true
        return addBookToQReadShelf(baseUrl, token, book)
    }

    private suspend fun addBookToQReadShelf(baseUrl: String, token: String, book: Book): Boolean {
        val item = JSONObject().apply {
            put(QREAD_BOOK_URL, book.bookUrl)
            put("tocUrl", book.tocUrl)
            put("origin", book.origin)
            put("originName", book.originName)
            put("name", book.name)
            put("author", book.author)
            put("kind", book.kind ?: "")
            put("coverUrl", book.coverUrl ?: "")
            put("intro", book.intro ?: "")
            put("type", book.type)
            put(QREAD_DUR_CHAPTER_TITLE, book.durChapterTitle ?: "")
            put(QREAD_DUR_CHAPTER_INDEX, book.durChapterIndex)
            put(QREAD_DUR_CHAPTER_POS, book.durChapterPos.toDouble())
            put(QREAD_DUR_CHAPTER_TIME, book.durChapterTime)
            put("wordCount", book.wordCount ?: "")
            put("latestChapterTitle", book.latestChapterTitle ?: "")
            put("latestChapterTime", book.latestChapterTime)
            put("lastCheckTime", book.lastCheckTime)
            put("lastCheckCount", book.lastCheckCount)
            put("totalChapterNum", book.totalChapterNum)
        }
        val content = JSONArray().put(item).toString()
        QREAD_API_VERSIONS.forEach { version ->
            val requestUrl = "$baseUrl${QREAD_PATH_SAVE_BOOKS.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .build()
            val request = Request.Builder()
                .url(requestUrl)
                .post(content.toRequestBody(QREAD_CONTENT_TYPE_JSON.toMediaType()))
                .build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX saveBooks HTTP ${response.code}, v=$version")
                    return@use
                }
                val body = response.body.string()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use
                if (root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    return true
                } else {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX saveBooks isSuccess=false, v=$version, error=${
                            root.optString(QREAD_ERROR_MSG)
                        }"
                    )
                }
            }
        }
        return false
    }

    private suspend fun fetchBookSourceJsonQRead(baseUrl: String, token: String): String {
        QREAD_API_VERSIONS.forEach { version ->
            val ids = fetchBookSourceIdsFromReadApi(baseUrl, token, version)
            if (ids.isEmpty()) return@forEach
            val payload = fetchBookSourceJsonPayloadQRead(baseUrl, token, version, ids)
            if (!payload.isNullOrBlank()) return payload
        }
        return ""
    }

    /**
     * 使用 [ReadController.getBookSources] 拉取书源 URL 列表（与轻阅读一致），
     * 避免依赖 [SourceController.getBookSourcesNew] 等分页缓存接口（服务端曾返回 false）。
     */
    private fun fetchBookSourceIdsFromReadApi(
        baseUrl: String,
        token: String,
        version: Int
    ): List<String> {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCES_LIST.format(version)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .addQueryParameter("isall", "1")
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        return okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getBookSources HTTP ${response.code}, v=$version")
                return@use emptyList()
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList()
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getBookSources isSuccess=false, v=$version, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return@use emptyList()
            }
            val data = root.optJSONArray(QREAD_DATA) ?: return@use emptyList()
            val ids = linkedSetOf<String>()
            for (i in 0 until data.length()) {
                val item = data.optJSONObject(i) ?: continue
                val id = item.optString(QREAD_BOOK_SOURCE_URL).orEmpty()
                if (id.isNotBlank()) ids.add(id)
            }
            ids.toList()
        }
    }

    private suspend fun fetchBookSourceJsonPayloadQRead(
        baseUrl: String,
        token: String,
        version: Int,
        ids: List<String>
    ): String? {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCE_JSON.format(version)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder()
            .url(requestUrl)
            .post(JSONArray(ids).toString().toRequestBody(QREAD_CONTENT_TYPE_JSON.toMediaType()))
            .build()
        okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getbookSourcejson HTTP ${response.code}, v=$version")
                return null
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return null
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getbookSourcejson isSuccess=false, v=$version, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return null
            }
            return root.optString(QREAD_DATA)
        }
    }

    /**
     * 使用 [RssController.getRssSourcess] 一次拉全量摘要（与轻阅读一致），
     * 避免 [getRssSourcessNew] 分页缓存未实现的问题。
     */
    private fun fetchRssSourceSummariesQRead(
        baseUrl: String,
        token: String
    ): List<QReadRssSourceSummary> {
        QREAD_API_VERSIONS.forEach { version ->
            val requestUrl = "$baseUrl${QREAD_PATH_GET_RSS_SOURCES_BULK.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .build()
            val request = Request.Builder().url(requestUrl).get().build()
            val summaries = okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX getRssSourcess HTTP ${response.code}, v=$version")
                    return@use emptyList()
                }
                val body = response.body.string()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList()
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX getRssSourcess isSuccess=false, v=$version, error=${
                            root.optString(QREAD_ERROR_MSG)
                        }"
                    )
                    return@use emptyList()
                }
                val data = root.optJSONObject(QREAD_DATA) ?: return@use emptyList()
                val sources = data.optJSONArray("sources") ?: return@use emptyList()
                val list = mutableListOf<QReadRssSourceSummary>()
                for (i in 0 until sources.length()) {
                    val item = sources.optJSONObject(i) ?: continue
                    val sourceUrl = item.optString(QREAD_SOURCE_URL).orEmpty()
                    if (sourceUrl.isBlank()) continue
                    list.add(
                        QReadRssSourceSummary(
                            sourceUrl = sourceUrl,
                            sourceGroup = item.optString(QREAD_SOURCE_GROUP).ifBlank { null },
                            enabled = item.optBoolean(QREAD_ENABLED, true)
                        )
                    )
                }
                list
            }
            if (summaries.isNotEmpty()) return summaries
        }
        return emptyList()
    }

    private suspend fun fetchRssSourceDetailQRead(
        baseUrl: String,
        token: String,
        sourceUrl: String
    ): RssSource? {
        QREAD_API_VERSIONS.forEach { version ->
            val requestUrl = "$baseUrl${QREAD_PATH_GET_RSS_SOURCE.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .addQueryParameter("id", sourceUrl)
                .build()
            val request = Request.Builder().url(requestUrl).get().build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX getRssSources HTTP ${response.code}, v=$version")
                    return@use
                }
                val body = response.body.string()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX getRssSources isSuccess=false, v=$version, error=${
                            root.optString(QREAD_ERROR_MSG)
                        }"
                    )
                    return@use
                }
                val data = root.optJSONObject(QREAD_DATA) ?: return@use
                val sourceJson = data.optString(QREAD_JSON).orEmpty()
                if (sourceJson.isBlank()) return@use
                val source = GSON.fromJsonObject<RssSource>(sourceJson).getOrNull() ?: return@use
                return source
            }
        }
        return null
    }

    private suspend fun getBookProgressQRead(book: Book): BookProgress? {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return null
        return try {
            val progressList = fetchBookProgressListQRead(baseUrl, token, book.name)
            progressList.firstOrNull {
                !it.bookUrl.isNullOrBlank() && it.bookUrl == book.bookUrl
            }?.toBookProgress()
                ?: progressList.firstOrNull {
                    it.name == book.name && it.author == book.author
                }?.toBookProgress()
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead拉取进度异常\n${e.localizedMessage}", e)
            null
        }
    }

    private suspend fun fetchBookProgressListQRead(
        baseUrl: String,
        token: String,
        name: String? = null
    ): List<QReadBookProgressPayload> {
        QREAD_API_VERSIONS.forEach { version ->
            val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOKSHELF.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .addQueryParameter(PARAM_VERSION, QREAD_CLIENT_VERSION)
                .apply {
                    if (!name.isNullOrBlank()) {
                        addQueryParameter(PARAM_NAME, name)
                    }
                }
                .build()
            val request = Request.Builder()
                .url(requestUrl)
                .get()
                .build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX getBookshelf HTTP ${response.code}, v=$version")
                    return@use
                }
                val body = response.body.string()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    val errorMsg = root.optString(QREAD_ERROR_MSG)
                    if (errorMsg.isNotBlank()) {
                        AppLog.put("$LOG_QREAD_PREFIX getBookshelf isSuccess=false, v=$version, error=$errorMsg")
                    }
                    return@use
                }
                val dataArray = root.optJSONArray(QREAD_DATA) ?: return@use
                val result = mutableListOf<QReadBookProgressPayload>()
                for (i in 0 until dataArray.length()) {
                    val item = dataArray.optJSONObject(i) ?: continue
                    result.add(
                        QReadBookProgressPayload(
                            name = item.optString(QREAD_NAME),
                            author = item.optString(QREAD_AUTHOR),
                            bookUrl = item.optString(QREAD_BOOK_URL),
                            durChapterIndex = item.optInt(QREAD_DUR_CHAPTER_INDEX, 0),
                            durChapterPos = item.optDouble(QREAD_DUR_CHAPTER_POS, 0.0).toInt(),
                            durChapterTime = item.optLong(QREAD_DUR_CHAPTER_TIME, 0L),
                            durChapterTitle = item.optString(QREAD_DUR_CHAPTER_TITLE).ifBlank { null },
                            sourceJsonForShelf = item.toString()
                        )
                    )
                }
                return result
            }
        }
        return emptyList()
    }

    private suspend fun downloadAllBookProgressQRead() {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return
        val progressList = fetchBookProgressListQRead(baseUrl, token)
        if (progressList.isEmpty()) return

        progressList.forEach { payload ->
            val progress = payload.toBookProgress()
            val bookUrl = payload.bookUrl?.trim().orEmpty()
            val existing = when {
                bookUrl.isNotBlank() -> appDb.bookDao.getBook(bookUrl)
                else -> null
            } ?: appDb.bookDao.getBook(payload.name, payload.author)

            if (existing != null) {
                if (progress.durChapterIndex > existing.durChapterIndex ||
                    (progress.durChapterIndex == existing.durChapterIndex &&
                        progress.durChapterPos > existing.durChapterPos)
                ) {
                    existing.durChapterIndex = progress.durChapterIndex
                    existing.durChapterPos = progress.durChapterPos
                    if (!progress.durChapterTitle.isNullOrBlank()) {
                        existing.durChapterTitle = progress.durChapterTitle
                    }
                    if (progress.durChapterTime > 0L) {
                        existing.durChapterTime = progress.durChapterTime
                    }
                    appDb.bookDao.update(existing)
                }
            } else {
                val newBook = payload.toShelfBook() ?: return@forEach
                if (appDb.bookDao.getBook(newBook.bookUrl) == null) {
                    appDb.bookDao.insert(newBook)
                }
            }
        }
    }
}

private data class QReadRssSourceSummary(
    val sourceUrl: String,
    val sourceGroup: String?,
    val enabled: Boolean
)

private data class QReadBookProgressPayload(
    val name: String,
    val author: String,
    val bookUrl: String?,
    val durChapterIndex: Int,
    val durChapterPos: Int,
    val durChapterTime: Long,
    val durChapterTitle: String?,
    val sourceJsonForShelf: String = ""
) {
    fun toBookProgress(): BookProgress {
        return BookProgress(
            name = name,
            author = author,
            durChapterIndex = durChapterIndex,
            durChapterPos = durChapterPos,
            durChapterTime = durChapterTime,
            durChapterTitle = durChapterTitle
        )
    }

    /** 将云端 getBookshelf 单条 JSON 还原为本地书架 [Book]，用于空设备首次下拉同步 */
    fun toShelfBook(): Book? {
        val url = bookUrl?.trim().orEmpty()
        if (url.isBlank() || sourceJsonForShelf.isBlank()) return null
        val book = GSON.fromJsonObject<Book>(sourceJsonForShelf).getOrNull()?.takeIf { it.bookUrl.isNotBlank() }
            ?: return null
        book.durChapterIndex = durChapterIndex
        book.durChapterPos = durChapterPos
        if (durChapterTime > 0L) book.durChapterTime = durChapterTime
        if (!durChapterTitle.isNullOrBlank()) book.durChapterTitle = durChapterTitle
        return book
    }
}
