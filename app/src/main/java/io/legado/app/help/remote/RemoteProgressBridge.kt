package io.legado.app.help.remote

import io.legado.app.constant.AppLog
import io.legado.app.data.appDb
import io.legado.app.data.entities.Book
import io.legado.app.data.entities.BookSource
import io.legado.app.data.entities.BookProgress
import io.legado.app.data.entities.RssSource
import io.legado.app.help.AppWebDav
import io.legado.app.help.config.AppConfig
import io.legado.app.help.http.okHttpClient
import io.legado.app.utils.GSON
import io.legado.app.utils.fromJsonArray
import io.legado.app.utils.fromJsonObject
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
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
    private const val QREAD_PATH_GET_BOOK_SOURCES_PAGE = "/api/%d/getBookSourcesPage"
    private const val QREAD_PATH_GET_BOOK_SOURCES_NEW = "/api/%d/getBookSourcesNew"
    private const val QREAD_PATH_GET_BOOK_SOURCE_JSON = "/api/%d/getbookSourcejson"
    private const val QREAD_PATH_GET_RSS_SOURCES_PAGE = "/api/%d/getRssSourcessPage"
    private const val QREAD_PATH_GET_RSS_SOURCES_NEW = "/api/%d/getRssSourcessNew"
    private const val QREAD_PATH_GET_RSS_SOURCE = "/api/%d/getRssSources"
    private const val PARAM_ACCESS_TOKEN = "accessToken"
    private const val PARAM_VERSION = "version"
    private const val PARAM_NAME = "name"
    private const val PARAM_MD5 = "md5"
    private const val PARAM_PAGE = "page"
    private const val PARAM_URL = "url"
    private const val PARAM_POS = "pos"
    private const val PARAM_TITLE = "title"
    private const val PARAM_INDEX = "index"
    private const val PARAM_IS_NEW = "isnew"
    private const val QREAD_IS_SUCCESS = "isSuccess"
    private const val QREAD_ERROR_MSG = "errorMsg"
    private const val QREAD_DATA = "data"
    private const val QREAD_BOOK_URL = "bookUrl"
    private const val QREAD_MD5 = "md5"
    private const val QREAD_PAGE = "page"
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

    suspend fun uploadBookProgress(
        book: Book,
        toast: Boolean = false,
        onSuccess: (() -> Unit)? = null
    ) {
        when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
            RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(book, toast, onSuccess)
            RemoteSyncMode.QREAD -> uploadBookProgressQRead(book, BookProgress(book), onSuccess)
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

    suspend fun syncBookSourcesFromQRead(): Int {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return try {
            val sourceJson = fetchBookSourceJsonQRead(baseUrl, token)
            if (sourceJson.isBlank()) return 0
            val sources = GSON.fromJsonArray<BookSource>(sourceJson).getOrNull() ?: emptyList()
            if (sources.isEmpty()) return 0
            appDb.bookSourceDao.insert(*sources.toTypedArray())
            sources.size
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead同步书源异常\n${e.localizedMessage}", e)
            0
        }
    }

    suspend fun syncRssSourcesFromQRead(): Int {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return try {
            val summaries = fetchRssSourceSummariesQRead(baseUrl, token)
            if (summaries.isEmpty()) return 0
            val rssSources = mutableListOf<RssSource>()
            summaries.forEach { summary ->
                val source = fetchRssSourceDetailQRead(baseUrl, token, summary.sourceUrl) ?: return@forEach
                source.enabled = summary.enabled
                source.sourceGroup = summary.sourceGroup
                rssSources.add(source)
            }
            if (rssSources.isEmpty()) return 0
            appDb.rssSourceDao.insert(*rssSources.toTypedArray())
            rssSources.size
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            AppLog.put("QRead同步订阅源异常\n${e.localizedMessage}", e)
            0
        }
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
                    val payload = response.body?.string().orEmpty()
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
                val body = response.body?.string().orEmpty()
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
            val meta = fetchBookSourceMetaQRead(baseUrl, token, version) ?: return@forEach
            val ids = fetchBookSourceIdsQRead(baseUrl, token, version, meta.second, meta.first)
            if (ids.isEmpty()) return@forEach
            val payload = fetchBookSourceJsonPayloadQRead(baseUrl, token, version, ids)
            if (!payload.isNullOrBlank()) return payload
        }
        return ""
    }

    private suspend fun fetchBookSourceMetaQRead(
        baseUrl: String,
        token: String,
        version: Int
    ): Pair<Int, String>? {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCES_PAGE.format(version)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getBookSourcesPage HTTP ${response.code}, v=$version")
                return null
            }
            val body = response.body?.string().orEmpty()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return null
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getBookSourcesPage isSuccess=false, v=$version, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return null
            }
            val data = root.optJSONObject(QREAD_DATA) ?: return null
            val page = data.optInt(QREAD_PAGE, 0)
            val md5 = data.optString(QREAD_MD5).orEmpty()
            if (page <= 0 || md5.isBlank()) return null
            return page to md5
        }
    }

    private suspend fun fetchBookSourceIdsQRead(
        baseUrl: String,
        token: String,
        version: Int,
        md5: String,
        pageCount: Int
    ): List<String> {
        val ids = linkedSetOf<String>()
        for (page in 1..pageCount) {
            val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCES_NEW.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .addQueryParameter(PARAM_MD5, md5)
                .addQueryParameter(PARAM_PAGE, page.toString())
                .build()
            val request = Request.Builder().url(requestUrl).get().build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX getBookSourcesNew HTTP ${response.code}, v=$version, page=$page")
                    return@use
                }
                val body = response.body?.string().orEmpty()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX getBookSourcesNew isSuccess=false, v=$version, page=$page, error=${
                            root.optString(QREAD_ERROR_MSG)
                        }"
                    )
                    return@use
                }
                val data = root.optJSONArray(QREAD_DATA) ?: return@use
                for (i in 0 until data.length()) {
                    val item = data.optJSONObject(i) ?: continue
                    val id = item.optString(QREAD_BOOK_SOURCE_URL).orEmpty()
                    if (id.isNotBlank()) ids.add(id)
                }
            }
        }
        return ids.toList()
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
            val body = response.body?.string().orEmpty()
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

    private suspend fun fetchRssSourceSummariesQRead(
        baseUrl: String,
        token: String
    ): List<QReadRssSourceSummary> {
        QREAD_API_VERSIONS.forEach { version ->
            val meta = fetchPagedMetaQRead(
                baseUrl = baseUrl,
                token = token,
                pathTemplate = QREAD_PATH_GET_RSS_SOURCES_PAGE,
                requestName = "getRssSourcessPage",
                version = version
            ) ?: return@forEach
            val summaries = fetchPagedSourceSummariesQRead(
                baseUrl = baseUrl,
                token = token,
                version = version,
                pathTemplate = QREAD_PATH_GET_RSS_SOURCES_NEW,
                requestName = "getRssSourcessNew",
                md5 = meta.second,
                pageCount = meta.first,
                idField = QREAD_SOURCE_URL,
                groupField = QREAD_SOURCE_GROUP,
                enabledField = QREAD_ENABLED
            )
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
                val body = response.body?.string().orEmpty()
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

    private suspend fun fetchPagedMetaQRead(
        baseUrl: String,
        token: String,
        pathTemplate: String,
        requestName: String,
        version: Int
    ): Pair<Int, String>? {
        val requestUrl = "$baseUrl${pathTemplate.format(version)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX $requestName HTTP ${response.code}, v=$version")
                return null
            }
            val body = response.body?.string().orEmpty()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return null
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX $requestName isSuccess=false, v=$version, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return null
            }
            val data = root.optJSONObject(QREAD_DATA) ?: return null
            val page = data.optInt(QREAD_PAGE, 0)
            val md5 = data.optString(QREAD_MD5).orEmpty()
            if (page <= 0 || md5.isBlank()) return null
            return page to md5
        }
    }

    private suspend fun fetchPagedSourceSummariesQRead(
        baseUrl: String,
        token: String,
        version: Int,
        pathTemplate: String,
        requestName: String,
        md5: String,
        pageCount: Int,
        idField: String,
        groupField: String,
        enabledField: String
    ): List<QReadRssSourceSummary> {
        val summaries = linkedMapOf<String, QReadRssSourceSummary>()
        for (page in 1..pageCount) {
            val requestUrl = "$baseUrl${pathTemplate.format(version)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .addQueryParameter(PARAM_MD5, md5)
                .addQueryParameter(PARAM_PAGE, page.toString())
                .build()
            val request = Request.Builder().url(requestUrl).get().build()
            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX $requestName HTTP ${response.code}, v=$version, page=$page")
                    return@use
                }
                val body = response.body?.string().orEmpty()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX $requestName isSuccess=false, v=$version, page=$page, error=${
                            root.optString(QREAD_ERROR_MSG)
                        }"
                    )
                    return@use
                }
                val data = root.optJSONArray(QREAD_DATA) ?: return@use
                for (i in 0 until data.length()) {
                    val item = data.optJSONObject(i) ?: continue
                    val sourceUrl = item.optString(idField).orEmpty()
                    if (sourceUrl.isBlank()) continue
                    summaries[sourceUrl] = QReadRssSourceSummary(
                        sourceUrl = sourceUrl,
                        sourceGroup = item.optString(groupField).ifBlank { null },
                        enabled = item.optBoolean(enabledField, true)
                    )
                }
            }
        }
        return summaries.values.toList()
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
                val body = response.body?.string().orEmpty()
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
                            durChapterTitle = item.optString(QREAD_DUR_CHAPTER_TITLE).ifBlank { null }
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
        val localBooks = appDb.bookDao.all
        localBooks.forEach { book ->
            val progress = progressList.firstOrNull {
                !it.bookUrl.isNullOrBlank() && it.bookUrl == book.bookUrl
            }?.toBookProgress()
                ?: progressList.firstOrNull {
                    it.name == book.name && it.author == book.author
                }?.toBookProgress()
                ?: return@forEach
            if (progress.durChapterIndex > book.durChapterIndex ||
                (progress.durChapterIndex == book.durChapterIndex &&
                    progress.durChapterPos > book.durChapterPos)
            ) {
                book.durChapterIndex = progress.durChapterIndex
                book.durChapterPos = progress.durChapterPos
                if (!progress.durChapterTitle.isNullOrBlank()) {
                    book.durChapterTitle = progress.durChapterTitle
                }
                if (progress.durChapterTime > 0L) {
                    book.durChapterTime = progress.durChapterTime
                }
                appDb.bookDao.update(book)
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
    val durChapterTitle: String?
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
}
