package io.legado.app.help.remote

import io.legado.app.BuildConfig
import io.legado.app.constant.AppLog
import io.legado.app.constant.BookType
import io.legado.app.constant.EventBus
import io.legado.app.constant.PreferKey
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
import io.legado.app.utils.getPrefStringSet
import io.legado.app.utils.putPrefStringSet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.FormBody
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import org.json.JSONObject
import android.os.Handler
import android.os.Looper
import splitties.init.appCtx
import io.legado.app.model.ReadBook
import io.legado.app.model.ReadManga
import io.legado.app.utils.LogUtils
import io.legado.app.utils.postEvent

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
    private const val QREAD_API_VERSION = 5
    private const val QREAD_PATH_GET_BOOKSHELF = "/api/%d/getBookshelf"
    private const val QREAD_PATH_GET_BOOKSHELF_PAGE = "/api/%d/getBookshelfPage"
    private const val QREAD_PATH_GET_BOOKSHELF_NEW = "/api/%d/getBookshelfNew"
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
    private const val QREAD_PATH_WS = "/api/%d/ws"
    private const val PARAM_ACCESS_TOKEN = "accessToken"
    private const val PARAM_VERSION = "version"
    private const val PARAM_MD5 = "md5"
    private const val PARAM_PAGE = "page"
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
    private const val TAG_QREAD_PUSH = "QReadPush"
    private const val QREAD_PUSH_MSG_LOG_MAX = 240
    private const val QREAD_PUSH_RECONNECT_DELAY_MS = 15_000L
    private const val QREAD_HEARTBEAT_INTERVAL_MS = 30_000L
    private const val QREAD_HEARTBEAT_PAYLOAD = """{"msg":"HEART_CHECK","message":"请求心跳"}"""
    @Volatile
    private var qreadSocket: WebSocket? = null
    @Volatile
    private var qreadSocketConnecting = false
    @Volatile
    private var qreadHeartbeatSeq = 0L

    /**
     * 统一同步开关语义：
     * - qread: 始终启用（不再受 syncBookProgress 影响）
     * - webdav: 受 syncBookProgress 控制
     * - local: 关闭
     */
    fun isProgressSyncEnabled(): Boolean {
        return when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.QREAD -> true
            RemoteSyncMode.WEBDAV -> AppConfig.syncBookProgress
            RemoteSyncMode.LOCAL_ONLY -> false
        }
    }

    private inline fun qreadPushDebug(lazyMsg: () -> String) {
        if (BuildConfig.DEBUG || AppConfig.recordLog) {
            val msg = lazyMsg()
            LogUtils.d(TAG_QREAD_PUSH, msg)
            if (AppConfig.recordLog) {
                AppLog.putNotSave("[$TAG_QREAD_PUSH] $msg")
            }
        }
    }

    private fun stopQReadHeartbeat() {
        qreadHeartbeatSeq++
    }

    private fun startQReadHeartbeat(webSocket: WebSocket) {
        val seq = ++qreadHeartbeatSeq
        qreadPushDebug { "heartbeat start interval=${QREAD_HEARTBEAT_INTERVAL_MS}ms" }
        Coroutine.async {
            while (qreadHeartbeatSeq == seq && qreadSocket === webSocket) {
                delay(QREAD_HEARTBEAT_INTERVAL_MS)
                if (qreadHeartbeatSeq != seq || qreadSocket !== webSocket) break
                val sent = runCatching { webSocket.send(QREAD_HEARTBEAT_PAYLOAD) }.getOrDefault(false)
                qreadPushDebug { "heartbeat send ok=$sent" }
                if (!sent) break
            }
            qreadPushDebug { "heartbeat stop seq=$seq" }
        }
    }

    /**
     * 对齐网页端策略：
     * - 首次收到 md5 仅记忆，不触发同步
     * - 后续只有 md5 变化才触发同步
     */
    @Synchronized
    private fun shouldSyncOnMd5(kind: String, md5: String): Boolean {
        return when (kind) {
            "bookmd5" -> {
                val old = appCtx.getPrefString(PreferKey.qreadLastBookMd5)
                appCtx.putPrefString(PreferKey.qreadLastBookMd5, md5)
                old != null && old != md5
            }

            "sourcemd5" -> {
                val old = appCtx.getPrefString(PreferKey.qreadLastSourceMd5)
                appCtx.putPrefString(PreferKey.qreadLastSourceMd5, md5)
                old != null && old != md5
            }

            "rssmd5" -> {
                val old = appCtx.getPrefString(PreferKey.qreadLastRssMd5)
                appCtx.putPrefString(PreferKey.qreadLastRssMd5, md5)
                old != null && old != md5
            }

            else -> false
        }
    }

    /** WebSocket URL 日志脱敏：不输出 token */
    private fun maskQreadWebSocketUrlForLog(wsUrl: String): String {
        val q = wsUrl.indexOf('?')
        if (q < 0) return wsUrl
        return wsUrl.substring(0, q) + "?id=***"
    }

    /** 与推送、本地存库的 bookUrl 比对时：去首尾空白与末尾 `/`，避免同书不同写法导致无法「挤下线」。 */
    fun normalizeQReadBookUrl(url: String): String = url.trim().trimEnd('/')

    /** QRead WebSocket `read` 消息里的书籍地址（兼容不同 JSON 键名）。 */
    private fun qreadReadMessageBookUrl(msg: JSONObject): String {
        return msg.optString("bookurl").ifBlank { msg.optString("bookUrl") }
            .ifBlank { msg.optString("book_url") }.trim()
    }

    /**
     * 该书是否正在本机文字/漫画阅读界面打开。
     * 正在阅读时由界面更新进度；全量拉取云端进度后仅在不属该状态时对书架发 [EventBus.UP_BOOKSHELF]，避免与本地「下一章」等日志混淆观感。
     */
    private fun isBookOpenInReader(bookUrl: String): Boolean {
        if (bookUrl.isBlank()) return false
        val n = normalizeQReadBookUrl(bookUrl)
        ReadBook.callBack?.let {
            ReadBook.book?.bookUrl?.takeIf { u -> u.isNotBlank() }?.let { u ->
                if (normalizeQReadBookUrl(u) == n) return true
            }
        }
        ReadManga.mCallback?.let {
            ReadManga.book?.bookUrl?.takeIf { u -> u.isNotBlank() }?.let { u ->
                if (normalizeQReadBookUrl(u) == n) return true
            }
        }
        return false
    }

    private fun shouldDeleteByRemoteMissing(book: Book, remoteUrls: Set<String>): Boolean {
        if ((book.type and BookType.local) != 0) return false
        if (book.bookUrl.isBlank()) return false
        val normalized = normalizeQReadBookUrl(book.bookUrl)
        if (normalized in remoteUrls) return false
        if (isBookOpenInReader(book.bookUrl)) return false
        return true
    }

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
        onSuccess: (() -> Unit)? = null,
        ensureShelf: Boolean = false
    ) {
        withContext(Dispatchers.IO) {
            when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
                RemoteSyncMode.LOCAL_ONLY -> onSuccess?.invoke()
                RemoteSyncMode.WEBDAV -> AppWebDav.uploadBookProgress(progress, onSuccess)
                RemoteSyncMode.QREAD -> uploadBookProgressQRead(progress, onSuccess, ensureShelf)
            }
        }
    }

    fun scheduleUploadOnChapterChanged(progress: BookProgress) {
        if (!isProgressSyncEnabled()) return
        Coroutine.async {
            uploadBookProgress(progress)
        }
    }

    /**
     * 离开阅读界面时上传当前进度（对齐轻阅读关书行为）。仅 QRead。
     */
    fun scheduleUploadOnReaderExitQRead(progress: BookProgress) {
        if (!isProgressSyncEnabled()) return
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) return
        Coroutine.async {
            uploadBookProgress(progress, ensureShelf = false)
        }
    }

    /**
     * 进入阅读界面时上传当前进度（用于触发服务端 read 推送，实现网页/其他端挤出）。仅 QRead。
     */
    fun scheduleUploadOnReaderEnterQRead(progress: BookProgress) {
        if (!isProgressSyncEnabled()) return
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) return
        Coroutine.async {
            uploadBookProgress(progress, ensureShelf = false)
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

    suspend fun fullSyncOnStartup() {
        when (RemoteSyncMode.fromValue(AppConfig.remoteSyncMode)) {
            RemoteSyncMode.LOCAL_ONLY -> Unit
            RemoteSyncMode.WEBDAV -> downloadAllBookProgress()
            RemoteSyncMode.QREAD -> {
                // Align with QRead client startup behavior: connect push first.
                startQReadPushIfEnabled()
            }
        }
    }

    fun startQReadPushIfEnabled() {
        if (!AppConfig.remoteSyncMode.equals(MODE_QREAD, true)) {
            qreadPushDebug { "skip: remoteSyncMode=${AppConfig.remoteSyncMode}" }
            return
        }
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken.trim()
        if (baseUrl.isBlank() || token.isBlank()) {
            qreadPushDebug { "skip: baseUrlOrTokenBlank baseUrlLen=${baseUrl.length} tokenLen=${token.length}" }
            return
        }
        if (qreadSocket != null || qreadSocketConnecting) {
            qreadPushDebug { "skip: already socket=${qreadSocket != null} connecting=$qreadSocketConnecting" }
            return
        }
        qreadSocketConnecting = true
        val wsUrl = qreadWebSocketUrl(baseUrl, token) ?: run {
            qreadSocketConnecting = false
            qreadPushDebug { "abort: qreadWebSocketUrl null (invalid baseUrl scheme?)" }
            return
        }
        qreadPushDebug { "connect ${maskQreadWebSocketUrlForLog(wsUrl)}" }
        val request = Request.Builder().url(wsUrl).build()
        qreadSocket = okHttpClient.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                qreadSocketConnecting = false
                qreadPushDebug { "onOpen http=${response.code} ${response.message}" }
                startQReadHeartbeat(webSocket)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                val snippet = if (text.length <= QREAD_PUSH_MSG_LOG_MAX) {
                    text
                } else {
                    text.take(QREAD_PUSH_MSG_LOG_MAX) + "…(${text.length})"
                }
                qreadPushDebug { "onMessage len=${text.length} $snippet" }
                handleQReadPushMessage(text)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                qreadSocket = null
                qreadSocketConnecting = false
                stopQReadHeartbeat()
                val err = t.localizedMessage ?: t.message ?: t.javaClass.simpleName
                qreadPushDebug {
                    "onFailure http=${response?.code} ${response?.message} err=${t.javaClass.simpleName}: $err"
                }
                AppLog.put("QRead推送连接失败: $err", t)
                Coroutine.async {
                    qreadPushDebug { "reconnect in ${QREAD_PUSH_RECONNECT_DELAY_MS}ms" }
                    delay(QREAD_PUSH_RECONNECT_DELAY_MS)
                    startQReadPushIfEnabled()
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                qreadSocket = null
                qreadSocketConnecting = false
                stopQReadHeartbeat()
                qreadPushDebug { "onClosed code=$code reason=$reason" }
            }
        })
    }

    private fun qreadWebSocketUrl(baseUrl: String, token: String): String? {
        val httpUrl = runCatching { baseUrl.toHttpUrl() }.getOrNull() ?: return null
        // OkHttp 的 HttpUrl.Builder 仅允许 http/https，不能直接设为 ws/wss
        val wsScheme = when (httpUrl.scheme.lowercase()) {
            "https", "wss" -> "wss"
            "http", "ws" -> "ws"
            else -> return null
        }
        val builderScheme = if (wsScheme == "wss") "https" else "http"
        val built = httpUrl.newBuilder()
            .scheme(builderScheme)
            .encodedPath(QREAD_PATH_WS.format(QREAD_API_VERSION))
            .setQueryParameter("id", token)
            .build()
            .toString()
        return built.replaceFirst(
            "${builderScheme}://",
            "${wsScheme}://",
            ignoreCase = true
        )
    }

    private fun handleQReadPushMessage(text: String) {
        val msg = runCatching { JSONObject(text) }.getOrNull() ?: run {
            qreadPushDebug { "drop: not JSON, head=${text.take(80)}" }
            return
        }
        val kind = msg.optString("msg")
        when (kind) {
            "read" -> {
                val bookUrlRaw = qreadReadMessageBookUrl(msg)
                if (bookUrlRaw.isBlank()) {
                    qreadPushDebug { "read: empty bookurl" }
                    return
                }
                val bookUrlNorm = normalizeQReadBookUrl(bookUrlRaw)
                qreadPushDebug { "dispatch read(bookUrl.len=${bookUrlNorm.length}) -> update single book progress" }
                Coroutine.async {
                    syncBookProgressByPushBookUrl(bookUrlRaw, bookUrlNorm)
                }
                Handler(Looper.getMainLooper()).post {
                    postEvent(EventBus.QREAD_REMOTE_READ, bookUrlNorm)
                }
            }

            "bookmd5" -> {
                val md5 = msg.optString("md5").trim()
                val shouldSync = shouldSyncOnMd5(kind, md5)
                if (!shouldSync) {
                    qreadPushDebug { "bookmd5 cache-only md5.len=${md5.length}" }
                    return
                }
                qreadPushDebug { "dispatch bookmd5 changed -> downloadAllBookProgress md5.len=${md5.length}" }
                Coroutine.async {
                    downloadAllBookProgress()
                }
            }

            "sourcemd5", "rssmd5" -> {
                val md5 = msg.optString("md5").trim()
                val shouldSync = shouldSyncOnMd5(kind, md5)
                if (!shouldSync) {
                    qreadPushDebug { "$kind cache-only md5.len=${md5.length}" }
                    return
                }
                qreadPushDebug { "dispatch $kind changed -> syncQReadSourcesIfEnabled md5.len=${md5.length}" }
                Coroutine.async {
                    syncQReadSourcesIfEnabled()
                }
            }

            else -> {
                if (kind.isNotEmpty()) {
                    qreadPushDebug { "ignore msg type=$kind" }
                }
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
                val requestUrl = "$baseUrl${QREAD_PATH_DELETE_BOOKS.format(QREAD_API_VERSION)}".toHttpUrl()
                    .newBuilder()
                    .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                    .build()
                val request = Request.Builder().url(requestUrl).post(body).build()
                okHttpClient.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) {
                        AppLog.put("$LOG_QREAD_PREFIX deleteBooks HTTP ${response.code}")
                        return@use
                    }
                    val payload = response.body.string()
                    val root = runCatching { JSONObject(payload) }.getOrNull() ?: return@use
                    if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                        AppLog.put(
                            "$LOG_QREAD_PREFIX deleteBooks isSuccess=false, error=${
                                root.optString(QREAD_ERROR_MSG)
                            }"
                        )
                    }
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
                val sources = fetchBookSourcesQRead(baseUrl, token)
                if (sources.isEmpty()) return@withContext 0
                val remoteUrls = sources.mapNotNull { it.bookSourceUrl.trim().takeIf { u -> u.isNotBlank() } }.toSet()
                appDb.bookSourceDao.insert(*sources.toTypedArray())
                // 服务端删除：仅清理“历史由 QRead 同步过”的 URL，避免改动书源分组字段
                val lastManagedUrls = appCtx
                    .getPrefStringSet(PreferKey.qreadManagedBookSourceUrls, mutableSetOf())
                    ?.toSet()
                    ?: emptySet()
                if (lastManagedUrls.isNotEmpty()) {
                    lastManagedUrls
                        .asSequence()
                        .map { it.trim() }
                        .filter { it.isNotBlank() && it !in remoteUrls }
                        .distinct()
                        .forEach { appDb.bookSourceDao.delete(it) }
                }
                appCtx.putPrefStringSet(PreferKey.qreadManagedBookSourceUrls, remoteUrls.toMutableSet())
                sources.size
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                AppLog.put("QRead同步书源异常\n${e.localizedMessage}", e)
                0
            }
        }
    }

    private suspend fun fetchBookSourcesQRead(baseUrl: String, token: String): List<BookSource> {
        val summaries = fetchBookSourceSummariesFromReadApi(baseUrl, token)
        if (summaries.isEmpty()) return emptyList()
        val payload = fetchBookSourceJsonPayloadQRead(
            baseUrl = baseUrl,
            token = token,
            ids = summaries.map { it.bookSourceUrl }
        ) ?: return emptyList()
        val sources = GSON.fromJsonArray<BookSource>(payload).getOrNull() ?: emptyList()
        if (sources.isEmpty()) return emptyList()
        val summaryMap = summaries.associateBy { it.bookSourceUrl }
        sources.forEach { source ->
            val summary = summaryMap[source.bookSourceUrl] ?: return emptyList()
            source.enabled = summary.enabled
            source.enabledExplore = summary.enabledExplore
            source.bookSourceGroup = summary.bookSourceGroup
        }
        return sources
    }

    suspend fun syncRssSourcesFromQRead(accessToken: String? = null): Int {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = accessToken?.trim()?.takeIf { it.isNotBlank() } ?: AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return 0
        return withContext(Dispatchers.IO) {
            try {
                val summaries = fetchRssSourceSummariesQRead(baseUrl, token)
                if (summaries.isEmpty()) return@withContext 0
                val remoteUrls = summaries.mapNotNull { it.sourceUrl.trim().takeIf { u -> u.isNotBlank() } }.toSet()
                val rssSources = mutableListOf<RssSource>()
                summaries.forEach { summary ->
                    val source = fetchRssSourceDetailQRead(baseUrl, token, summary.sourceUrl) ?: return@forEach
                    source.enabled = summary.enabled
                    source.sourceGroup = summary.sourceGroup
                    rssSources.add(source)
                }
                if (rssSources.isEmpty()) return@withContext 0
                appDb.rssSourceDao.insert(*rssSources.toTypedArray())
                // 服务端删除：仅清理“历史由 QRead 同步过”的 URL，避免改动订阅源分组字段
                val lastManagedUrls = appCtx
                    .getPrefStringSet(PreferKey.qreadManagedRssSourceUrls, mutableSetOf())
                    ?.toSet()
                    ?: emptySet()
                if (lastManagedUrls.isNotEmpty()) {
                    lastManagedUrls
                        .asSequence()
                        .map { it.trim() }
                        .filter { it.isNotBlank() && it !in remoteUrls }
                        .distinct()
                        .forEach { appDb.rssSourceDao.delete(it) }
                }
                appCtx.putPrefStringSet(PreferKey.qreadManagedRssSourceUrls, remoteUrls.toMutableSet())
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
        onSuccess: (() -> Unit)? = null,
        ensureShelf: Boolean = true
    ) {
        val localBook = appDb.bookDao.getBook(progress.name, progress.author)
        uploadBookProgressQRead(localBook, progress, onSuccess, ensureShelf)
    }

    private suspend fun uploadBookProgressQRead(
        book: Book?,
        progress: BookProgress,
        onSuccess: (() -> Unit)? = null,
        ensureShelf: Boolean = true
    ) {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        val bookUrl = book?.bookUrl.orEmpty()
        if (baseUrl.isBlank() || token.isBlank() || bookUrl.isBlank()) return
        try {
            if (ensureShelf && book != null && !ensureBookOnQReadShelf(baseUrl, token, book)) {
                AppLog.put("QRead书架缺书且自动入架失败, bookUrl=$bookUrl")
                return
            }
            val form = FormBody.Builder()
                .add(PARAM_ACCESS_TOKEN, token)
                .add(PARAM_URL, bookUrl)
                .add(PARAM_POS, progress.durChapterPos.toDouble().toString())
                .add(PARAM_TITLE, progress.durChapterTitle.orEmpty())
                .add(PARAM_INDEX, progress.durChapterIndex.toString())
                .add(PARAM_IS_NEW, "0")
                .build()
            val request = Request.Builder()
                .url("$baseUrl${QREAD_PATH_SAVE_BOOK_PROGRESS.format(QREAD_API_VERSION)}")
                .post(form)
                .build()
            val uploadOk = okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX saveBookProgress HTTP ${response.code}")
                    return@use false
                }
                val payload = response.body.string()
                val json = runCatching { JSONObject(payload) }.getOrNull() ?: return@use false
                if (json.optBoolean(QREAD_IS_SUCCESS, false)) {
                    true
                } else {
                    AppLog.put(
                        "$LOG_QREAD_PREFIX saveBookProgress isSuccess=false, error=${
                            json.optString(QREAD_ERROR_MSG)
                        }"
                    )
                    false
                }
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
        val requestUrl = "$baseUrl${QREAD_PATH_SAVE_BOOKS.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder()
            .url(requestUrl)
            .post(content.toRequestBody(QREAD_CONTENT_TYPE_JSON.toMediaType()))
            .build()
        okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX saveBooks HTTP ${response.code}")
                return false
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return false
            if (root.optBoolean(QREAD_IS_SUCCESS, false)) {
                return true
            }
            AppLog.put(
                "$LOG_QREAD_PREFIX saveBooks isSuccess=false, error=${
                    root.optString(QREAD_ERROR_MSG)
                }"
            )
            return false
        }
    }

    /**
     * 使用 [ReadController.getBookSources] 拉取书源摘要，
     * 其中 enabled/enabledExplore/bookSourceGroup 以摘要返回为准。
     */
    private fun fetchBookSourceSummariesFromReadApi(
        baseUrl: String,
        token: String
    ): List<QReadBookSourceSummary> {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCES_LIST.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .addQueryParameter("isall", "1")
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        return okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getBookSources HTTP ${response.code}")
                return@use emptyList()
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList()
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getBookSources isSuccess=false, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return@use emptyList()
            }
            val data = root.optJSONArray(QREAD_DATA) ?: return@use emptyList()
            val list = linkedMapOf<String, QReadBookSourceSummary>()
            for (i in 0 until data.length()) {
                val item = data.optJSONObject(i) ?: continue
                val url = item.optString(QREAD_BOOK_SOURCE_URL).orEmpty().trim()
                if (url.isBlank()) continue
                list[url] = QReadBookSourceSummary(
                    bookSourceUrl = url,
                    enabled = item.optBoolean(QREAD_ENABLED, true),
                    enabledExplore = item.optBoolean("enabledExplore", true),
                    bookSourceGroup = item.optString("bookSourceGroup").ifBlank { null }
                )
            }
            list.values.toList()
        }
    }

    private suspend fun fetchBookSourceJsonPayloadQRead(
        baseUrl: String,
        token: String,
        ids: List<String>
    ): String? {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOK_SOURCE_JSON.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder()
            .url(requestUrl)
            .post(JSONArray(ids).toString().toRequestBody(QREAD_CONTENT_TYPE_JSON.toMediaType()))
            .build()
        okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getbookSourcejson HTTP ${response.code}")
                return null
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return null
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getbookSourcejson isSuccess=false, error=${
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
        val requestUrl = "$baseUrl${QREAD_PATH_GET_RSS_SOURCES_BULK.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        return okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getRssSourcess HTTP ${response.code}")
                return@use emptyList()
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList()
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getRssSourcess isSuccess=false, error=${
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
    }

    private suspend fun fetchRssSourceDetailQRead(
        baseUrl: String,
        token: String,
        sourceUrl: String
    ): RssSource? {
        val requestUrl = "$baseUrl${QREAD_PATH_GET_RSS_SOURCE.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .addQueryParameter("id", sourceUrl)
            .build()
        val request = Request.Builder().url(requestUrl).get().build()
        return okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getRssSources HTTP ${response.code}")
                return@use null
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use null
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                AppLog.put(
                    "$LOG_QREAD_PREFIX getRssSources isSuccess=false, error=${
                        root.optString(QREAD_ERROR_MSG)
                    }"
                )
                return@use null
            }
            val data = root.optJSONObject(QREAD_DATA) ?: return@use null
            val sourceJson = data.optString(QREAD_JSON).orEmpty()
            if (sourceJson.isBlank()) return@use null
            GSON.fromJsonObject<RssSource>(sourceJson).getOrNull()
        }
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

    private suspend fun syncBookProgressByPushBookUrl(rawFromPush: String, normalized: String) {
        val local = appDb.bookDao.getBook(rawFromPush)
            ?: appDb.bookDao.getBook(normalized)
            ?: appDb.bookDao.webBooks.firstOrNull {
                normalizeQReadBookUrl(it.bookUrl) == normalized
            }
            ?: return
        val remote = getBookProgressQRead(local) ?: return
        val changed = remote.durChapterIndex != local.durChapterIndex ||
            remote.durChapterPos != local.durChapterPos ||
            (!remote.durChapterTitle.isNullOrBlank() && remote.durChapterTitle != local.durChapterTitle) ||
            (remote.durChapterTime > 0L && remote.durChapterTime != local.durChapterTime)
        if (!changed) return
        local.durChapterIndex = remote.durChapterIndex
        local.durChapterPos = remote.durChapterPos
        if (!remote.durChapterTitle.isNullOrBlank()) {
            local.durChapterTitle = remote.durChapterTitle
        }
        if (remote.durChapterTime > 0L) {
            local.durChapterTime = remote.durChapterTime
        }
        appDb.bookDao.update(local)
        if (!isBookOpenInReader(local.bookUrl)) {
            postEvent(EventBus.UP_BOOKSHELF, local.bookUrl)
        }
    }

    private suspend fun fetchBookProgressListQRead(
        baseUrl: String,
        token: String,
        name: String? = null
    ): List<QReadBookProgressPayload> {
        if (name.isNullOrBlank()) {
            return fetchBookProgressListQReadByPage(baseUrl, token)
        }
        val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOKSHELF.format(QREAD_API_VERSION)}".toHttpUrl()
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
        return okHttpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getBookshelf HTTP ${response.code}")
                return@use emptyList()
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList()
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                val errorMsg = root.optString(QREAD_ERROR_MSG)
                if (errorMsg.isNotBlank()) {
                    AppLog.put("$LOG_QREAD_PREFIX getBookshelf isSuccess=false, error=$errorMsg")
                }
                return@use emptyList()
            }
            val dataArray = root.optJSONArray(QREAD_DATA) ?: return@use emptyList()
            val result = parseQReadProgressPayloads(dataArray)
            result
        }
    }

    private fun parseQReadProgressPayloads(dataArray: JSONArray): MutableList<QReadBookProgressPayload> {
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

    private suspend fun fetchBookProgressListQReadByPage(
        baseUrl: String,
        token: String
    ): List<QReadBookProgressPayload> {
        val pageMetaUrl = "$baseUrl${QREAD_PATH_GET_BOOKSHELF_PAGE.format(QREAD_API_VERSION)}".toHttpUrl()
            .newBuilder()
            .addQueryParameter(PARAM_ACCESS_TOKEN, token)
            .build()
        val pageMetaRequest = Request.Builder().url(pageMetaUrl).get().build()
        val meta = okHttpClient.newCall(pageMetaRequest).execute().use { response ->
            if (!response.isSuccessful) {
                AppLog.put("$LOG_QREAD_PREFIX getBookshelfPage HTTP ${response.code}")
                return emptyList()
            }
            val body = response.body.string()
            val root = runCatching { JSONObject(body) }.getOrNull() ?: return emptyList()
            if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                val errorMsg = root.optString(QREAD_ERROR_MSG)
                if (errorMsg.isNotBlank()) {
                    AppLog.put("$LOG_QREAD_PREFIX getBookshelfPage isSuccess=false, error=$errorMsg")
                }
                return emptyList()
            }
            root.optJSONObject(QREAD_DATA) ?: return emptyList()
        }
        val totalPage = meta.optInt(PARAM_PAGE, 0)
        val md5 = meta.optString(PARAM_MD5).orEmpty()
        if (totalPage <= 0 || md5.isBlank()) {
            return emptyList()
        }
        val all = mutableListOf<QReadBookProgressPayload>()
        for (page in 1..totalPage) {
            val requestUrl = "$baseUrl${QREAD_PATH_GET_BOOKSHELF_NEW.format(QREAD_API_VERSION)}".toHttpUrl()
                .newBuilder()
                .addQueryParameter(PARAM_ACCESS_TOKEN, token)
                .addQueryParameter(PARAM_MD5, md5)
                .addQueryParameter(PARAM_PAGE, page.toString())
                .build()
            val request = Request.Builder().url(requestUrl).get().build()
            val onePage = okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    AppLog.put("$LOG_QREAD_PREFIX getBookshelfNew HTTP ${response.code}, page=$page")
                    return@use emptyList<QReadBookProgressPayload>()
                }
                val body = response.body.string()
                val root = runCatching { JSONObject(body) }.getOrNull() ?: return@use emptyList<QReadBookProgressPayload>()
                if (!root.optBoolean(QREAD_IS_SUCCESS, false)) {
                    val errorMsg = root.optString(QREAD_ERROR_MSG)
                    if (errorMsg.isNotBlank()) {
                        AppLog.put("$LOG_QREAD_PREFIX getBookshelfNew isSuccess=false, page=$page, error=$errorMsg")
                    }
                    return@use emptyList<QReadBookProgressPayload>()
                }
                val dataArray = root.optJSONArray(QREAD_DATA) ?: return@use emptyList<QReadBookProgressPayload>()
                parseQReadProgressPayloads(dataArray)
            }
            all.addAll(onePage)
        }
        return all
    }

    private suspend fun downloadAllBookProgressQRead() {
        val baseUrl = AppConfig.qreadBaseUrl.trimEnd('/')
        val token = AppConfig.qreadToken
        if (baseUrl.isBlank() || token.isBlank()) return
        val progressList = fetchBookProgressListQRead(baseUrl, token)
        if (progressList.isEmpty()) return
        var updated = 0
        var inserted = 0
        var deleted = 0
        val remoteUrls = progressList
            .mapNotNull { it.bookUrl?.trim()?.takeIf { u -> u.isNotBlank() } }
            .map { normalizeQReadBookUrl(it) }
            .toSet()

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
                    updated++
                    if (!isBookOpenInReader(existing.bookUrl)) {
                        postEvent(EventBus.UP_BOOKSHELF, existing.bookUrl)
                    }
                }
            } else {
                val newBook = payload.toShelfBook() ?: return@forEach
                if (appDb.bookDao.getBook(newBook.bookUrl) == null) {
                    appDb.bookDao.insert(newBook)
                    inserted++
                    if (!isBookOpenInReader(newBook.bookUrl)) {
                        postEvent(EventBus.UP_BOOKSHELF, newBook.bookUrl)
                    }
                }
            }
        }
        appDb.bookDao.webBooks.forEach { local ->
            if (!shouldDeleteByRemoteMissing(local, remoteUrls)) return@forEach
            appDb.bookDao.delete(local)
            deleted++
        }
    }
}

private data class QReadRssSourceSummary(
    val sourceUrl: String,
    val sourceGroup: String?,
    val enabled: Boolean
)

private data class QReadBookSourceSummary(
    val bookSourceUrl: String,
    val enabled: Boolean,
    val enabledExplore: Boolean,
    val bookSourceGroup: String?
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
