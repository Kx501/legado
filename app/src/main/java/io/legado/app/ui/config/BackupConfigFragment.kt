package io.legado.app.ui.config

import android.content.Context
import android.content.SharedPreferences
import android.os.Bundle
import android.text.InputType
import android.view.Menu
import android.view.MenuInflater
import android.view.MenuItem
import android.view.View
import androidx.core.view.MenuProvider
import androidx.fragment.app.activityViewModels
import androidx.lifecycle.lifecycleScope
import androidx.preference.EditTextPreference
import androidx.preference.ListPreference
import androidx.preference.Preference
import androidx.preference.PreferenceCategory
import androidx.preference.PreferenceGroup
import io.legado.app.constant.PreferKey.qreadBaseUrl
import io.legado.app.constant.PreferKey.qreadPassword
import io.legado.app.constant.PreferKey.qreadToken
import io.legado.app.constant.PreferKey.qreadUsername
import io.legado.app.constant.PreferKey.remoteSyncMode
import io.legado.app.R
import io.legado.app.constant.AppLog
import io.legado.app.constant.PreferKey
import io.legado.app.exception.NoStackTraceException
import io.legado.app.help.AppWebDav
import io.legado.app.help.config.AppConfig
import io.legado.app.help.config.LocalConfig
import io.legado.app.help.coroutine.Coroutine
import io.legado.app.help.storage.Backup
import io.legado.app.help.storage.BackupConfig
import io.legado.app.help.storage.ImportOldData
import io.legado.app.help.remote.RemoteProgressBridge
import io.legado.app.help.storage.Restore
import io.legado.app.lib.dialogs.alert
import io.legado.app.lib.dialogs.selector
import io.legado.app.lib.permission.Permissions
import io.legado.app.lib.permission.PermissionsCompat
import io.legado.app.lib.prefs.fragment.PreferenceFragment
import io.legado.app.lib.theme.primaryColor
import io.legado.app.ui.about.AppLogDialog
import io.legado.app.ui.file.HandleFileContract
import io.legado.app.ui.widget.dialog.WaitDialog
import io.legado.app.utils.FileDoc
import io.legado.app.utils.applyTint
import io.legado.app.utils.checkWrite
import io.legado.app.utils.getPrefString
import io.legado.app.utils.isContentScheme
import io.legado.app.utils.launch
import io.legado.app.utils.putPrefString
import io.legado.app.utils.setEdgeEffectColor
import io.legado.app.utils.showDialogFragment
import io.legado.app.utils.showHelp
import io.legado.app.utils.toEditable
import io.legado.app.utils.toastOnUi
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.Dispatchers.Main
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.FormBody
import okhttp3.Request
import org.json.JSONObject
import splitties.init.appCtx

class BackupConfigFragment : PreferenceFragment(),
    SharedPreferences.OnSharedPreferenceChangeListener,
    MenuProvider {
    companion object {
        private const val MODE_WEBDAV = "webdav"
        private const val MODE_QREAD = "qread"
        private const val WEB_DAV_CONFIG_CATEGORY = "webDavConfigCategory"
        private const val WEB_DAV_BACKUP_CATEGORY = "webDavBackupCategory"
        private const val QREAD_CONFIG_CATEGORY = "qreadConfigCategory"
        private const val QREAD_LOCAL_BACKUP_CATEGORY = "qreadLocalBackupCategory"
        private const val QREAD_MODEL = "Legado-Android"
        private const val QREAD_LOGIN_PATH_TEMPLATE = "/api/%d/login"
        private const val QREAD_PARAM_USERNAME = "username"
        private const val QREAD_PARAM_PASSWORD = "password"
        private const val QREAD_PARAM_MODEL = "model"
        private const val QREAD_API_VERSION = 5
    }

    private val viewModel by activityViewModels<ConfigViewModel>()
    private val waitDialog by lazy { WaitDialog(requireContext()) }
    private var backupJob: Job? = null
    private var restoreJob: Job? = null

    private val selectBackupPath = registerForActivityResult(HandleFileContract()) {
        it.uri?.let { uri ->
            if (uri.isContentScheme()) {
                AppConfig.backupPath = uri.toString()
            } else {
                AppConfig.backupPath = uri.path
            }
        }
    }
    private val backupDir = registerForActivityResult(HandleFileContract()) { result ->
        result.uri?.let { uri ->
            if (uri.isContentScheme()) {
                AppConfig.backupPath = uri.toString()
                backup(uri.toString())
            } else {
                uri.path?.let { path ->
                    AppConfig.backupPath = path
                    backup(path)
                }
            }
        }
    }
    private val restoreDoc = registerForActivityResult(HandleFileContract()) {
        it.uri?.let { uri ->
            waitDialog.setText("恢复中…")
            waitDialog.show()
            val task = Coroutine.async {
                Restore.restore(appCtx, uri)
            }.onFinally {
                waitDialog.dismiss()
            }
            waitDialog.setOnCancelListener {
                task.cancel()
            }
        }
    }
    private val restoreOld = registerForActivityResult(HandleFileContract()) {
        it.uri?.let { uri ->
            ImportOldData.importUri(appCtx, uri)
        }
    }

    override fun onCreatePreferences(savedInstanceState: Bundle?, rootKey: String?) {
        addPreferencesFromResource(R.xml.pref_config_backup)
        findPreference<EditTextPreference>(PreferKey.webDavPassword)?.let {
            it.setOnBindEditTextListener { editText ->
                editText.inputType =
                    InputType.TYPE_TEXT_VARIATION_PASSWORD or InputType.TYPE_CLASS_TEXT
                editText.setSelection(editText.text.length)
            }
        }
        findPreference<EditTextPreference>(qreadPassword)?.let {
            it.setOnBindEditTextListener { editText ->
                editText.inputType =
                    InputType.TYPE_TEXT_VARIATION_PASSWORD or InputType.TYPE_CLASS_TEXT
                editText.setSelection(editText.text.length)
            }
        }
        findPreference<EditTextPreference>(PreferKey.webDavDir)?.let {
            it.setOnBindEditTextListener { editText ->
                editText.text = AppConfig.webDavDir?.toEditable()
                editText.setSelection(editText.text.length)
            }
        }
        findPreference<EditTextPreference>(PreferKey.webDavDeviceName)?.let {
            it.setOnBindEditTextListener { editText ->
                editText.text = AppConfig.webDavDeviceName?.toEditable()
                editText.setSelection(editText.text.length)
            }
        }
        upPreferenceSummary(PreferKey.webDavUrl, getPrefString(PreferKey.webDavUrl))
        upPreferenceSummary(PreferKey.webDavAccount, getPrefString(PreferKey.webDavAccount))
        upPreferenceSummary(PreferKey.webDavPassword, getPrefString(PreferKey.webDavPassword))
        upPreferenceSummary(PreferKey.webDavDir, AppConfig.webDavDir)
        upPreferenceSummary(PreferKey.webDavDeviceName, AppConfig.webDavDeviceName)
        upPreferenceSummary(remoteSyncMode, getPrefString(remoteSyncMode))
        upPreferenceSummary(qreadBaseUrl, getPrefString(qreadBaseUrl))
        upPreferenceSummary(qreadUsername, getPrefString(qreadUsername))
        upPreferenceSummary(qreadPassword, getPrefString(qreadPassword))
        upPreferenceSummary(PreferKey.backupPath, getPrefString(PreferKey.backupPath))
        updateConfigCategoryVisibility(getPrefString(remoteSyncMode))
        findPreference<io.legado.app.lib.prefs.Preference>("web_dav_restore")
            ?.onLongClick {
                restoreFromLocal()
                true
            }
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        activity?.setTitle(R.string.backup_restore)
        preferenceManager.sharedPreferences?.registerOnSharedPreferenceChangeListener(this)
        listView.setEdgeEffectColor(primaryColor)
        activity?.addMenuProvider(this, viewLifecycleOwner)
        if (!LocalConfig.backupHelpVersionIsLast) {
            showHelp("webDavHelp")
        }
    }

    override fun onCreateMenu(menu: Menu, menuInflater: MenuInflater) {
        menuInflater.inflate(R.menu.backup_restore, menu)
        menu.applyTint(requireContext())
    }

    override fun onMenuItemSelected(menuItem: MenuItem): Boolean {
        when (menuItem.itemId) {
            R.id.menu_help -> {
                showHelp("webDavHelp")
                return true
            }

            R.id.menu_log -> showDialogFragment<AppLogDialog>()
        }
        return false
    }

    override fun onDestroy() {
        super.onDestroy()
        preferenceManager.sharedPreferences?.unregisterOnSharedPreferenceChangeListener(this)
    }

    override fun onSharedPreferenceChanged(sharedPreferences: SharedPreferences?, key: String?) {
        when (key) {
            PreferKey.backupPath -> upPreferenceSummary(key, getPrefString(key))
            PreferKey.webDavUrl,
            PreferKey.webDavAccount,
            PreferKey.webDavPassword,
            PreferKey.webDavDir,
            remoteSyncMode,
            qreadBaseUrl,
            qreadUsername,
            qreadPassword,
            qreadToken -> listView.post {
                upPreferenceSummary(key, appCtx.getPrefString(key))
                if (key == remoteSyncMode) {
                    updateConfigCategoryVisibility(appCtx.getPrefString(remoteSyncMode))
                }
                viewModel.upWebDavConfig()
            }

            PreferKey.webDavDeviceName -> upPreferenceSummary(key, getPrefString(key))
        }
    }

    private fun upPreferenceSummary(preferenceKey: String, value: String?) {
        val preference = findPreference<Preference>(preferenceKey) ?: return
        when (preferenceKey) {
            PreferKey.webDavUrl ->
                if (value.isNullOrBlank()) {
                    preference.summary = getString(R.string.web_dav_url_s)
                } else {
                    preference.summary = value
                }

            PreferKey.webDavAccount ->
                if (value.isNullOrBlank()) {
                    preference.summary = getString(R.string.web_dav_account_s)
                } else {
                    preference.summary = value
                }

            PreferKey.webDavPassword ->
                if (value.isNullOrEmpty()) {
                    preference.summary = getString(R.string.web_dav_pw_s)
                } else {
                    preference.summary = "*".repeat(value.length)
                }

            qreadPassword ->
                if (value.isNullOrEmpty()) {
                    preference.summary = getString(R.string.qread_password_s)
                } else {
                    preference.summary = "*".repeat(value.length)
                }

            qreadBaseUrl ->
                if (value.isNullOrBlank()) {
                    preference.summary = getString(R.string.qread_base_url_s)
                } else {
                    preference.summary = value
                }

            qreadUsername ->
                if (value.isNullOrBlank()) {
                    preference.summary = getString(R.string.qread_username_s)
                } else {
                    preference.summary = value
                }

            PreferKey.webDavDir -> preference.summary = when (value) {
                null -> "legado"
                else -> value
            }

            else -> {
                if (preference is ListPreference) {
                    val index = preference.findIndexOfValue(value)
                    // Set the summary to reflect the new value.
                    preference.summary = if (index >= 0) preference.entries[index] else null
                } else {
                    preference.summary = value
                }
            }
        }
    }

    private fun updateConfigCategoryVisibility(mode: String?) {
        val isWebDavMode = mode.equals(MODE_WEBDAV, ignoreCase = true)
        findPreference<PreferenceCategory>(WEB_DAV_CONFIG_CATEGORY)?.isVisible = isWebDavMode
        findPreference<PreferenceCategory>(WEB_DAV_BACKUP_CATEGORY)?.isVisible = isWebDavMode
        findPreference<PreferenceCategory>(QREAD_CONFIG_CATEGORY)?.isVisible = !isWebDavMode
        findPreference<PreferenceCategory>(QREAD_LOCAL_BACKUP_CATEGORY)?.isVisible = !isWebDavMode
        moveBackupPathPreference(isWebDavMode)
    }

    private fun moveBackupPathPreference(isWebDavMode: Boolean) {
        val pref = findPreference<Preference>(PreferKey.backupPath) ?: return
        val target = if (isWebDavMode) {
            findPreference<PreferenceCategory>(WEB_DAV_BACKUP_CATEGORY)
        } else {
            findPreference<PreferenceCategory>(QREAD_LOCAL_BACKUP_CATEGORY)
        } ?: return
        val parent = pref.parent
        if (parent == target) {
            pref.order = 0
            return
        }
        parent?.removePreference(pref)
        target.addPreference(pref)
        pref.order = 0
    }

    override fun onPreferenceTreeClick(preference: Preference): Boolean {
        when (preference.key) {
            PreferKey.backupPath -> selectBackupPath.launch()
            PreferKey.restoreIgnore -> backupIgnore()
            "web_dav_backup" -> backup()
            "web_dav_restore" -> restore()
            PreferKey.localBackup -> backup()
            PreferKey.localRestore -> restoreFromLocal()
            "import_old" -> restoreOld.launch()
            "qreadLogin" -> loginQRead()
        }
        return super.onPreferenceTreeClick(preference)
    }

    private fun loginQRead() {
        val baseUrl = appCtx.getPrefString(qreadBaseUrl)?.trim().orEmpty().trimEnd('/')
        val username = appCtx.getPrefString(qreadUsername)?.trim().orEmpty()
        val password = appCtx.getPrefString(qreadPassword)?.trim().orEmpty()
        if (baseUrl.isBlank() || username.isBlank() || password.isBlank()) {
            appCtx.toastOnUi(R.string.qread_login_missing_required_fields)
            return
        }
        waitDialog.setText(getString(R.string.qread_login_loading))
        waitDialog.show()
        lifecycleScope.launch(IO) {
            runCatching {
                requestQReadToken(baseUrl, username, password)
            }.mapCatching { token ->
                // 同步必须使用本轮登录拿到的 token；prefs 中的 qreadToken 要等 onSuccess 才写入，否则会一直是 0
                val syncedBookSourceCount = if (token.isNullOrBlank()) {
                    0
                } else {
                    RemoteProgressBridge.syncBookSourcesFromQRead(token)
                }
                val syncedRssSourceCount = if (token.isNullOrBlank()) {
                    0
                } else {
                    RemoteProgressBridge.syncRssSourcesFromQRead(token)
                }
                Triple(token, syncedBookSourceCount, syncedRssSourceCount)
            }.onSuccess { (token, syncedBookSourceCount, syncedRssSourceCount) ->
                withContext(Main) {
                    waitDialog.dismiss()
                    if (token.isNullOrBlank()) {
                        appCtx.toastOnUi(R.string.qread_login_token_empty)
                    } else {
                        appCtx.putPrefString(qreadToken, token)
                        upPreferenceSummary(qreadToken, token)
                        lifecycleScope.launch(IO) {
                            if (AppConfig.remoteSyncMode.equals(MODE_QREAD, true) &&
                                RemoteProgressBridge.isProgressSyncEnabled()
                            ) {
                                RemoteProgressBridge.downloadAllBookProgress()
                                RemoteProgressBridge.startQReadPushIfEnabled()
                            }
                        }
                        appCtx.toastOnUi(
                            getString(
                                R.string.qread_login_and_sync_success,
                                syncedBookSourceCount,
                                syncedRssSourceCount
                            )
                        )
                    }
                }
            }.onFailure { error ->
                withContext(Main) {
                    waitDialog.dismiss()
                    appCtx.toastOnUi(
                        getString(
                            R.string.qread_login_failed,
                            error.localizedMessage ?: getString(R.string.unknown_error)
                        )
                    )
                }
            }
        }
    }

    private fun requestQReadToken(
        baseUrl: String,
        username: String,
        password: String
    ): String? {
        val loginUrl = "$baseUrl${QREAD_LOGIN_PATH_TEMPLATE.format(QREAD_API_VERSION)}"
        val body = FormBody.Builder()
            .add(QREAD_PARAM_USERNAME, username)
            .add(QREAD_PARAM_PASSWORD, password)
            .add(QREAD_PARAM_MODEL, QREAD_MODEL)
            .build()
        val request = Request.Builder()
            .url(loginUrl)
            .post(body)
            .build()
        io.legado.app.help.http.okHttpClient.newCall(request).execute().use { response ->
            val payload = response.body.string()
            if (!response.isSuccessful) {
                error("HTTP ${response.code}")
            }
            val json = JSONObject(payload)
            val isSuccess = json.optBoolean("isSuccess", false)
            if (!isSuccess) {
                error(json.optString("errorMsg", "login failed"))
            }
            val data = json.optJSONObject("data")
            val token = data?.optString("accessToken")?.takeIf { it.isNotBlank() }
            if (token.isNullOrBlank()) {
                error("accessToken is empty")
            }
            return token
        }
    }

    /**
     * 备份忽略设置
     */
    private fun backupIgnore() {
        val checkedItems = BooleanArray(BackupConfig.ignoreKeys.size) {
            BackupConfig.ignoreConfig[BackupConfig.ignoreKeys[it]] ?: false
        }
        alert(R.string.restore_ignore) {
            multiChoiceItems(BackupConfig.ignoreTitle, checkedItems) { _, which, isChecked ->
                BackupConfig.ignoreConfig[BackupConfig.ignoreKeys[which]] = isChecked
            }
            onDismiss {
                BackupConfig.saveIgnoreConfig()
            }
        }
    }


    fun backup() {
        val backupPath = AppConfig.backupPath
        if (backupPath.isNullOrEmpty()) {
            backupDir.launch()
        } else {
            if (backupPath.isContentScheme()) {
                lifecycleScope.launch {
                    val canWrite = withContext(IO) {
                        FileDoc.fromDir(backupPath).checkWrite()
                    }
                    if (canWrite) {
                        backup(backupPath)
                    } else {
                        backupDir.launch()
                    }
                }
            } else {
                backupUsePermission(backupPath)
            }
        }
    }

    private fun backup(backupPath: String) {
        waitDialog.setText("备份中…")
        waitDialog.setOnCancelListener {
            backupJob?.cancel()
        }
        waitDialog.show()
        backupJob?.cancel()
        backupJob = lifecycleScope.launch {
            try {
                Backup.backupLocked(requireContext(), backupPath)
                appCtx.toastOnUi(R.string.backup_success)
            } catch (e: Throwable) {
                ensureActive()
                AppLog.put("备份出错\n${e.localizedMessage}", e)
                appCtx.toastOnUi(
                    appCtx.getString(
                        R.string.backup_fail,
                        e.localizedMessage
                    )
                )
            } finally {
                ensureActive()
                waitDialog.dismiss()
            }
        }
    }

    private fun backupUsePermission(path: String) {
        PermissionsCompat.Builder()
            .addPermissions(*Permissions.Group.STORAGE)
            .rationale(R.string.tip_perm_request_storage)
            .onGranted {
                backup(path)
            }
            .request()
    }

    fun restore() {
        waitDialog.setText(R.string.loading)
        waitDialog.setOnCancelListener {
            restoreJob?.cancel()
        }
        waitDialog.show()
        Coroutine.async {
            restoreJob = coroutineContext[Job]
            showRestoreDialog(requireContext())
        }.onError {
            AppLog.put("恢复备份出错WebDavError\n${it.localizedMessage}", it)
            if (context == null) {
                return@onError
            }
            alert {
                setTitle(R.string.restore)
                setMessage("WebDavError\n${it.localizedMessage}\n将从本地备份恢复。")
                okButton {
                    restoreFromLocal()
                }
                cancelButton()
            }
        }.onFinally {
            waitDialog.dismiss()
        }
    }

    private suspend fun showRestoreDialog(context: Context) {
        val names = withContext(IO) {
            AppWebDav.ensureWebDavDefaultForRemoteBooks()
            AppWebDav.getBackupNames()
        }
        if (AppWebDav.isJianGuoYun && names.size > 700) {
            context.toastOnUi("由于坚果云限制列出文件数量，部分备份可能未显示，请及时清理旧备份")
        }
        if (names.isNotEmpty()) {
            currentCoroutineContext().ensureActive()
            withContext(Main) {
                context.selector(
                    title = context.getString(R.string.select_restore_file),
                    items = names
                ) { _, index ->
                    if (index in 0 until names.size) {
                        listView.post {
                            restoreWebDav(names[index])
                        }
                    }
                }
            }
        } else {
            throw NoStackTraceException("Web dav no back up file")
        }
    }

    private fun restoreWebDav(name: String) {
        waitDialog.setText("恢复中…")
        waitDialog.show()
        val task = Coroutine.async {
            AppWebDav.ensureWebDavDefaultForRemoteBooks()
            AppWebDav.restoreWebDav(name)
        }.onError {
            AppLog.put("WebDav恢复出错\n${it.localizedMessage}", it)
            appCtx.toastOnUi("WebDav恢复出错\n${it.localizedMessage}")
        }.onFinally {
            waitDialog.dismiss()
        }
        waitDialog.setOnCancelListener {
            task.cancel()
        }
    }

    private fun restoreFromLocal() {
        restoreDoc.launch {
            title = getString(R.string.select_restore_file)
            mode = HandleFileContract.FILE
            allowExtensions = arrayOf("zip")
        }
    }

    override fun onDestroyView() {
        super.onDestroyView()
        waitDialog.dismiss()
    }

}