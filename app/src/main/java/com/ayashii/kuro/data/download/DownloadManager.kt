package com.ayashii.kuro.data.download

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.ContentValues
import android.content.Context
import android.content.SharedPreferences
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import androidx.core.app.NotificationCompat
import com.ayashii.kuro.data.model.Track
import com.ayashii.kuro.data.remote.MusicRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.TimeUnit

/**
 * Downloads audio (M4A) to the **public** Music/Kuro folder so users can
 * browse and copy files from any file manager.
 *
 * Path (typical):
 *   /storage/emulated/0/Music/Kuro/{Artist} - {Title}.m4a
 *
 * Uses MediaStore on Android 10+ (no special storage permission needed to write).
 * Metadata is still tracked in SharedPreferences for the in-app Downloads tab.
 */
class DownloadManager private constructor(private val context: Context) {

    private val prefs: SharedPreferences =
        context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)

    /** App cache used only as temporary download buffer. */
    private val tempDir: File =
        File(context.cacheDir, "download_tmp").also { if (!it.exists()) it.mkdirs() }

    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .build()

    private val mutex = Mutex()
    private val activeJobs = mutableSetOf<String>()

    private val _downloadedTracks = MutableStateFlow(loadDownloadedTracks())
    val downloadedTracks: StateFlow<List<Track>> = _downloadedTracks.asStateFlow()

    /** trackId -> progress 0f..1f (or -1f when failed) */
    private val _progress = MutableStateFlow<Map<String, Float>>(emptyMap())
    val progress: StateFlow<Map<String, Float>> = _progress.asStateFlow()

    private val notificationManager =
        context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager

    init {
        createNotificationChannel()
        cleanupMissingFiles()
    }

    fun isDownloaded(trackId: String): Boolean {
        return _downloadedTracks.value.any { it.id == trackId }
    }

    fun getLocalPath(trackId: String): String? {
        return _downloadedTracks.value.find { it.id == trackId }?.localPath
    }

    /** Content URI (MediaStore) for playback when file path is unavailable. */
    fun getMediaUri(trackId: String): Uri? {
        val s = prefs.getString(KEY_URI_PREFIX + trackId, null) ?: return null
        return try {
            Uri.parse(s)
        } catch (_: Exception) {
            null
        }
    }

    fun getDownloadedTrack(trackId: String): Track? {
        return _downloadedTracks.value.find { it.id == trackId }
    }

    /**
     * Download audio-only M4A into public Music/Kuro folder.
     * Returns the updated Track with localPath on success, null on failure.
     */
    suspend fun downloadTrack(
        track: Track,
        repository: MusicRepository
    ): Track? = withContext(Dispatchers.IO) {
        if (isDownloaded(track.id)) {
            return@withContext getDownloadedTrack(track.id)
        }

        mutex.withLock {
            if (track.id in activeJobs) return@withContext null
            activeJobs.add(track.id)
        }

        updateProgress(track.id, 0f)
        showProgressNotification(track, 0, indeterminate = true)

        val tempFile = File(tempDir, "${track.id}.m4a.tmp")

        try {
            val stream = repository.getAudioStreamForDownload(track.url)
                ?: run {
                    failDownload(track, "Stream not available")
                    return@withContext null
                }

            val request = Request.Builder()
                .url(stream.url)
                .header(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"
                )
                .build()

            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    failDownload(track, "HTTP ${response.code}")
                    return@withContext null
                }

                val body = response.body ?: run {
                    failDownload(track, "Empty body")
                    return@withContext null
                }

                val contentLength = body.contentLength()
                body.byteStream().use { input ->
                    FileOutputStream(tempFile).use { output ->
                        val buffer = ByteArray(8192)
                        var totalRead = 0L
                        var lastNotifiedPercent = -1

                        while (true) {
                            val read = input.read(buffer)
                            if (read == -1) break
                            output.write(buffer, 0, read)
                            totalRead += read

                            if (contentLength > 0) {
                                val percent = ((totalRead * 100) / contentLength).toInt()
                                val progress = totalRead.toFloat() / contentLength
                                updateProgress(track.id, progress)

                                if (percent != lastNotifiedPercent && percent % 2 == 0) {
                                    lastNotifiedPercent = percent
                                    showProgressNotification(track, percent, indeterminate = false)
                                }
                            }
                        }
                        output.flush()
                    }
                }
            }

            // Move into public Music/Kuro via MediaStore (or legacy public dir)
            val saved = saveToPublicMusic(track, tempFile)
                ?: run {
                    failDownload(track, "Failed to save to public folder")
                    return@withContext null
                }

            tempFile.delete()

            val downloaded = track.copy(
                localPath = saved.path,
                streamUrl = saved.path,
                mimeType = "audio/mp4"
            )

            // Keep media store URI in prefs for reliable delete/playback
            saveDownloadedTrack(downloaded, saved.uriString)

            updateProgress(track.id, 1f)
            showCompletedNotification(track)
            downloaded
        } catch (e: Exception) {
            failDownload(track, e.message ?: "Unknown error")
            null
        } finally {
            mutex.withLock { activeJobs.remove(track.id) }
            try { tempFile.delete() } catch (_: Exception) {}
        }
    }

    suspend fun deleteDownload(trackId: String) = withContext(Dispatchers.IO) {
        val track = getDownloadedTrack(trackId) ?: return@withContext
        val uriString = prefs.getString(KEY_URI_PREFIX + trackId, null)

        // Delete via MediaStore URI if we have it
        if (!uriString.isNullOrBlank()) {
            try {
                context.contentResolver.delete(Uri.parse(uriString), null, null)
            } catch (_: Exception) {}
        }

        // Fallback: delete file path
        track.localPath?.let { path ->
            try {
                File(path).delete()
            } catch (_: Exception) {}
        }

        prefs.edit().remove(KEY_URI_PREFIX + trackId).apply()

        val updated = _downloadedTracks.value.filter { it.id != trackId }
        saveAll(updated)
        _downloadedTracks.value = updated

        val progressMap = _progress.value.toMutableMap()
        progressMap.remove(trackId)
        _progress.value = progressMap

        notificationManager.cancel(NOTIFICATION_ID_BASE + trackId.hashCode().and(0x7FFF))
    }

    fun clearProgress(trackId: String) {
        val map = _progress.value.toMutableMap()
        map.remove(trackId)
        _progress.value = map
    }

    // ── Public storage ───────────────────────────────────────────

    private data class SavedFile(val path: String, val uriString: String?)

    private fun sanitizeFileName(name: String): String {
        return name
            .replace(Regex("[\\\\/:*?\"<>|]"), "_")
            .replace(Regex("\\s+"), " ")
            .trim()
            .take(80)
            .ifBlank { "track" }
    }

    private fun buildDisplayName(track: Track): String {
        val artist = sanitizeFileName(track.artist)
        val title = sanitizeFileName(track.title)
        return "$artist - $title.m4a"
    }

    private fun saveToPublicMusic(track: Track, sourceFile: File): SavedFile? {
        val displayName = buildDisplayName(track)
        val relativePath = Environment.DIRECTORY_MUSIC + "/Kuro"

        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            // Android 10+: MediaStore → visible in Files / Music apps
            val values = ContentValues().apply {
                put(MediaStore.Audio.Media.DISPLAY_NAME, displayName)
                put(MediaStore.Audio.Media.MIME_TYPE, "audio/mp4")
                put(MediaStore.Audio.Media.RELATIVE_PATH, relativePath)
                put(MediaStore.Audio.Media.IS_PENDING, 1)
                put(MediaStore.Audio.Media.TITLE, track.title)
                put(MediaStore.Audio.Media.ARTIST, track.artist)
                if (track.durationSeconds > 0) {
                    put(MediaStore.Audio.Media.DURATION, track.durationSeconds * 1000)
                }
            }

            val collection = MediaStore.Audio.Media.getContentUri(MediaStore.VOLUME_EXTERNAL_PRIMARY)
            val resolver = context.contentResolver
            val itemUri = resolver.insert(collection, values) ?: return null

            try {
                resolver.openOutputStream(itemUri)?.use { out ->
                    sourceFile.inputStream().use { input -> input.copyTo(out) }
                } ?: run {
                    resolver.delete(itemUri, null, null)
                    return null
                }

                values.clear()
                values.put(MediaStore.Audio.Media.IS_PENDING, 0)
                resolver.update(itemUri, values, null, null)

                // Resolve real filesystem path when possible (for ExoPlayer file:// and user copy)
                val path = queryMediaStorePath(itemUri)
                    ?: "/storage/emulated/0/$relativePath/$displayName"

                SavedFile(path = path, uriString = itemUri.toString())
            } catch (e: Exception) {
                try { resolver.delete(itemUri, null, null) } catch (_: Exception) {}
                null
            }
        } else {
            // Android 8–9: direct public Music folder (needs WRITE_EXTERNAL_STORAGE)
            val musicRoot = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MUSIC)
            val kuroDir = File(musicRoot, "Kuro").also { if (!it.exists()) it.mkdirs() }
            val dest = File(kuroDir, displayName)
            sourceFile.copyTo(dest, overwrite = true)
            // Notify media scanner
            try {
                val values = ContentValues().apply {
                    put(MediaStore.Audio.Media.DATA, dest.absolutePath)
                    put(MediaStore.Audio.Media.MIME_TYPE, "audio/mp4")
                    put(MediaStore.Audio.Media.TITLE, track.title)
                    put(MediaStore.Audio.Media.ARTIST, track.artist)
                }
                val uri = context.contentResolver.insert(
                    MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                    values
                )
                SavedFile(path = dest.absolutePath, uriString = uri?.toString())
            } catch (_: Exception) {
                SavedFile(path = dest.absolutePath, uriString = null)
            }
        }
    }

    private fun queryMediaStorePath(uri: Uri): String? {
        return try {
            context.contentResolver.query(
                uri,
                arrayOf(MediaStore.Audio.Media.DATA),
                null,
                null,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val idx = cursor.getColumnIndex(MediaStore.Audio.Media.DATA)
                    if (idx >= 0) cursor.getString(idx) else null
                } else null
            }
        } catch (_: Exception) {
            null
        }
    }

    private fun failDownload(track: Track, reason: String) {
        updateProgress(track.id, -1f)
        showFailedNotification(track, reason)
        File(tempDir, "${track.id}.m4a.tmp").delete()
    }

    private fun updateProgress(trackId: String, value: Float) {
        val map = _progress.value.toMutableMap()
        map[trackId] = value
        _progress.value = map
    }

    // ── Persistence ──────────────────────────────────────────────

    private fun loadDownloadedTracks(): List<Track> {
        val json = prefs.getString(KEY_DOWNLOADS, null) ?: return emptyList()
        return try {
            val arr = JSONArray(json)
            buildList {
                for (i in 0 until arr.length()) {
                    val o = arr.getJSONObject(i)
                    val path = o.optString("localPath", "")
                    val exists = path.isNotBlank() && (
                        File(path).exists() ||
                            !prefs.getString(KEY_URI_PREFIX + o.optString("id"), null).isNullOrBlank()
                        )
                    if (path.isNotBlank() && exists) {
                        add(
                            Track(
                                id = o.getString("id"),
                                url = o.optString("url", ""),
                                title = o.optString("title", "Unknown"),
                                artist = o.optString("artist", "Unknown"),
                                thumbnailUrl = o.optString("thumbnailUrl", ""),
                                durationSeconds = o.optLong("durationSeconds", 0),
                                localPath = path,
                                streamUrl = path,
                                mimeType = "audio/mp4",
                                isFavorite = o.optBoolean("isFavorite", false)
                            )
                        )
                    }
                }
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    @Synchronized
    private fun saveDownloadedTrack(track: Track, uriString: String? = null) {
        if (!uriString.isNullOrBlank()) {
            prefs.edit().putString(KEY_URI_PREFIX + track.id, uriString).apply()
        }
        val current = _downloadedTracks.value.toMutableList()
        val idx = current.indexOfFirst { it.id == track.id }
        if (idx >= 0) current[idx] = track else current.add(0, track)
        saveAll(current)
        _downloadedTracks.value = current
    }

    private fun saveAll(list: List<Track>) {
        val arr = JSONArray()
        list.forEach { t ->
            arr.put(
                JSONObject().apply {
                    put("id", t.id)
                    put("url", t.url)
                    put("title", t.title)
                    put("artist", t.artist)
                    put("thumbnailUrl", t.thumbnailUrl)
                    put("durationSeconds", t.durationSeconds)
                    put("localPath", t.localPath ?: "")
                    put("isFavorite", t.isFavorite)
                }
            )
        }
        prefs.edit().putString(KEY_DOWNLOADS, arr.toString()).apply()
    }

    private fun cleanupMissingFiles() {
        val valid = _downloadedTracks.value.filter { t ->
            val pathOk = t.localPath != null && File(t.localPath).exists()
            val uriOk = !prefs.getString(KEY_URI_PREFIX + t.id, null).isNullOrBlank()
            pathOk || uriOk
        }
        if (valid.size != _downloadedTracks.value.size) {
            saveAll(valid)
            _downloadedTracks.value = valid
        }
    }

    // ── Notifications ────────────────────────────────────────────

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Downloads",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Music download progress"
                setShowBadge(false)
            }
            notificationManager.createNotificationChannel(channel)
        }
    }

    private fun notifId(trackId: String): Int =
        NOTIFICATION_ID_BASE + trackId.hashCode().and(0x7FFF)

    private fun showProgressNotification(track: Track, percent: Int, indeterminate: Boolean) {
        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle("Downloading")
            .setContentText(track.title)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)

        if (indeterminate) {
            builder.setProgress(0, 0, true)
        } else {
            builder.setProgress(100, percent, false)
            builder.setSubText("$percent%")
        }

        notificationManager.notify(notifId(track.id), builder.build())
    }

    private fun showCompletedNotification(track: Track) {
        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle("Download complete")
            .setContentText("${track.title} → Music/Kuro")
            .setSmallIcon(android.R.drawable.stat_sys_download_done)
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
        notificationManager.notify(notifId(track.id), builder.build())
    }

    private fun showFailedNotification(track: Track, reason: String) {
        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle("Download failed")
            .setContentText("${track.title}: $reason")
            .setSmallIcon(android.R.drawable.stat_notify_error)
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
        notificationManager.notify(notifId(track.id), builder.build())
    }

    companion object {
        private const val PREF_NAME = "kuro_downloads"
        private const val KEY_DOWNLOADS = "downloaded_tracks"
        private const val KEY_URI_PREFIX = "media_uri_"
        private const val CHANNEL_ID = "kuro_downloads"
        private const val NOTIFICATION_ID_BASE = 42000

        @Volatile
        private var INSTANCE: DownloadManager? = null

        fun getInstance(context: Context): DownloadManager {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: DownloadManager(context.applicationContext).also { INSTANCE = it }
            }
        }
    }
}
