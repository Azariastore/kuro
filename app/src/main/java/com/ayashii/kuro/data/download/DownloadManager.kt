package com.ayashii.kuro.data.download

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.SharedPreferences
import android.os.Build
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
 * Manages offline audio downloads (M4A) into app-private internal storage.
 * Files live under filesDir/music_downloads/{trackId}.m4a
 * Metadata is persisted in SharedPreferences.
 */
class DownloadManager private constructor(private val context: Context) {

    private val prefs: SharedPreferences =
        context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)

    private val downloadDir: File =
        File(context.filesDir, "music_downloads").also { if (!it.exists()) it.mkdirs() }

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
        // Clean up orphaned metadata (file missing)
        cleanupMissingFiles()
    }

    fun isDownloaded(trackId: String): Boolean {
        return _downloadedTracks.value.any { it.id == trackId }
    }

    fun getLocalPath(trackId: String): String? {
        return _downloadedTracks.value.find { it.id == trackId }?.localPath
    }

    fun getDownloadedTrack(trackId: String): Track? {
        return _downloadedTracks.value.find { it.id == trackId }
    }

    /**
     * Download audio-only M4A (prefer ~256 kbps) into internal storage.
     * Returns the updated Track with localPath on success, null on failure/cancel.
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

        try {
            // Prefer highest available M4A (targets 256 kbps when present)
            val stream = repository.getAudioStreamForDownload(track.url)
                ?: run {
                    failDownload(track, "Stream not available")
                    return@withContext null
                }

            val targetFile = File(downloadDir, "${track.id}.m4a")
            val tempFile = File(downloadDir, "${track.id}.m4a.tmp")

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

                                // Throttle notification updates
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

            // Atomic rename
            if (targetFile.exists()) targetFile.delete()
            if (!tempFile.renameTo(targetFile)) {
                tempFile.copyTo(targetFile, overwrite = true)
                tempFile.delete()
            }

            val downloaded = track.copy(
                localPath = targetFile.absolutePath,
                streamUrl = targetFile.absolutePath,
                mimeType = "audio/mp4"
            )

            saveDownloadedTrack(downloaded)
            updateProgress(track.id, 1f)
            showCompletedNotification(track)
            downloaded
        } catch (e: Exception) {
            failDownload(track, e.message ?: "Unknown error")
            null
        } finally {
            mutex.withLock { activeJobs.remove(track.id) }
            // Clear progress after a short delay is handled by caller if needed
        }
    }

    suspend fun deleteDownload(trackId: String) = withContext(Dispatchers.IO) {
        val track = getDownloadedTrack(trackId) ?: return@withContext
        track.localPath?.let { path ->
            try {
                File(path).delete()
            } catch (_: Exception) {}
        }
        File(downloadDir, "$trackId.m4a").delete()
        File(downloadDir, "$trackId.m4a.tmp").delete()

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

    private fun failDownload(track: Track, reason: String) {
        updateProgress(track.id, -1f)
        showFailedNotification(track, reason)
        File(downloadDir, "${track.id}.m4a.tmp").delete()
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
                    if (path.isNotBlank() && File(path).exists()) {
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
    private fun saveDownloadedTrack(track: Track) {
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
            t.localPath != null && File(t.localPath).exists()
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
            .setContentText(track.title)
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
