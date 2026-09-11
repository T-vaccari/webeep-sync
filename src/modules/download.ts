import path from "path"
import fs from "fs/promises"
import { createWriteStream } from "fs"
import stream from "stream/promises"
import { EventEmitter } from "events"
import got, { HTTPError } from "got"

import { createLogger } from "./logger"
import { FileInfo, moodleClient } from "./moodle"
import { storeIsReady, store } from "./store"
import { loginManager } from "./login"

import { DownloadState, SyncResult } from "../util"
import { hashFile, resolveInside, syncState, SyncConflict } from "./sync-state"

const { log, error, debug } = createLogger("DownloadManager")

export interface FileProgress {
  filename: string
  absolutePath: string
  downloaded: number
  total: number
}

export type Progress = {
  downloaded: number
  total: number
  files: FileProgress[]
}

export type NewFilesList = {
  [course: string]: {
    filename: string
    absolutePath: string
    filesize: number
    updated: boolean
  }[]
}

export type ConflictList = SyncConflict[]

// just use this error to encapsulate all errors that can happen while writing a file to disk
class FSError extends Error {
  constructor() {
    super()
    this.name = "FSError"
  }
}

export declare interface DownloadManager {
  on(event: "sync", listener: () => void): this
  on(event: "stop", listener: (result: SyncResult) => void): this
  on(event: "state", listener: (state: DownloadState) => void): this
  on(event: "new-files", listener: (files: NewFilesList) => void): this
  on(event: "conflicts", listener: (conflicts: ConflictList) => void): this
}

export class DownloadManager extends EventEmitter {
  private stopped = false
  syncing = false

  private total = 0 // total to be downloaded
  private totalUntilNow = 0 // size of all completed downloads

  currentDownloads: {
    cancel: () => void
    progress: FileProgress
  }[] = []

  currentState: DownloadState = DownloadState.idle

  constructor() {
    super()
    storeIsReady().then(() => {
      setTimeout(
        () => {
          const autosync = () => {
            if (!store.data.settings.autosyncEnabled) return
            if (!store.data.persistence.lastSynced) return
            const dt = Date.now() - (store.data.persistence.lastSynced ?? 0)
            if (dt > store.data.settings.autosyncInterval && !this.syncing) {
              log("Scheduled autosync beginning!")
              this.sync()
            }
          }
          setInterval(() => autosync(), 60000) // try autosync every minute
          autosync()
        },
        60000 - (Date.now() % 60000), // align the timer with the tick of the minute
      )
    })
  }

  /**
   * internally updates the state and emits a state event used by the app to track sync progress
   * @param newState the new state
   */
  private updateState(newState: DownloadState) {
    this.emit("state", newState)
    debug("new state: " + DownloadState[newState])
    this.currentState = newState
  }

  private cancelAllRequests() {
    for (const download of this.currentDownloads) download.cancel()
  }

  /**
   * stops the sync, should be called externally (i.e, with the click on the "stop" button in the
   * frontend) to cancel all current requests end terminate syncing.
   *
   * This function does not directly emit the "stopped" {@link SyncResult}, as the correct status
   * will be returned by the {@link sync} function (the stopped state will bubble as the next TODO)
   */
  stop(): void {
    if (!this.stopped) {
      this.stopped = true
      this.cancelAllRequests()
    }
  }

  /**
   * The "Sync" in WeBeep Sync. Starts the syncing progress, and takes care of a bunch of side
   * effects.
   *
   * When called, if no sync is already in progress, sets {@link syncing} to true, emits the 'sync'
   * event and starts the download process, then writes the last synced time to store, updates the
   * {@link currentState} and emits the 'stop' event with the correct {@link SyncResult}
   * @returns A promise wich resolves to true if the sync was succesful, false if it is
   * interrupted for whatever reason (subscirbe to the 'stop' event to get the sync result)
   */
  async sync(): Promise<boolean> {
    if (this.syncing) return false
    log("started syncing")
    this.syncing = true
    this.emit("sync")

    const result = await this._sync()
    log(`finished syncing with result:  ${SyncResult[result]}`)

    if (
      result === SyncResult.success ||
      result === SyncResult.successWithConflicts
    ) {
      // update the last synced timestamp only if the sync was successful
      store.data.persistence.lastSynced = Date.now()
      store.write()
    }
    this.syncing = false
    this.updateState(DownloadState.idle)
    this.emit("stop", result)
    return (
      result === SyncResult.success ||
      result === SyncResult.successWithConflicts
    )
  }

  private async _sync(): Promise<SyncResult> {
    await storeIsReady() // just to be sure that the settings are initialized
    const { downloadPath } = store.data.settings
    this.stopped = false
    try {
      const files = await this.getFilesToDownload()

      this.updateState(DownloadState.downloading)
      const newFilesList: NewFilesList = {}
      const conflicts: ConflictList = []
      this.currentDownloads = []
      this.total = files.reduce((tot, f) => tot + f.filesize, 0)
      this.totalUntilNow = 0

      for (const file of files) {
        if (this.stopped) return SyncResult.stopped
        const existingEntry = await syncState.entry(downloadPath, file.remoteId)
        const result = await this.syncFile(file, downloadPath)
        if (result.conflict) conflicts.push(result.conflict)
        if (!result.downloaded) continue

        if (!newFilesList[file.coursename]) newFilesList[file.coursename] = []
        newFilesList[file.coursename].push({
          filename: file.filename,
          absolutePath: result.absolutePath,
          filesize: file.filesize,
          updated: Boolean(existingEntry),
        })
        this.totalUntilNow += file.filesize
      }

      this.emit("new-files", newFilesList)
      if (conflicts.length) this.emit("conflicts", conflicts)

      if (this.stopped) return SyncResult.stopped
      return conflicts.length
        ? SyncResult.successWithConflicts
        : SyncResult.success
    } catch (e) {
      // other request chains other than the one which threw the error need to be stopped
      this.cancelAllRequests()
      switch (e.name) {
        case "CancelError":
        case "AbortError":
          return SyncResult.stopped

        case "RequestError":
        case "HTTPError":
        case "TimeoutError":
          return SyncResult.networkError

        case "FSError":
          return SyncResult.fsError

        default:
          error("An unkown error occured on a sync attempt:")
          error(`Current state: ${DownloadState[this.currentState]}`)
          error(e)
          return SyncResult.unknownError
      }
    }
  }

  private revision(file: FileInfo) {
    return `${file.timemodified}:${file.filesize}:${file.fileurl}`
  }

  private async syncFile(file: FileInfo, downloadPath: string) {
    const relativePath = path.join(file.filepath, file.filename)
    const absolutePath = resolveInside(downloadPath, relativePath)
    const previous = await syncState.entry(downloadPath, file.remoteId)
    const localSha256 = await hashFile(absolutePath).catch(
      (): undefined => undefined,
    )
    const staged = await this.downloadToStage(file, absolutePath)
    if (!staged) return { downloaded: false, absolutePath }

    const remoteSha256 = await hashFile(staged)
    const remoteRevision = this.revision(file)
    const saveEntry = async () =>
      syncState.upsertEntry(downloadPath, {
        remoteId: file.remoteId,
        relativePath,
        baseSha256: remoteSha256,
        remoteRevision,
      })

    if (!previous) {
      if (!localSha256) {
        await fs.rename(staged, absolutePath)
        await saveEntry()
        return { downloaded: true, absolutePath }
      }
      if (localSha256 === remoteSha256) {
        await fs.rm(staged, { force: true })
        await saveEntry()
        return { downloaded: false, absolutePath }
      }
    } else if (!localSha256 && remoteSha256 === previous.baseSha256) {
      await fs.rename(staged, absolutePath)
      await saveEntry()
      return { downloaded: true, absolutePath }
    } else if (remoteSha256 === previous.baseSha256) {
      await fs.rm(staged, { force: true })
      await saveEntry()
      return { downloaded: false, absolutePath }
    } else if (localSha256 === previous.baseSha256) {
      await fs.rename(staged, absolutePath)
      await saveEntry()
      return { downloaded: true, absolutePath }
    }

    const incomingPath = resolveInside(
      downloadPath,
      path.join(
        ".webeep-sync-conflicts",
        `${Date.now()}-${file.remoteId.replace(/[^a-zA-Z0-9]/g, "_")}`,
        relativePath,
      ),
    )
    await fs.mkdir(path.dirname(incomingPath), { recursive: true })
    await fs.rename(staged, incomingPath)
    const conflict: SyncConflict = {
      id: `${file.remoteId}:${remoteRevision}`,
      remoteId: file.remoteId,
      relativePath,
      incomingPath,
      baseSha256: previous?.baseSha256,
      localSha256,
      remoteSha256,
      remoteRevision,
      detectedAt: Date.now(),
    }
    await syncState.addConflict(downloadPath, conflict)
    return { downloaded: false, absolutePath, conflict }
  }

  private async downloadToStage(file: FileInfo, absolutePath: string) {
    const fullpath = path.join(file.filepath, file.filename)
    const staged = `${absolutePath}.webeep-sync-${process.pid}-${Date.now()}.tmp`
    const controller = new AbortController()
    const request = got.stream(file.fileurl, {
      searchParams: { token: loginManager.token },
    })
    const download: (typeof this.currentDownloads)[number] = {
      cancel: () => controller.abort(),
      progress: {
        absolutePath,
        filename: file.filename,
        downloaded: 0,
        total: file.filesize,
      },
    }
    this.currentDownloads.push(download)
    request.on(
      "downloadProgress",
      ({ transferred }) => (download.progress.downloaded = transferred),
    )
    try {
      await fs.mkdir(path.dirname(absolutePath), { recursive: true })
      await stream.pipeline(
        request,
        createWriteStream(staged, { flags: "wx" }),
        {
          signal: controller.signal,
        },
      )
      await fs.utimes(staged, new Date(), new Date(file.timemodified * 1000))
      return staged
    } catch (e) {
      await fs.rm(staged, { force: true }).catch(() => {})
      if (
        e.name === "HTTPError" &&
        (e as HTTPError).response?.statusCode === 404
      ) {
        error(`Ignored missing file (404 Not Found): ${fullpath}`)
        return
      }
      switch (e.name) {
        case "AbortError":
        case "RequestError":
        case "HTTPError":
        case "TimeoutError":
          throw e
      }
      error("An error occured while writing a file to disk:")
      error(e)
      throw new FSError()
    } finally {
      const idx = this.currentDownloads.indexOf(download)
      if (idx !== -1) this.currentDownloads.splice(idx, 1)
    }
  }

  /**
   * constructs the progress object, calculating how download progress until now
   * @returns The Progress object that needs to be sent to the frontend
   */
  getCurrentProgress(): Progress | null {
    if (!this.currentDownloads.length) return null

    return {
      total: this.total,
      downloaded: this.currentDownloads.reduce(
        (t, d) => t + d.progress.downloaded,
        this.totalUntilNow,
      ),
      files: this.currentDownloads.map(d => d.progress),
    }
  }

  /**
   * Gets all files that need to be downloaded, gets all courses from the moodle API and for each
   * file gets all files. The manifest determines whether the remote revision needs processing.
   * @returns A promise that resolves to an array of FileInfos with the files that need to be downloaded
   */
  async getFilesToDownload(): Promise<FileInfo[]> {
    this.updateState(DownloadState.fetchingCourses)
    const cs = await moodleClient.getCoursesWithoutCache()

    const filesToDownload: FileInfo[] = []
    const { courses } = store.data.persistence
    const { downloadPath } = store.data.settings

    this.updateState(DownloadState.fetchingFiles)

    // get files for all courses in parallel, otherwise it takes a shit ton of time
    const syncableCourses = cs.filter(c => courses[c.id].shouldSync)
    const courseFiles = await Promise.all(
      syncableCourses.map(c => moodleClient.getFileInfos(c)),
    )

    for (const files of courseFiles)
      for (const file of files) {
        const previous = await syncState.entry(downloadPath, file.remoteId)
        const target = resolveInside(
          downloadPath,
          path.join(file.filepath, file.filename),
        )
        const missingLocalFile = await fs
          .stat(target)
          .then(() => false)
          .catch(() => true)
        if (
          !previous ||
          missingLocalFile ||
          (previous.remoteRevision !== this.revision(file) &&
            !(await syncState.hasConflict(
              downloadPath,
              file.remoteId,
              this.revision(file),
            )))
        )
          filesToDownload.push(file)
      }

    return filesToDownload
  }

  async setAutosync(sync: boolean): Promise<void> {
    await storeIsReady()
    store.data.settings.autosyncEnabled = sync
    store.write()
  }

  async getConflicts() {
    await storeIsReady()
    return syncState.conflicts(store.data.settings.downloadPath)
  }

  async resolveConflict(id: string, resolution: "keep-local" | "use-remote") {
    await storeIsReady()
    return syncState.resolveConflict(
      store.data.settings.downloadPath,
      id,
      resolution,
    )
  }
}

/**
 * Module used to handle the actual syncing process, the download and writing for each file
 */
export const downloadManager = new DownloadManager()
