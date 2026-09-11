import crypto from "crypto"
import fs from "fs/promises"
import path from "path"
import { app } from "electron"

export interface SyncEntry {
  remoteId: string
  relativePath: string
  baseSha256: string
  remoteRevision: string
}

export interface SyncConflict {
  id: string
  remoteId: string
  relativePath: string
  incomingPath: string
  baseSha256?: string
  localSha256?: string
  remoteSha256: string
  remoteRevision: string
  detectedAt: number
}

interface SyncRoot {
  canonicalPath: string
  entries: Record<string, SyncEntry>
  conflicts: Record<string, SyncConflict>
}

interface SyncStateData {
  schemaVersion: 1
  roots: Record<string, SyncRoot>
}

const emptyState = (): SyncStateData => ({ schemaVersion: 1, roots: {} })

export function sha256(value: string) {
  return crypto.createHash("sha256").update(value).digest("hex")
}

export async function hashFile(file: string) {
  const data = await fs.readFile(file)
  return crypto.createHash("sha256").update(data).digest("hex")
}

export function safeRelativePath(relativePath: string) {
  const normalized = path.normalize(relativePath)
  return (
    normalized !== "." &&
    !path.isAbsolute(normalized) &&
    normalized !== ".." &&
    !normalized.startsWith(`..${path.sep}`)
  )
}

export function resolveInside(root: string, relativePath: string) {
  if (!safeRelativePath(relativePath)) throw new Error("Unsafe relative path")
  const target = path.resolve(root, relativePath)
  const normalizedRoot = path.resolve(root)
  if (!target.startsWith(normalizedRoot + path.sep))
    throw new Error("Path escapes download folder")
  return target
}

export class SyncState {
  private data: SyncStateData = emptyState()
  private initialized = false
  private readonly statePath: string

  constructor(
    statePath = path.join(app.getPath("userData"), "sync-state.json"),
  ) {
    this.statePath = statePath
  }

  async init() {
    if (this.initialized) return
    try {
      const parsed = JSON.parse(await fs.readFile(this.statePath, "utf8"))
      if (parsed?.schemaVersion === 1 && parsed.roots) this.data = parsed
    } catch (e) {
      if (e.code !== "ENOENT") {
        const backup = `${this.statePath}.corrupt-${Date.now()}`
        await fs.rename(this.statePath, backup).catch(() => {})
      }
    }
    this.initialized = true
  }

  private async root(downloadPath: string) {
    await this.init()
    const canonicalPath = await fs
      .realpath(downloadPath)
      .catch(() => path.resolve(downloadPath))
    const key = sha256(canonicalPath)
    if (!this.data.roots[key])
      this.data.roots[key] = { canonicalPath, entries: {}, conflicts: {} }
    return this.data.roots[key]
  }

  async entry(downloadPath: string, remoteId: string) {
    return (await this.root(downloadPath)).entries[remoteId]
  }

  async upsertEntry(downloadPath: string, entry: SyncEntry) {
    ;(await this.root(downloadPath)).entries[entry.remoteId] = entry
    await this.write()
  }

  async addConflict(downloadPath: string, conflict: SyncConflict) {
    ;(await this.root(downloadPath)).conflicts[conflict.id] = conflict
    await this.write()
  }

  async conflicts(downloadPath: string) {
    return Object.values((await this.root(downloadPath)).conflicts).sort(
      (a, b) => b.detectedAt - a.detectedAt,
    )
  }

  async hasConflict(
    downloadPath: string,
    remoteId: string,
    remoteRevision: string,
  ) {
    return Object.values((await this.root(downloadPath)).conflicts).some(
      conflict =>
        conflict.remoteId === remoteId &&
        conflict.remoteRevision === remoteRevision,
    )
  }

  async resolveConflict(
    downloadPath: string,
    id: string,
    resolution: "keep-local" | "use-remote",
  ) {
    const root = await this.root(downloadPath)
    const conflict = root.conflicts[id]
    if (!conflict) return

    const target = resolveInside(root.canonicalPath, conflict.relativePath)
    const incoming = path.resolve(conflict.incomingPath)
    const conflictsDir = path.resolve(
      root.canonicalPath,
      ".webeep-sync-conflicts",
    )
    if (!incoming.startsWith(conflictsDir + path.sep))
      throw new Error("Invalid conflict path")

    if (resolution === "use-remote") {
      await fs.mkdir(path.dirname(target), { recursive: true })
      await fs.rename(incoming, target)
    } else {
      await fs.rm(incoming, { force: true })
    }

    root.entries[conflict.remoteId] = {
      remoteId: conflict.remoteId,
      relativePath: conflict.relativePath,
      baseSha256: conflict.remoteSha256,
      remoteRevision: conflict.remoteRevision,
    }
    delete root.conflicts[id]
    await this.write()
    return conflict
  }

  private async write() {
    await fs.mkdir(path.dirname(this.statePath), { recursive: true })
    const tempPath = `${this.statePath}.${process.pid}.${Date.now()}.tmp`
    await fs.writeFile(tempPath, JSON.stringify(this.data), { mode: 0o600 })
    await fs.rename(tempPath, this.statePath)
  }
}

export const syncState = new SyncState()
