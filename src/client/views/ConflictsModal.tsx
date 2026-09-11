import { ipcRenderer, shell } from "electron"
import React, { FC } from "react"
import { useTranslation } from "react-i18next"

import { SyncConflict } from "../../modules/sync-state"
import { Modal } from "../components/Modal"

export const ConflictsModal: FC<{
  conflicts: SyncConflict[]
  onClose: () => void
  onResolved: (conflicts: SyncConflict[]) => void
}> = props => {
  const { t } = useTranslation("client", { keyPrefix: "conflicts" })

  const resolve = async (id: string, resolution: "keep-local" | "use-remote") =>
    props.onResolved(
      await ipcRenderer.invoke("resolve-conflict", id, resolution),
    )

  return (
    <Modal
      title={t("title", { count: props.conflicts.length })}
      onClose={props.onClose}
    >
      <div className="new-files-modal">
        {props.conflicts.map(conflict => (
          <div className="new-files" key={conflict.id}>
            <h3>{conflict.relativePath}</h3>
            <button
              className="confirm-button"
              onClick={() => shell.showItemInFolder(conflict.incomingPath)}
            >
              {t("revealIncoming")}
            </button>
            <button
              className="confirm-button"
              onClick={() => resolve(conflict.id, "keep-local")}
            >
              {t("keepLocal")}
            </button>
            <button
              className="confirm-button"
              onClick={() => resolve(conflict.id, "use-remote")}
            >
              {t("useRemote")}
            </button>
          </div>
        ))}
      </div>
    </Modal>
  )
}
