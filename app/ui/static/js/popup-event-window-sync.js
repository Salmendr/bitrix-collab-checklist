(function (global) {
    'use strict';

    async function sendFolderReplacementSummaryToBitrix(
        summaryData,
        sourceWindow
    ) {
        const summaryId = String(
            summaryData && summaryData.summaryId || ''
        ).trim();
        const itemId = String(
            summaryData && summaryData.itemId || ''
        ).trim();
        const replacements = Array.isArray(
            summaryData && summaryData.replacements
        )
            ? summaryData.replacements
            : [];

        // Stage 7.2.4: external messages are never sent from an
        // intermediate folder event. Individual replacement changes are
        // already merged into the active popup session by the
        // checklist-document-replaced event and are delivered only after
        // the local edit-session commit has completed and released locks.
        debugLog('replacement_summary_deferred_until_commit', {
            summaryId,
            itemId,
            replacementsCount: replacements.length,
            editSessionId: getActiveEditSessionId()
        });

        try {
            if (
                sourceWindow
                && typeof sourceWindow.postMessage === 'function'
            ) {
                sourceWindow.postMessage({
                    type: (
                        'checklist-document-'
                        + 'replacement-summary-result'
                    ),
                    dialogId,
                    checklistKey: String(
                        summaryData && summaryData.checklistKey
                        || currentChecklistKey
                        || 'id'
                    ),
                    itemId,
                    summaryId,
                    ok: true,
                    deferredUntilCommit: true,
                    error: ''
                }, '*');
            }
        } catch (error) {
            console.log(
                'replacement summary deferred response error:',
                error
            );
        }
    }


    async function handlePopupWindowMessage(event) {
        const data = event && event.data ? event.data : {};
        const messageType = String(data && data.type || '');

        const supportedMessageTypes = [
            'checklist-document-removed',
            'checklist-document-uploaded',
            'checklist-document-replaced',
            'checklist-document-changed',
            'checklist-document-replacement-summary'
        ];

        if (!supportedMessageTypes.includes(messageType)) return;
        if (String(data.dialogId || '') !== String(dialogId || '')) return;

        if (
            messageType
            === 'checklist-document-replacement-summary'
        ) {
            await sendFolderReplacementSummaryToBitrix(
                data,
                event.source
            );
            return;
        }

        const sourceChecklistKey = String(
            data.checklistKey || ''
        ).trim() || 'id';

        const sourceItemId = String(
            data.itemId || ''
        ).trim();

        const existingState = sourceChecklistKey === currentChecklistKey
            ? {
                changes: deepClone(sessionChanges),
                dirty: !!sessionDirty
            }
            : deepClone(
                getChecklistState(sourceChecklistKey)
            );

        const nextChanges = Array.isArray(
            existingState && existingState.changes
        )
            ? existingState.changes
            : [];

        let externalChangeAdded = false;

        try {
            const response = await fetch(
                appUrl('api/checklist')
                + '?dialogId=' + encodeURIComponent(dialogId)
                + '&checklistKey=' + encodeURIComponent(
                    sourceChecklistKey
                )
            );

            const result = await response.json();

            if (!response.ok) {
                throw new Error(
                    result.error
                    || 'reload after folder sync failed'
                );
            }

            const refreshedItems = Array.isArray(result.items)
                ? result.items
                : [];

            const refreshedItem = refreshedItems.find(item => (
                String(item && item.id || '')
                === sourceItemId
            ));

            const itemName = String(
                data.itemName
                || refreshedItem && refreshedItem.name
                || ''
            ).trim();

            if (messageType === 'checklist-document-removed') {
                const documentName = String(
                    data.documentName || 'Файл'
                ).trim() || 'Файл';

                nextChanges.push({
                    field: 'document',
                    itemId: sourceItemId,
                    itemName,
                    oldValue: documentName,
                    newValue: 'Удален'
                });

                externalChangeAdded = true;
            }

            if (messageType === 'checklist-document-uploaded') {
                const fileNames = Array.isArray(data.fileNames)
                    ? data.fileNames
                    : [];

                fileNames.forEach(fileName => {
                    const normalizedName = String(
                        fileName || ''
                    ).trim();

                    if (!normalizedName) return;

                    nextChanges.push({
                        field: 'document',
                        itemId: sourceItemId,
                        itemName,
                        oldValue: '',
                        newValue: normalizedName
                    });

                    externalChangeAdded = true;
                });
            }

            if (messageType === 'checklist-document-replaced') {
                const replacement = (
                    data.replacement
                    && typeof data.replacement === 'object'
                        ? data.replacement
                        : {}
                );
                const oldName = String(
                    replacement.oldDocumentName
                    || 'Файл'
                ).trim() || 'Файл';
                const newName = String(
                    replacement.newDocumentName
                    || data.documentName
                    || 'Файл'
                ).trim() || 'Файл';

                nextChanges.push({
                    field: 'document-replacement',
                    itemId: sourceItemId,
                    itemName,
                    oldValue: oldName,
                    newValue: newName,
                    operationId: String(
                        replacement.operationId || ''
                    ).trim(),
                    archiveVersion: Number(
                        replacement.archiveVersion || 0
                    ),
                    archiveVersionLabel: String(
                        replacement.archiveVersionLabel || ''
                    ).trim(),
                    seriesId: String(
                        replacement.seriesId || ''
                    ).trim()
                });

                externalChangeAdded = true;
            }

            const localSnapshot = (
                sourceChecklistKey === currentChecklistKey
                    ? buildChecklistSnapshot()
                    : deepClone(
                        checklistCache[sourceChecklistKey]
                        || result
                    )
            );

            const mergedChecklist = (
                mergeDocumentRefreshSnapshot(
                    localSnapshot,
                    result,
                    sourceItemId,
                    existingState.changes
                )
            );

            checklistCache[sourceChecklistKey] = deepClone(
                mergedChecklist
            );

            checklistSessionState[sourceChecklistKey] = {
                changes: nextChanges,
                dirty: (
                    !!existingState.dirty
                    || externalChangeAdded
                )
            };

            if (sourceChecklistKey === currentChecklistKey) {
                applyChecklistData(
                    deepClone(mergedChecklist)
                );
                renderAll();
            }

            debugLog(
                'folder_document_sync_applied',
                {
                    messageType,
                    sourceChecklistKey,
                    sourceItemId,
                    currentChecklistKey,
                    externalChangeAdded,
                    managedByUploadManager: !!(
                        data.managedByUploadManager
                    ),
                    rendered: (
                        sourceChecklistKey
                        === currentChecklistKey
                    )
                }
            );

        } catch (e) {
            console.log('folder sync error:', e);

            debugLog(
                'folder_document_sync_failed',
                {
                    messageType,
                    sourceChecklistKey,
                    sourceItemId,
                    error: String(
                        e && e.message || e
                    )
                }
            );
        }
    }

    global.addEventListener(
        'message',
        handlePopupWindowMessage
    );

    global.ChecklistPopupWindowSyncEvents = Object.freeze({
        handleMessage: handlePopupWindowMessage,
        sendReplacementSummary: sendFolderReplacementSummaryToBitrix
    });
})(window);
