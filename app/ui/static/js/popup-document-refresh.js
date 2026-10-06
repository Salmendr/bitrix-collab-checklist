'use strict';

(function (global) {
    const DOCUMENT_FIELDS = Object.freeze([
        'folderKey',
        'folderPath',
        'folderUrl',
        'yandexFolderStatus',
        'yandexFolderError',
        'yandexFolderPath',
        'yandexFolderUrl',
        'yandexFolderTargetPath',
        'yandexStructureJobId',
        'yandexStructureAction',
        'yandexStructureUpdatedAt',
        'documents',
        'subfolders',
        'archivedDocumentSeries',
        'documentUrl',
        'documentName'
    ]);

    const STATUS_FIELDS = Object.freeze([
        'status',
        'priority',
        'group',
        'order'
    ]);

    // Where an item sits: the server applies every move at once.
    const STRUCTURE_FIELDS = Object.freeze([
        'group',
        'order',
        'parentItemId',
        'notRequiredReturnGroupId',
        'notRequiredReturnParentId'
    ]);

    function clone(value) {
        if (value === undefined) {
            return undefined;
        }
        return JSON.parse(JSON.stringify(value));
    }

    function normalizeId(value) {
        return String(value || '').trim();
    }

    function normalizeStatusValue(value) {
        return String(value || '').trim();
    }

    function isAutomaticStatusChange(change) {
        return !!(
            change
            && (
                change.automatic === true
                || String(change.source || '').trim() === 'upload-auto'
            )
        );
    }

    function getLastManualStatusChange(changes, itemId) {
        const targetId = normalizeId(itemId);
        const source = Array.isArray(changes) ? changes : [];

        for (let index = source.length - 1; index >= 0; index -= 1) {
            const change = source[index];

            if (
                normalizeId(change && change.itemId) !== targetId
                || String(change && change.field || '').trim() !== 'status'
                || isAutomaticStatusChange(change)
            ) {
                continue;
            }

            return change;
        }

        return null;
    }

    function hasEffectivePendingStatusChange(
        changes,
        itemId,
        localItem
    ) {
        const lastManualChange = getLastManualStatusChange(
            changes,
            itemId
        );

        if (!lastManualChange) {
            return false;
        }

        return normalizeStatusValue(lastManualChange.newValue)
            === normalizeStatusValue(localItem && localItem.status);
    }

    function getUploadResponseItem(successfulUploads, itemId) {
        const targetId = normalizeId(itemId);
        const source = Array.isArray(successfulUploads)
            ? successfulUploads
            : [];

        for (let index = source.length - 1; index >= 0; index -= 1) {
            const responseItem = (
                source[index]
                && source[index].result
                && source[index].result.item
            );

            if (
                responseItem
                && typeof responseItem === 'object'
                && normalizeId(responseItem.id) === targetId
            ) {
                return responseItem;
            }
        }

        return null;
    }

    function mergeUploadResponseStatus(
        refreshedSnapshot,
        itemId,
        successfulUploads
    ) {
        const mergedSnapshot = clone(refreshedSnapshot || {});
        const responseItem = getUploadResponseItem(
            successfulUploads,
            itemId
        );

        if (!responseItem) {
            return mergedSnapshot;
        }

        if (!Array.isArray(mergedSnapshot.items)) {
            mergedSnapshot.items = [];
        }

        const targetId = normalizeId(itemId);
        const targetIndex = mergedSnapshot.items.findIndex(candidate => (
            normalizeId(candidate && candidate.id) === targetId
        ));

        if (targetIndex < 0) {
            mergedSnapshot.items.push(clone(responseItem));
            return mergedSnapshot;
        }

        const mergedItem = clone(
            mergedSnapshot.items[targetIndex] || {}
        );

        ['status', 'priority'].forEach(field => {
            if (
                Object.prototype.hasOwnProperty.call(
                    responseItem,
                    field
                )
            ) {
                mergedItem[field] = clone(responseItem[field]);
            }
        });

        mergedSnapshot.items[targetIndex] = mergedItem;
        return mergedSnapshot;
    }

    function mergeSingleItem(
        mergedSnapshot,
        serverItem,
        pendingChanges
    ) {
        const targetId = normalizeId(serverItem && serverItem.id);
        const localIndex = mergedSnapshot.items.findIndex(candidate => (
            normalizeId(candidate && candidate.id) === targetId
        ));

        if (localIndex < 0) {
            mergedSnapshot.items.push(clone(serverItem));
            return;
        }

        const localItem = clone(
            mergedSnapshot.items[localIndex] || {}
        );
        const mergedItem = clone(localItem);

        DOCUMENT_FIELDS.forEach(field => {
            if (Object.prototype.hasOwnProperty.call(serverItem, field)) {
                mergedItem[field] = clone(serverItem[field]);
            }
        });

        if (
            !hasEffectivePendingStatusChange(
                pendingChanges,
                targetId,
                localItem
            )
        ) {
            STATUS_FIELDS.forEach(field => {
                if (Object.prototype.hasOwnProperty.call(serverItem, field)) {
                    mergedItem[field] = clone(serverItem[field]);
                }
            });
        }

        mergedSnapshot.items[localIndex] = mergedItem;
    }

    // An item was moved or deleted in its window: take the item list
    // (which items exist, their section, parent, order) from the server.
    // Field values the popup has not saved yet stay as merged above.
    function syncStructure(mergedSnapshot, serverSnapshot, itemId) {
        const serverItems = Array.isArray(serverSnapshot && serverSnapshot.items)
            ? serverSnapshot.items
            : [];
        const serverById = new Map(serverItems.map(candidate => [
            normalizeId(candidate && candidate.id),
            candidate
        ]));
        const targetId = normalizeId(itemId);
        const kept = [];
        (mergedSnapshot.items || []).forEach(localItem => {
            const id = normalizeId(localItem && localItem.id);
            const serverItem = serverById.get(id);
            if (!serverItem) return;
            const merged = clone(localItem);
            STRUCTURE_FIELDS.forEach(field => {
                if (Object.prototype.hasOwnProperty.call(serverItem, field)) {
                    merged[field] = clone(serverItem[field]);
                }
            });
            if (id === targetId && Object.prototype.hasOwnProperty.call(serverItem, 'name')) {
                // A move can add a suffix to a name taken in the new place.
                merged.name = serverItem.name;
            }
            kept.push(merged);
        });
        const keptIds = new Set(kept.map(candidate => normalizeId(candidate && candidate.id)));
        serverItems.forEach(serverItem => {
            if (!keptIds.has(normalizeId(serverItem && serverItem.id))) {
                kept.push(clone(serverItem));
            }
        });
        mergedSnapshot.items = kept;
        if (
            Number(serverSnapshot.orderVersion || 0)
            > Number(mergedSnapshot.orderVersion || 0)
        ) {
            mergedSnapshot.orderVersion = serverSnapshot.orderVersion;
        }
        return mergedSnapshot;
    }

    function mergeSnapshot(
        localSnapshot,
        refreshedSnapshot,
        itemId,
        pendingChanges,
        options
    ) {
        const structureChanged = !!(options && options.structureChanged);
        const serverSnapshot = clone(refreshedSnapshot || {});
        const localHasItems = !!(
            localSnapshot
            && Array.isArray(localSnapshot.items)
        );

        const mergedSnapshot = localHasItems
            ? clone(localSnapshot)
            : serverSnapshot;

        if (!mergedSnapshot || typeof mergedSnapshot !== 'object') {
            return serverSnapshot || {};
        }

        if (!Array.isArray(mergedSnapshot.items)) {
            mergedSnapshot.items = [];
        }

        const targetId = normalizeId(itemId);
        const serverItems = Array.isArray(
            serverSnapshot && serverSnapshot.items
        )
            ? serverSnapshot.items
            : [];

        const serverItem = serverItems.find(candidate => (
            normalizeId(candidate && candidate.id) === targetId
        ));

        if (!serverItem) {
            return structureChanged
                ? syncStructure(mergedSnapshot, serverSnapshot, targetId)
                : mergedSnapshot;
        }

        // The item, its parent and its subitems: a change in the folder
        // window (upload into a subitem, new subitem, folder operation)
        // also changes the derived status of the family.
        const parentId = normalizeId(serverItem.parentItemId);
        const family = serverItems.filter(candidate => {
            const candidateId = normalizeId(candidate && candidate.id);
            return (
                candidateId === targetId
                || (parentId && candidateId === parentId)
                || normalizeId(candidate && candidate.parentItemId) === targetId
            );
        });

        family.forEach(candidate => {
            mergeSingleItem(mergedSnapshot, candidate, pendingChanges);
        });

        // A subitem created in the folder window bumps the order version.
        if (
            Number(serverSnapshot.orderVersion || 0)
            > Number(mergedSnapshot.orderVersion || 0)
        ) {
            mergedSnapshot.orderVersion = serverSnapshot.orderVersion;
        }

        if (structureChanged) {
            return syncStructure(mergedSnapshot, serverSnapshot, targetId);
        }
        return mergedSnapshot;
    }

    global.ChecklistPopupDocumentRefresh = Object.freeze({
        mergeSnapshot,
        mergeUploadResponseStatus,
        hasEffectivePendingStatusChange,
        documentFields: DOCUMENT_FIELDS,
        statusFields: STATUS_FIELDS
    });
})(window);
