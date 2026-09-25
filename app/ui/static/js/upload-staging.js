(function (global) {
    'use strict';

    const states = new Map();
    let generatedStateId = 0;

    function normalizedText(value) {
        return String(value == null ? '' : value).trim();
    }

    // Folder uploads: every file keeps the folder it came from
    // ("Альбом/Разделы"), empty folders are kept as paths.
    const SKIPPED_SYSTEM_FILES = new Set(['.ds_store', 'thumbs.db', 'desktop.ini']);

    function fileFolderPath(file) {
        if (!file) return '';
        if (typeof file.__checklistFolderPath === 'string') {
            return file.__checklistFolderPath;
        }
        const relative = normalizedText(file.webkitRelativePath);
        if (!relative || relative.indexOf('/') < 0) return '';
        return relative.split('/').slice(0, -1).filter(Boolean).join('/');
    }

    function topFolderOf(path) {
        return normalizedText(path).split('/').filter(Boolean)[0] || '';
    }

    function tagFile(file, folderPath) {
        try {
            Object.defineProperty(file, '__checklistFolderPath', {
                value: normalizedText(folderPath),
                configurable: true,
                enumerable: false,
                writable: true
            });
        } catch (error) {
            file.__checklistFolderPath = normalizedText(folderPath);
        }
        return file;
    }

    function isSkippedSystemFile(file) {
        return SKIPPED_SYSTEM_FILES.has(
            normalizedText(file && file.name).toLowerCase()
        );
    }

    function readDirectoryEntries(directoryEntry) {
        const reader = directoryEntry.createReader();
        const all = [];
        return new Promise((resolve, reject) => {
            function readBatch() {
                // readEntries returns at most ~100 entries per call.
                reader.readEntries(batch => {
                    if (!batch.length) {
                        resolve(all);
                        return;
                    }
                    all.push(...batch);
                    readBatch();
                }, reject);
            }
            readBatch();
        });
    }

    function entryFile(fileEntry) {
        return new Promise((resolve, reject) => fileEntry.file(resolve, reject));
    }

    async function walkEntry(entry, parentPath, result) {
        if (!entry) return;
        if (entry.isFile) {
            const file = await entryFile(entry);
            if (!isSkippedSystemFile(file)) {
                result.files.push(tagFile(file, parentPath));
            }
            return;
        }
        if (!entry.isDirectory) return;
        const path = parentPath ? parentPath + '/' + entry.name : entry.name;
        const children = await readDirectoryEntries(entry);
        if (!children.length) {
            result.emptyDirs.push(path);
            return;
        }
        for (const child of children) {
            await walkEntry(child, path, result);
        }
    }

    // Entries must be taken synchronously inside the drop handler: the
    // DataTransfer list is emptied when the event returns.
    function takeDroppedEntries(dataTransfer) {
        const entries = [];
        const looseFiles = [];
        Array.from(dataTransfer && dataTransfer.items || []).forEach(item => {
            if (!item || item.kind !== 'file') return;
            const entry = typeof item.webkitGetAsEntry === 'function'
                ? item.webkitGetAsEntry()
                : null;
            if (entry) {
                entries.push(entry);
                return;
            }
            const file = typeof item.getAsFile === 'function' ? item.getAsFile() : null;
            if (file) looseFiles.push(file);
        });
        if (!entries.length && !looseFiles.length) {
            Array.from(dataTransfer && dataTransfer.files || []).forEach(file => {
                looseFiles.push(file);
            });
        }
        return { entries, looseFiles };
    }

    async function collectDropped(taken) {
        const result = { files: taken.looseFiles.slice(), emptyDirs: [] };
        for (const entry of taken.entries) {
            await walkEntry(entry, '', result);
        }
        return result;
    }

    function fileIdentity(file) {
        return [
            fileFolderPath(file),
            normalizedText(file && file.name),
            Number(file && file.size || 0),
            Number(file && file.lastModified || 0),
            normalizedText(file && file.type)
        ].join('\u0000');
    }

    function fileExtension(file) {
        const name = normalizedText(file && file.name);
        const dotIndex = name.lastIndexOf('.');
        if (dotIndex <= 0 || dotIndex === name.length - 1) {
            return 'FILE';
        }
        return name.slice(dotIndex + 1, dotIndex + 6).toUpperCase();
    }

    function formatBytes(value) {
        const size = Math.max(0, Number(value || 0));
        if (size < 1024) return size + ' Б';
        if (size < 1024 * 1024) {
            return (size / 1024).toFixed(size < 10 * 1024 ? 1 : 0) + ' КБ';
        }
        if (size < 1024 * 1024 * 1024) {
            return (size / (1024 * 1024)).toFixed(size < 10 * 1024 * 1024 ? 1 : 0) + ' МБ';
        }
        return (size / (1024 * 1024 * 1024)).toFixed(1) + ' ГБ';
    }

    function getState(stateKey) {
        if (!states.has(stateKey)) {
            states.set(stateKey, {
                busy: false,
                dropActive: false,
                expanded: false,
                files: [],
                emptyDirs: [],
                listeners: new Set()
            });
        }
        return states.get(stateKey);
    }

    function notifyState(state) {
        Array.from(state.listeners).forEach(listener => {
            try {
                listener();
            } catch (error) {
                console.log('upload staging render error:', error);
                state.listeners.delete(listener);
            }
        });
    }

    function createFileCard(file, removeFile) {
        const card = global.document.createElement('article');
        card.className = 'upload-staging-file-card';
        card.title = normalizedText(file && file.name) || 'Файл';

        const iconWrap = global.document.createElement('div');
        iconWrap.className = 'upload-staging-file-icon-wrap';
        iconWrap.innerHTML = `
            <svg class="upload-staging-file-icon" viewBox="0 0 36 44" aria-hidden="true">
                <path d="M7 2.5h14l8 8V40a1.5 1.5 0 0 1-1.5 1.5h-20A1.5 1.5 0 0 1 6 40V4A1.5 1.5 0 0 1 7.5 2.5Z"></path>
                <path d="M21 2.5V11h8"></path>
            </svg>
        `;

        const extension = global.document.createElement('span');
        extension.className = 'upload-staging-file-extension';
        extension.textContent = fileExtension(file);
        iconWrap.appendChild(extension);

        const removeButton = global.document.createElement('button');
        removeButton.type = 'button';
        removeButton.className = 'upload-staging-file-remove';
        removeButton.title = 'Убрать файл из списка';
        removeButton.setAttribute('aria-label', 'Убрать файл из списка');
        removeButton.textContent = '×';
        removeButton.addEventListener('click', event => {
            event.preventDefault();
            event.stopPropagation();
            removeFile(file);
        });
        iconWrap.appendChild(removeButton);

        const name = global.document.createElement('div');
        name.className = 'upload-staging-file-name';
        name.textContent = normalizedText(file && file.name) || 'Файл';

        const size = global.document.createElement('div');
        size.className = 'upload-staging-file-size';
        size.textContent = formatBytes(file && file.size);

        card.append(iconWrap, name, size);
        return card;
    }

    function createFolderCard(group, removeFolder) {
        const card = global.document.createElement('article');
        card.className = 'upload-staging-file-card upload-staging-folder-card';
        card.title = group.name;

        const iconWrap = global.document.createElement('div');
        iconWrap.className = 'upload-staging-file-icon-wrap';
        iconWrap.innerHTML = `
            <svg class="upload-staging-folder-icon" viewBox="0 0 44 36" aria-hidden="true">
                <path d="M3 6.5A2.5 2.5 0 0 1 5.5 4h11l3.5 4h18.5A2.5 2.5 0 0 1 41 10.5v19a2.5 2.5 0 0 1-2.5 2.5h-33A2.5 2.5 0 0 1 3 29.5Z"></path>
            </svg>
        `;

        const removeButton = global.document.createElement('button');
        removeButton.type = 'button';
        removeButton.className = 'upload-staging-file-remove';
        removeButton.title = 'Убрать папку из списка';
        removeButton.setAttribute('aria-label', 'Убрать папку из списка');
        removeButton.textContent = '×';
        removeButton.addEventListener('click', event => {
            event.preventDefault();
            event.stopPropagation();
            removeFolder(group.name);
        });
        iconWrap.appendChild(removeButton);

        const name = global.document.createElement('div');
        name.className = 'upload-staging-file-name';
        name.textContent = group.name;

        const size = global.document.createElement('div');
        size.className = 'upload-staging-file-size';
        size.textContent = formatBytes(group.size);

        card.append(iconWrap, name, size);
        return card;
    }

    function groupStaged(files, emptyDirs) {
        const loose = [];
        const folders = new Map();
        function folderGroup(top) {
            const key = top.toLocaleLowerCase('ru');
            if (!folders.has(key)) {
                folders.set(key, { name: top, size: 0 });
            }
            return folders.get(key);
        }
        files.forEach(file => {
            const top = topFolderOf(fileFolderPath(file));
            if (!top) {
                loose.push(file);
                return;
            }
            folderGroup(top).size += Number(file && file.size || 0);
        });
        emptyDirs.forEach(path => {
            const top = topFolderOf(path);
            if (top) folderGroup(top);
        });
        return { loose, folders: Array.from(folders.values()) };
    }

    function create(options = {}) {
        const trigger = options.trigger;
        const input = options.input;
        const mount = options.mount;

        if (!trigger || !input || !mount) return null;
        if (trigger.__checklistUploadStaging) {
            return trigger.__checklistUploadStaging;
        }

        generatedStateId += 1;
        const stateKey = normalizedText(options.stateKey)
            || 'upload-staging-' + generatedStateId;
        const state = getState(stateKey);

        const zone = global.document.createElement('section');
        zone.className = 'upload-staging-zone';
        zone.hidden = true;
        zone.setAttribute('aria-label', 'Предварительная загрузка файлов');
        zone.innerHTML = `
            <div class="upload-staging-heading-row">
                <button class="upload-staging-picker" type="button" data-role="upload-staging-picker">
                    <svg class="upload-staging-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
                        <path d="M12 16V4"></path>
                        <path d="m7 9 5-5 5 5"></path>
                        <path d="M5 20h14"></path>
                    </svg>
                    <span class="upload-staging-picker-copy">
                        <strong>Перетащите файлы или папки для загрузки</strong>
                        <small>или нажмите здесь, чтобы выбрать файлы</small>
                    </span>
                </button>
                <button
                    class="upload-staging-folder-picker"
                    type="button"
                    data-role="upload-staging-folder-picker"
                    title="Выбрать папку целиком со всеми вложенными папками"
                >Выбрать папку</button>
                <button
                    class="upload-staging-close"
                    type="button"
                    data-role="upload-staging-close"
                    title="Свернуть область загрузки"
                    aria-label="Свернуть область загрузки"
                >×</button>
            </div>
            <div class="upload-staging-file-grid" data-role="upload-staging-files" hidden></div>
            <div class="upload-staging-footer" data-role="upload-staging-footer" hidden>
                <span class="upload-staging-count" data-role="upload-staging-count"></span>
                <button class="upload-staging-confirm" type="button" data-role="upload-staging-confirm">
                    Загрузить
                </button>
            </div>
        `;

        if (options.prepend === true) {
            mount.insertBefore(zone, mount.firstChild);
        } else {
            mount.appendChild(zone);
        }

        const picker = zone.querySelector('[data-role="upload-staging-picker"]');
        const closeButton = zone.querySelector('[data-role="upload-staging-close"]');
        const fileGrid = zone.querySelector('[data-role="upload-staging-files"]');
        const footer = zone.querySelector('[data-role="upload-staging-footer"]');
        const count = zone.querySelector('[data-role="upload-staging-count"]');
        const confirmButton = zone.querySelector('[data-role="upload-staging-confirm"]');
        const folderPicker = zone.querySelector('[data-role="upload-staging-folder-picker"]');
        const folderInput = global.document.createElement('input');
        folderInput.type = 'file';
        folderInput.multiple = true;
        folderInput.hidden = true;
        folderInput.setAttribute('webkitdirectory', '');
        folderInput.setAttribute('directory', '');
        zone.appendChild(folderInput);
        if (options.allowFolders === false) {
            folderPicker.hidden = true;
        }

        function canInteract() {
            if (state.busy || trigger.disabled) return false;
            try {
                return typeof options.canInteract !== 'function'
                    || options.canInteract() !== false;
            } catch (error) {
                if (typeof options.onBlocked === 'function') {
                    options.onBlocked(error);
                }
                return false;
            }
        }

        function replaceFiles(files, settings = {}) {
            const unique = new Map();
            Array.from(files || []).forEach(file => {
                if (!file || !normalizedText(file.name)) return;
                unique.set(fileIdentity(file), file);
            });
            state.files = Array.from(unique.values());
            // Empty folders are created before the files are sent.
            state.emptyDirs = Array.isArray(settings.emptyDirs)
                ? settings.emptyDirs.slice()
                : [];
            if (Object.prototype.hasOwnProperty.call(settings, 'expanded')) {
                state.expanded = !!settings.expanded;
            }
            notifyState(state);
        }

        function addFiles(files, emptyDirs = []) {
            if (!canInteract()) return;
            if (options.allowFolders === false) {
                const hasFolders = Array.from(files || []).some(file => !!fileFolderPath(file))
                    || (emptyDirs && emptyDirs.length);
                if (hasFolders) {
                    if (typeof options.onBlocked === 'function') {
                        options.onBlocked(new Error('Сюда можно загрузить только файлы'));
                    }
                    files = Array.from(files || []).filter(file => !fileFolderPath(file));
                    emptyDirs = [];
                }
            }
            const unique = new Map(
                state.files.map(file => [fileIdentity(file), file])
            );
            Array.from(files || []).forEach(file => {
                if (!file || !normalizedText(file.name)) return;
                unique.set(fileIdentity(file), file);
            });
            state.files = Array.from(unique.values());
            const dirs = new Map(
                state.emptyDirs.map(path => [path.toLocaleLowerCase('ru'), path])
            );
            Array.from(emptyDirs || []).forEach(path => {
                const normalized = normalizedText(path);
                if (normalized) dirs.set(normalized.toLocaleLowerCase('ru'), normalized);
            });
            state.emptyDirs = Array.from(dirs.values());
            state.expanded = true;
            state.dropActive = false;
            input.value = '';
            folderInput.value = '';
            notifyState(state);
        }

        function removeFolder(name) {
            if (state.busy) return;
            const key = normalizedText(name).toLocaleLowerCase('ru');
            state.files = state.files.filter(file => (
                topFolderOf(fileFolderPath(file)).toLocaleLowerCase('ru') !== key
            ));
            state.emptyDirs = state.emptyDirs.filter(path => (
                topFolderOf(path).toLocaleLowerCase('ru') !== key
            ));
            notifyState(state);
        }

        function removeFile(file) {
            if (state.busy) return;
            const identity = fileIdentity(file);
            state.files = state.files.filter(candidate => (
                fileIdentity(candidate) !== identity
            ));
            notifyState(state);
        }

        function clear(settings = {}) {
            state.files = [];
            state.emptyDirs = [];
            state.dropActive = false;
            if (settings.collapse !== false) {
                state.expanded = false;
            }
            input.value = '';
            folderInput.value = '';
            notifyState(state);
        }

        function expand(settings = {}) {
            if (!canInteract()) return false;
            const wasExpanded = state.expanded;
            state.expanded = true;
            if (!wasExpanded) notifyState(state);
            if (
                !wasExpanded
                && settings.scroll === true
                && typeof zone.scrollIntoView === 'function'
            ) {
                global.setTimeout(() => {
                    zone.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                }, 0);
            }
            return true;
        }

        function setDropActive(value) {
            const next = !!value;
            if (state.dropActive === next) return;
            state.dropActive = next;
            notifyState(state);
        }

        const controller = {
            addFiles,
            clear,
            collapse() {
                state.expanded = false;
                state.dropActive = false;
                notifyState(state);
            },
            expand,
            getFiles() {
                return state.files.slice();
            },
            getEmptyDirs() {
                return state.emptyDirs.slice();
            },
            replaceFiles,
            setBusy(value) {
                state.busy = !!value;
                notifyState(state);
            },
            stateKey,
            zone
        };

        function render() {
            if (!zone.isConnected) {
                state.listeners.delete(render);
                return;
            }

            zone.hidden = !state.expanded;
            zone.classList.toggle('is-drop-target', state.dropActive);
            zone.classList.toggle('is-uploading', state.busy);
            mount.classList.toggle('upload-staging-open', state.expanded);
            trigger.classList.toggle(
                'is-upload-staging-trigger-hidden',
                !!options.hideTriggerWhenExpanded && state.expanded
            );
            trigger.classList.toggle('has-staged-files', state.files.length > 0);
            trigger.classList.toggle('is-uploading', state.busy);
            trigger.dataset.stagedCount = String(state.files.length);
            trigger.setAttribute('aria-expanded', state.expanded ? 'true' : 'false');
            trigger.setAttribute('aria-busy', state.busy ? 'true' : 'false');

            const hasContent = state.files.length > 0 || state.emptyDirs.length > 0;
            picker.disabled = state.busy;
            folderPicker.disabled = state.busy;
            closeButton.disabled = state.busy;
            confirmButton.disabled = state.busy || !hasContent;
            confirmButton.textContent = state.busy ? 'Загрузка…' : 'Загрузить';

            const grouped = groupStaged(state.files, state.emptyDirs);
            fileGrid.replaceChildren();
            grouped.folders.forEach(group => {
                fileGrid.appendChild(createFolderCard(group, removeFolder));
            });
            grouped.loose.forEach(file => {
                fileGrid.appendChild(createFileCard(file, removeFile));
            });

            fileGrid.hidden = !hasContent;
            footer.hidden = !hasContent;
            const parts = [];
            if (grouped.folders.length) {
                parts.push('папок: ' + grouped.folders.length);
            }
            if (state.files.length) {
                parts.push('файлов: ' + state.files.length);
            }
            count.textContent = hasContent
                ? 'Выбрано ' + parts.join(', ')
                : '';
        }

        async function confirmUpload() {
            if (!canInteract() || (!state.files.length && !state.emptyDirs.length)) return;
            const selectedFiles = state.files.slice();
            const selectedEmptyDirs = state.emptyDirs.slice();
            state.busy = true;
            notifyState(state);

            try {
                const result = typeof options.onConfirm === 'function'
                    ? await options.onConfirm(selectedFiles, controller, {
                        emptyDirs: selectedEmptyDirs
                    })
                    : null;

                if (!(result && result.keepState === true)) {
                    if (result && Array.isArray(result.failedFiles)) {
                        replaceFiles(result.failedFiles, {
                            expanded: result.failedFiles.length > 0
                        });
                    } else {
                        clear({ collapse: true });
                    }
                }
            } catch (error) {
                if (typeof options.onError === 'function') {
                    options.onError(error);
                } else {
                    console.log('upload staging confirm error:', error);
                }
            } finally {
                state.busy = false;
                notifyState(state);
            }
        }

        trigger.addEventListener('click', event => {
            event.preventDefault();
            if (!expand({ scroll: options.scrollOnExpand === true })) return;
        });

        ['dragenter', 'dragover'].forEach(eventName => {
            trigger.addEventListener(eventName, event => {
                if (!canInteract()) return;
                event.preventDefault();
                event.stopPropagation();
                if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
                expand({ scroll: options.scrollOnExpand === true });
                setDropActive(true);
            });

            zone.addEventListener(eventName, event => {
                if (!canInteract()) return;
                event.preventDefault();
                event.stopPropagation();
                if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
                setDropActive(true);
            });
        });

        [trigger, zone].forEach(target => {
            target.addEventListener('dragleave', event => {
                event.preventDefault();
                event.stopPropagation();
                if (event.relatedTarget && target.contains(event.relatedTarget)) return;
                setDropActive(false);
            });

            target.addEventListener('drop', event => {
                if (!canInteract()) return;
                event.preventDefault();
                event.stopPropagation();
                setDropActive(false);
                const taken = takeDroppedEntries(event.dataTransfer);
                if (!taken.entries.length && !taken.looseFiles.length) return;
                collectDropped(taken).then(result => {
                    if (result.files.length || result.emptyDirs.length) {
                        addFiles(result.files, result.emptyDirs);
                    }
                }).catch(error => {
                    console.log('upload staging folder read error:', error);
                    if (typeof options.onError === 'function') {
                        options.onError(new Error('Не удалось прочитать перетащенную папку'));
                    }
                });
            });
        });

        picker.addEventListener('click', () => {
            if (canInteract()) input.click();
        });
        folderPicker.addEventListener('click', () => {
            if (canInteract()) folderInput.click();
        });
        folderInput.addEventListener('change', () => {
            const files = Array.from(folderInput.files || [])
                .filter(file => !isSkippedSystemFile(file))
                .map(file => tagFile(file, fileFolderPath(file)));
            if (files.length) addFiles(files);
            folderInput.value = '';
        });
        closeButton.addEventListener('click', () => controller.collapse());
        confirmButton.addEventListener('click', confirmUpload);
        input.addEventListener('change', () => {
            const files = Array.from(input.files || []);
            if (files.length) addFiles(files);
        });

        state.listeners.add(render);
        render();
        trigger.__checklistUploadStaging = controller;
        return controller;
    }

    function pendingFileCount() {
        return Array.from(states.values()).reduce(
            (total, state) => total + state.files.length + state.emptyDirs.length,
            0
        );
    }

    function clearAll() {
        states.forEach(state => {
            state.files = [];
            state.emptyDirs = [];
            state.dropActive = false;
            state.expanded = false;
            notifyState(state);
        });
    }

    global.ChecklistUploadStaging = Object.freeze({
        clearAll,
        create,
        fileFolderPath,
        tagFile,
        hasPendingFiles() {
            return pendingFileCount() > 0;
        },
        pendingFileCount
    });
})(window);
