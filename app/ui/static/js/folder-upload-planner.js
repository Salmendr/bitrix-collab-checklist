(function (global) {
    'use strict';

    // Where every file of a dropped folder goes.
    //
    // Into a top-level item: the dropped folder becomes a subitem (an
    // existing subitem with the same name is reused), deeper levels become
    // plain folders of that subitem. Into a subitem or one of its folders:
    // the whole dropped tree becomes plain folders below the current one.
    // Loose files go to the current item/folder as before.

    const FORBIDDEN_CHARS_RE = /[<>:"|?*\u0000-\u001f\u007f]/g;

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    // Mirrors sanitize_folder_segment on the server, so the planned names
    // (subitem lookup, empty folders) match what the server stores.
    function sanitizeSegment(value) {
        const segment = text(value)
            .replace(FORBIDDEN_CHARS_RE, '_')
            .replace(/\s+/g, ' ')
            .trim()
            .replace(/[. ]+$/, '')
            .trim();
        if (!segment || segment === '.' || segment === '..') return '';
        return segment;
    }

    function splitPath(path) {
        return text(path)
            .replace(/\\/g, '/')
            .split('/')
            .map(sanitizeSegment)
            .filter(Boolean);
    }

    function joinPath() {
        return Array.from(arguments)
            .map(text)
            .filter(Boolean)
            .join('/');
    }

    function sameName(left, right) {
        return text(left).toLocaleLowerCase('ru') === text(right).toLocaleLowerCase('ru');
    }

    function fileFolderPath(file) {
        const staging = global.ChecklistUploadStaging;
        if (staging && typeof staging.fileFolderPath === 'function') {
            return staging.fileFolderPath(file);
        }
        return text(file && file.__checklistFolderPath);
    }

    function hasFolders(files, emptyDirs) {
        return (Array.isArray(emptyDirs) && emptyDirs.length > 0)
            || Array.from(files || []).some(file => !!fileFolderPath(file));
    }

    /**
     * @param {object} options
     *   files, emptyDirs            staged content
     *   target: {itemId, topLevel, relativeFolder, subitems:[{id,name}]}
     *   ensureSubitem(name) -> Promise<{id, name}>   (top-level only)
     *   createFolder({itemId, path, root}) -> Promise
     * @returns {Promise<{placements, failures}>}
     *   placements: [{file, itemId, relativeFolder, folderUploadRoot}]
     *   failures:   [{file, error}]  (their subitem could not be created)
     */
    async function plan(options) {
        const target = options.target || {};
        const files = Array.from(options.files || []);
        const emptyDirs = Array.from(options.emptyDirs || []);
        const currentFolder = splitPath(target.relativeFolder).join('/');
        const knownSubitems = Array.isArray(target.subitems)
            ? target.subitems.slice()
            : [];
        const subitemPromises = new Map();
        const placements = [];
        const failures = [];

        function subitemFor(name) {
            const key = name.toLocaleLowerCase('ru');
            if (!subitemPromises.has(key)) {
                const existing = knownSubitems.find(child => sameName(child.name, name));
                subitemPromises.set(
                    key,
                    existing
                        ? Promise.resolve(existing)
                        : Promise.resolve().then(() => options.ensureSubitem(name))
                );
            }
            return subitemPromises.get(key);
        }

        // Subitems are created one after another (stable order, no races).
        async function resolvePlacement(folderPath) {
            const parts = splitPath(folderPath);
            if (!parts.length) {
                return {
                    itemId: target.itemId,
                    relativeFolder: currentFolder,
                    folderUploadRoot: ''
                };
            }
            const root = parts[0];
            if (target.topLevel) {
                const subitem = await subitemFor(root);
                return {
                    itemId: text(subitem && subitem.id),
                    relativeFolder: parts.slice(1).join('/'),
                    folderUploadRoot: root
                };
            }
            return {
                itemId: target.itemId,
                relativeFolder: joinPath(currentFolder, parts.join('/')),
                folderUploadRoot: root
            };
        }

        for (const file of files) {
            try {
                const placement = await resolvePlacement(fileFolderPath(file));
                if (!placement.itemId) {
                    throw new Error('Подпункт для папки не создан');
                }
                placements.push({ file, ...placement });
            } catch (error) {
                failures.push({ file, error });
            }
        }

        // Empty folders: files create their folders implicitly, so only the
        // folders without any file below them are created explicitly.
        const filePaths = files.map(file => splitPath(fileFolderPath(file)).join('/'));
        for (const dir of emptyDirs) {
            const parts = splitPath(dir);
            if (!parts.length) continue;
            const path = parts.join('/');
            const prefix = path.toLocaleLowerCase('ru') + '/';
            const covered = filePaths.some(filePath => (
                (filePath + '/').toLocaleLowerCase('ru').startsWith(prefix)
            ));
            if (covered) continue;
            try {
                const placement = await resolvePlacement(path);
                if (placement.relativeFolder && placement.relativeFolder !== currentFolder) {
                    await options.createFolder({
                        itemId: placement.itemId,
                        path: placement.relativeFolder,
                        root: placement.folderUploadRoot
                    });
                }
            } catch (error) {
                failures.push({ file: null, emptyDir: dir, error });
            }
        }

        return { placements, failures };
    }

    global.ChecklistFolderUploadPlanner = Object.freeze({
        fileFolderPath,
        hasFolders,
        plan,
        sanitizeSegment,
        splitPath
    });
})(window);
