import pathlib
import sys

TARGET = pathlib.Path("app/ui/shell.py")
src = TARGET.read_text(encoding="utf-8")

# --- Правка 1: добавить флаг hasOpenedOnce ---
old_1 = """            var initialDialogId = {initial_dialog_id_json};
            var initialContextText = {initial_context_text_json};
            var autoOpened = false;
            var hostDiagnostics = window.ChecklistHostDiagnostics;"""

new_1 = """            var initialDialogId = {initial_dialog_id_json};
            var initialContextText = {initial_context_text_json};
            var autoOpened = false;
            var hasOpenedOnce = false;
            var hostDiagnostics = window.ChecklistHostDiagnostics;"""

if old_1 not in src:
    print("ANCHOR 1 NOT FOUND - stopping without changes")
    sys.exit(1)
src = src.replace(old_1, new_1, 1)

# --- Правка 2: повторное открытие при возврате видимости iframe ---
old_2 = """            document.addEventListener('visibilitychange', function () {{
                recordLauncherRuntimeState(
                    'visibilitychange',
                    Boolean(document.hidden)
                );
            }}, true);"""

new_2 = """            document.addEventListener('visibilitychange', function () {{
                recordLauncherRuntimeState(
                    'visibilitychange',
                    Boolean(document.hidden)
                );

                if (
                    document.visibilityState === 'visible'
                    && window.__dialogId
                    && hasOpenedOnce
                    && !autoOpened
                ) {{
                    openChecklist(
                        window.__dialogId,
                        'id',
                        'automatic_after_reshow'
                    );
                }}
            }}, true);"""

if old_2 not in src:
    print("ANCHOR 2 NOT FOUND - stopping without changes")
    sys.exit(1)
src = src.replace(old_2, new_2, 1)

# --- Правка 3: callback закрытия popup сбрасывает autoOpened ---
old_3 = """                        var openResult = BX24.openApplication({{
                            dialogId: dialogId,
                            checklistKey: checklistKey,
                            source: 'textarea'
                        }});
                        autoOpened = true;"""

new_3 = """                        var openResult = BX24.openApplication(
                            {{
                                dialogId: dialogId,
                                checklistKey: checklistKey,
                                source: 'textarea'
                            }},
                            function () {{
                                autoOpened = false;
                                recordHostDiagnostic(
                                    'popup_diag_launcher_close_callback',
                                    {{
                                        launchId: launch && launch.launchId || '',
                                        trigger: trigger,
                                        dialogId: dialogId,
                                        checklistKey: checklistKey
                                    }},
                                    true
                                );
                            }}
                        );
                        autoOpened = true;
                        hasOpenedOnce = true;"""

if old_3 not in src:
    print("ANCHOR 3 NOT FOUND - stopping without changes")
    sys.exit(1)
src = src.replace(old_3, new_3, 1)

TARGET.write_text(src, encoding="utf-8")
print("textarea reopen fix applied: 3/3 anchors patched")
