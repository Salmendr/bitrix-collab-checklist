            function safeInitBx24ForPopup() {
                popupBitrix.fitPopup({
                    width: 1180,
                    height: 720,
                    delays: [0, 80, 220]
                }).catch(function (error) {
                    console.log('Bitrix popup sizing skipped:', error);
                });
            }
            try {
                logRenderState('before_renderAll');
                renderAll();
                logRenderState('after_renderAll');

                debugLog('popup_loaded', {
                    href: window.location.href,
                    hasDialogId: !!dialogId
                });

                const initialFocusItemId = String(
                    new URLSearchParams(window.location.search)
                        .get('focusItemId')
                    || ''
                ).trim();
                if (
                    initialFocusItemId
                    && window.ChecklistPopupNavigation
                    && typeof window.ChecklistPopupNavigation.returnToItem
                        === 'function'
                ) {
                    window.setTimeout(function () {
                        window.ChecklistPopupNavigation.returnToItem({
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId: initialFocusItemId,
                            source: 'popup_url_focus'
                        }).catch(function (error) {
                            console.log(
                                'initial popup item focus skipped:',
                                error
                            );
                        });
                    }, 80);
                }
            } catch (e) {
                logRenderError('renderAll', e);
            }

            try {
                fetchChatTitleIfMissing();
            } catch (e) {
                logRenderError('fetchChatTitleIfMissing', e);
            }

            try {
                fetchCurrentUserIfPossible();
            } catch (e) {
                logRenderError('fetchCurrentUserIfPossible', e);
            }

            try {
                safeInitBx24ForPopup();
            } catch (e) {
                logRenderError('safeInitBx24ForPopup', e);
            }
