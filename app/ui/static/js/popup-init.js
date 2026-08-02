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
