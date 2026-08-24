(function (global) {
    'use strict';

    const paths = Object.freeze({
        upload: `
            <path d="M12 3v12"></path>
            <path d="m7 10 5 5 5-5"></path>
            <path d="M5 17v2a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-2"></path>
        `,
        folder: `
            <path d="M3.5 7.5v9.25A2.25 2.25 0 0 0 5.75 19h12.5a2.25 2.25 0 0 0 2.25-2.25v-7A2.25 2.25 0 0 0 18.25 7.5H11l-2-2H5.75A2.25 2.25 0 0 0 3.5 7.75"></path>
        `,
        share: `
            <circle cx="18" cy="5" r="2.5"></circle>
            <circle cx="6" cy="12" r="2.5"></circle>
            <circle cx="18" cy="19" r="2.5"></circle>
            <path d="m8.2 10.8 7.6-4.5"></path>
            <path d="m8.2 13.2 7.6 4.5"></path>
        `,
        open: `
            <path d="M14 3h7v7"></path>
            <path d="M10 14 21 3"></path>
            <path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"></path>
        `,
        download: `
            <path d="M5 4h14"></path>
            <path d="M12 20V4"></path>
            <path d="m7 9 5-5 5 5"></path>
        `,
        bell: `
            <path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"></path>
            <path d="M10 21h4"></path>
        `,
        yandex: `
            <path d="M7.5 18.5h9a4 4 0 0 0 .7-7.94A5.5 5.5 0 0 0 6.7 9.3a4.65 4.65 0 0 0 .8 9.2Z"></path>
            <path d="m10 13 2 2 3-4"></path>
        `,
        replace: ``,
        back: `
            <path d="m15 18-6-6 6-6"></path>
        `,
        remove: `
            <path d="M7 7l10 10"></path>
            <path d="M17 7 7 17"></path>
        `
    });

    const replacePath = Object.freeze({
        d: 'M17.511 7.444 19.289 5.667Q20.652 7.074 21.319 8.659Q22 10.319 22 12.2Q22 14.037 21.319 15.696Q20.607 17.37 19.289 18.689Q17.985 19.978 16.326 20.689Q14.726 21.37 12.83 21.37Q10.859 21.37 9.333 20.689Q7.615 19.919 6.37 18.689Q5.052 17.37 4.341 15.696Q3.659 14.037 3.659 12.2Q3.659 10.319 4.341 8.659Q5.037 7 6.37 5.667L6.889 5.148H2V2.63H10.237L11.185 3.578V11.815H8.667V6.926L8.148 7.444Q7.17 8.422 6.667 9.622Q6.178 10.822 6.178 12.2Q6.178 13.533 6.667 14.733Q7.156 15.933 8.133 16.911Q9.081 17.844 10.296 18.363Q11.452 18.852 12.859 18.852Q14.267 18.852 15.363 18.363Q16.637 17.785 17.526 16.911Q18.504 15.933 18.993 14.733Q19.481 13.533 19.481 12.2Q19.481 10.822 18.993 9.622Q18.519 8.422 17.511 7.444Z',
        erosionRadius: '0.30'
    });

    let replaceFilterSequence = 0;

    function buildReplaceMarkup() {
        replaceFilterSequence += 1;
        const filterId = (
            'checklist-replace-thin-' + replaceFilterSequence
        );

        return `
            <defs>
                <filter
                    id="${filterId}"
                    x="-16%"
                    y="-16%"
                    width="132%"
                    height="132%"
                    color-interpolation-filters="sRGB"
                >
                    <feMorphology
                        in="SourceGraphic"
                        operator="erode"
                        radius="${replacePath.erosionRadius}"
                    ></feMorphology>
                </filter>
            </defs>
            <path
                fill="currentColor"
                stroke="none"
                filter="url(#${filterId})"
                d="${replacePath.d}"
            ></path>
        `;
    }

    function normalizeName(name) {
        const normalized = String(name || '').trim().toLowerCase();
        return Object.prototype.hasOwnProperty.call(paths, normalized)
            ? normalized
            : '';
    }

    function svg(name) {
        const normalized = normalizeName(name);
        if (!normalized) return '';

        return `
            <svg
                class="checklist-action-icon"
                viewBox="0 0 24 24"
                aria-hidden="true"
                focusable="false"
                fill="none"
                stroke="currentColor"
                stroke-width="1.9"
                stroke-linecap="round"
                stroke-linejoin="round"
                data-checklist-icon-svg="${normalized}"
            >
                ${
                    normalized === 'replace'
                        ? buildReplaceMarkup()
                        : paths[normalized]
                }
            </svg>
        `;
    }

    function hydrate(root = global.document) {
        if (!root || typeof root.querySelectorAll !== 'function') {
            return 0;
        }

        let hydrated = 0;
        root.querySelectorAll('[data-checklist-icon]').forEach(element => {
            const iconName = normalizeName(
                element.dataset && element.dataset.checklistIcon
            );
            if (!iconName) return;

            const markup = svg(iconName);
            if (!markup) return;

            element.innerHTML = markup;
            element.dataset.checklistIconHydrated = '1';
            hydrated += 1;
        });

        return hydrated;
    }

    function setBusy(button, busy) {
        if (!button) return;

        const active = !!busy;
        button.classList.toggle('is-loading', active);
        button.setAttribute('aria-busy', active ? 'true' : 'false');
    }

    const api = Object.freeze({
        svg,
        hydrate,
        setBusy,
        names: Object.freeze(Object.keys(paths))
    });

    global.ChecklistActionIcons = api;

    if (global.document) {
        if (global.document.readyState === 'loading') {
            global.document.addEventListener(
                'DOMContentLoaded',
                function () {
                    hydrate(global.document);
                },
                { once: true }
            );
        } else {
            hydrate(global.document);
        }
    }
})(window);
