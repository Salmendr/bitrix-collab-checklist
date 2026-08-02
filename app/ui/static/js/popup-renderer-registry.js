(function (global) {
    'use strict';

    const strategies = new Map();

    function normalizeName(name) {
        return String(name || '').trim();
    }

    function normalizePriority(value) {
        const priority = Number(value);
        return Number.isFinite(priority) ? priority : 0;
    }

    function register(name, strategy) {
        const normalizedName = normalizeName(name);
        if (!normalizedName) {
            throw new Error('renderer strategy name is required');
        }
        if (!strategy || typeof strategy.render !== 'function') {
            throw new Error('renderer strategy must define render()');
        }

        strategies.set(normalizedName, Object.freeze({
            name: normalizedName,
            priority: normalizePriority(strategy.priority),
            matches: typeof strategy.matches === 'function'
                ? strategy.matches
                : function () { return false; },
            render: strategy.render,
            bindEvents: typeof strategy.bindEvents === 'function'
                ? strategy.bindEvents
                : null
        }));

        return strategies.get(normalizedName);
    }

    function get(name) {
        return strategies.get(normalizeName(name)) || null;
    }

    function has(name) {
        return strategies.has(normalizeName(name));
    }

    function resolve(context) {
        const ordered = Array.from(strategies.values()).sort(function (left, right) {
            return right.priority - left.priority;
        });

        for (const strategy of ordered) {
            if (strategy.matches(context || {})) {
                return strategy;
            }
        }

        return null;
    }

    function list() {
        return Array.from(strategies.values())
            .sort(function (left, right) {
                return right.priority - left.priority;
            })
            .map(function (strategy) {
                return {
                    name: strategy.name,
                    priority: strategy.priority
                };
            });
    }

    global.ChecklistPopupRendererRegistry = Object.freeze({
        register,
        get,
        has,
        resolve,
        list
    });
})(window);
