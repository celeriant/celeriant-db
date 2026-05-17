(function () {
    'use strict';

    let accounts = [];

    // Per-account state: state[accountId]
    const state = {};

    function getState(accountId) {
        if (!state[accountId]) {
            state[accountId] = { balanceCents: 0, aggregateVersion: 0, events: [] };
        }
        return state[accountId];
    }

    function formatCents(cents) {
        const negative = cents < 0;
        const abs = Math.abs(cents);
        return (negative ? '-' : '') + '$' + (abs / 100).toFixed(2);
    }

    function eventLabel(e) {
        switch (e.type) {
            case 'Deposited': return { text: 'Deposit', amount: e.amountCents, cls: 'positive' };
            case 'Withdrawn': return { text: 'Withdrawal', amount: -e.amountCents, cls: 'negative' };
            case 'TransferredOut': {
                const toName = accounts.find(a => a.id === e.toAccountId)?.name || '?';
                return { text: 'Transfer to ' + toName, amount: -e.amountCents, cls: 'negative' };
            }
            case 'TransferredIn': {
                const fromName = accounts.find(a => a.id === e.fromAccountId)?.name || '?';
                return { text: 'Transfer from ' + fromName, amount: e.amountCents, cls: 'positive' };
            }
            default: return { text: e.type || 'Unknown', amount: 0, cls: '' };
        }
    }

    // One UUID per user intent (per button click). Reused across retries so the
    // BFF can recognise the duplicate via event_id even after a BFF crash.
    function newIntentKey() {
        return crypto.randomUUID();
    }

    // Disable a button for the duration of an async action. Suppresses button-smash:
    // a rapid second click would otherwise be a separate intent (different UUID) and land
    // a second event in the stream.
    async function withBusy(btnEl, fn) {
        btnEl.disabled = true;
        try {
            await fn();
        } finally {
            btnEl.disabled = false;
        }
    }

    // Fetch with a single retry on network failure, reusing the same Idempotency-Key.
    async function fetchWithRetry(url, body, idempotencyKey) {
        let lastErr = null;
        for (let attempt = 1; attempt <= 2; attempt++) {
            try {
                return await fetch(url, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Idempotency-Key': idempotencyKey,
                    },
                    body: JSON.stringify(body),
                });
            } catch (e) {
                lastErr = e;
                if (attempt < 2) {
                    await new Promise(r => setTimeout(r, 300));
                }
            }
        }
        throw lastErr;
    }

    function parseCents(input) {
        const val = parseFloat(input);
        if (isNaN(val) || val <= 0) return null;
        return Math.round(val * 100);
    }

    // Serialize concurrent fetches per accountId to prevent race conditions
    const fetchLocks = {};

    async function fetchBalance(accountId) {
        const lockKey = accountId;
        const prev = fetchLocks[lockKey] || Promise.resolve();
        const next = prev.then(() => doFetchBalance(accountId), () => doFetchBalance(accountId));
        fetchLocks[lockKey] = next;
        return next;
    }

    async function doFetchBalance(accountId) {
        const s = getState(accountId);
        const res = await fetch(`/api/accounts/${accountId}/balance`);
        if (!res.ok) return;
        const data = await res.json();
        s.balanceCents = data.balanceCents;
        s.aggregateVersion = data.aggregateVersion;
    }

    async function fetchHistory(accountId) {
        const s = getState(accountId);
        const res = await fetch(`/api/accounts/${accountId}/history`);
        if (!res.ok) return;
        const data = await res.json();
        s.events = data.events || [];
        s.balanceCents = data.balanceCents;
        s.aggregateVersion = data.currentAggregateVersion;
    }

    function showToast(cardEl, msg, type) {
        const toast = cardEl.querySelector('.toast');
        toast.textContent = msg;
        toast.className = 'toast ' + type;
        setTimeout(() => { toast.className = 'toast'; }, 5000);
    }

    async function handleErrorResponse(res, cardEl) {
        const data = await res.json();
        if (data.error === 'INSUFFICIENT_FUNDS') {
            showToast(cardEl, data.message, 'warn');
        } else if (data.error === 'CONFLICT') {
            showToast(cardEl, data.message, 'error');
        } else if (data.error === 'VALIDATION_ERROR') {
            showToast(cardEl, data.message, 'warn');
        } else if (data.error === 'SERVICE_UNAVAILABLE') {
            showToast(cardEl, data.message, 'error');
        } else {
            showToast(cardEl, data.message || 'Server error.', 'error');
        }
    }

    async function doDeposit(accountId, cardEl) {
        const input = cardEl.querySelector('.amount-input');
        const cents = parseCents(input.value);
        if (!cents) { showToast(cardEl, 'Enter a valid amount.', 'warn'); return; }

        const intentKey = newIntentKey();
        try {
            const res = await fetchWithRetry(
                `/api/accounts/${accountId}/deposit`,
                { amountCents: cents },
                intentKey,
            );
            if (!res.ok) { await handleErrorResponse(res, cardEl); return; }
            const data = await res.json();
            const s = getState(accountId);
            s.balanceCents = data.balanceCents;
            s.aggregateVersion = data.aggregateVersion;
            input.value = '';
            await fetchHistory(accountId);
            showToast(cardEl, 'Deposit successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    async function doWithdraw(accountId, cardEl) {
        const input = cardEl.querySelector('.amount-input');
        const cents = parseCents(input.value);
        if (!cents) { showToast(cardEl, 'Enter a valid amount.', 'warn'); return; }

        const intentKey = newIntentKey();
        try {
            const res = await fetchWithRetry(
                `/api/accounts/${accountId}/withdraw`,
                { amountCents: cents },
                intentKey,
            );
            if (!res.ok) { await handleErrorResponse(res, cardEl); return; }
            const data = await res.json();
            const s = getState(accountId);
            s.balanceCents = data.balanceCents;
            s.aggregateVersion = data.aggregateVersion;
            input.value = '';
            await fetchHistory(accountId);
            showToast(cardEl, 'Withdrawal successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    async function doTransfer(accountId, cardEl) {
        const amountInput = cardEl.querySelector('.transfer-amount');
        const selectEl = cardEl.querySelector('.transfer-target');
        const cents = parseCents(amountInput.value);
        if (!cents) { showToast(cardEl, 'Enter a valid transfer amount.', 'warn'); return; }

        const toAccountId = selectEl.value;
        if (!toAccountId) { showToast(cardEl, 'Select a target account.', 'warn'); return; }

        const intentKey = newIntentKey();
        try {
            const res = await fetchWithRetry(
                '/api/transfers',
                { fromAccountId: accountId, toAccountId: toAccountId, amountCents: cents },
                intentKey,
            );
            if (!res.ok) { await handleErrorResponse(res, cardEl); return; }
            const data = await res.json();
            const fromState = getState(accountId);
            fromState.balanceCents = data.from.balanceCents;
            fromState.aggregateVersion = data.from.aggregateVersion;
            const toState = getState(toAccountId);
            toState.balanceCents = data.to.balanceCents;
            toState.aggregateVersion = data.to.aggregateVersion;
            amountInput.value = '';
            await Promise.all([fetchHistory(accountId), fetchHistory(toAccountId)]);
            showToast(cardEl, 'Transfer successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    // --- Watch (SSE) ---

    let watchEnabled = false;
    let eventSource = null;

    function startWatch() {
        if (eventSource) return;
        eventSource = new EventSource('/api/watch/stream');
        eventSource.onmessage = async (msg) => {
            const evt = JSON.parse(msg.data);
            const accountId = evt.aggregateId;
            if (!accounts.find(a => a.id === accountId)) return;
            await fetchHistory(accountId);
            render();
        };
        eventSource.onerror = () => {
            // EventSource auto-reconnects
        };
    }

    function stopWatch() {
        if (eventSource) {
            eventSource.close();
            eventSource = null;
        }
    }

    function toggleWatch(enabled) {
        watchEnabled = enabled;
        if (enabled) {
            startWatch();
        } else {
            stopWatch();
        }
        render();
    }

    async function doRefresh(accountId, cardEl) {
        try {
            await fetchHistory(accountId);
            render();
            showToast(cardEl, 'Refreshed.', 'success');
        } catch (e) {
            showToast(cardEl, 'Refresh failed.', 'error');
        }
    }

    function buildCard(account) {
        const s = getState(account.id);
        const others = accounts.filter(a => a.id !== account.id);

        const card = document.createElement('div');
        card.className = 'account-card';
        card.dataset.accountId = account.id;

        // History items (newest first)
        let historyHtml = '';
        const reversed = [...s.events].reverse();
        for (const e of reversed) {
            const label = eventLabel(e);
            historyHtml += `<div class="history-item">
                <span>${label.text}</span>
                <span class="amount ${label.cls}">${formatCents(label.amount)}</span>
                <span class="meta">v${e.aggregateVersion}</span>
            </div>`;
        }

        card.innerHTML = `
            <div class="balance-row">
                <h3>${account.name}</h3>
                <button class="btn-refresh" data-action="refresh">Refresh</button>
            </div>
            <div class="balance-row">
                <span class="balance">${formatCents(s.balanceCents)}</span>
                <span class="stream-pos">stream pos: ${s.aggregateVersion}</span>
            </div>
            <div class="actions">
                <input type="text" class="amount-input" placeholder="$0.00">
                <button data-action="deposit" class="btn-accent">Deposit</button>
                <button data-action="withdraw">Withdraw</button>
            </div>
            <div class="transfer-row">
                <label>Transfer</label>
                <input type="text" class="transfer-amount" placeholder="$0.00">
                <label>to</label>
                <select class="transfer-target">
                    ${others.map(a => `<option value="${a.id}">${a.name}</option>`).join('')}
                </select>
                <button data-action="transfer">Transfer</button>
            </div>
            <div class="toast"></div>
            <div class="history">
                <h4>Transaction History</h4>
                <div class="history-list">${historyHtml || '<div style="color:var(--text-dim);font-size:0.75rem;">No events yet</div>'}</div>
            </div>
        `;

        card.querySelector('[data-action="refresh"]').onclick = () => doRefresh(account.id, card);
        card.querySelector('[data-action="deposit"]').onclick = (e) => withBusy(e.currentTarget, () => doDeposit(account.id, card));
        card.querySelector('[data-action="withdraw"]').onclick = (e) => withBusy(e.currentTarget, () => doWithdraw(account.id, card));
        card.querySelector('[data-action="transfer"]').onclick = (e) => withBusy(e.currentTarget, () => doTransfer(account.id, card));

        return card;
    }

    function render() {
        const app = document.getElementById('app');
        app.innerHTML = '';

        const watchBar = document.createElement('div');
        watchBar.className = 'watch-bar';
        watchBar.innerHTML = `<button class="btn-watch ${watchEnabled ? 'active' : ''}">Watch</button>`;
        watchBar.querySelector('.btn-watch').onclick = () => toggleWatch(!watchEnabled);
        app.appendChild(watchBar);

        for (const account of accounts) {
            app.appendChild(buildCard(account));
        }
    }

    async function init() {
        const res = await fetch('/api/accounts');
        const data = await res.json();
        accounts = data.accounts;

        // Initial load: fetch balance + history for every account
        await Promise.all(accounts.map(a => fetchHistory(a.id)));
        render();
    }

    init();
})();
