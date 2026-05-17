(function () {
    'use strict';

    let accounts = [];
    let clients = [];

    // Per-client, per-account state: state[clientIdx][accountId]
    const state = [{}, {}];

    // Watch state per client column
    const watchEnabled = [false, false];
    let eventSource = null;

    function getState(ci, accountId) {
        if (!state[ci][accountId]) {
            state[ci][accountId] = { batches: [], currentAggregateVersion: 0, balance: 0 };
        }
        return state[ci][accountId];
    }

    function formatCents(cents) {
        const negative = cents < 0;
        const abs = Math.abs(cents);
        return (negative ? '-' : '') + '$' + (abs / 100).toFixed(2);
    }

    function eventLabel(e) {
        switch (e.eventTypeMajor) {
            case 1: return { text: 'Deposit', amount: e.amountCents, cls: 'positive' };
            case 2: return { text: 'Withdrawal', amount: -e.amountCents, cls: 'negative' };
            case 3: {
                const toName = accounts.find(a => a.id === e.toAccountId)?.name || '?';
                return { text: 'Transfer to ' + toName, amount: -e.amountCents, cls: 'negative' };
            }
            case 4: {
                const fromName = accounts.find(a => a.id === e.fromAccountId)?.name || '?';
                return { text: 'Transfer from ' + fromName, amount: e.amountCents, cls: 'positive' };
            }
            default: return { text: 'Unknown', amount: 0, cls: '' };
        }
    }

    function reproject(ci, accountId) {
        const s = getState(ci, accountId);
        let balance = 0;
        for (const batch of s.batches) {
            for (const e of batch.events) {
                switch (e.eventTypeMajor) {
                    case 1: balance += e.amountCents; break;
                    case 2: balance -= e.amountCents; break;
                    case 3: balance -= e.amountCents; break;
                    case 4: balance += e.amountCents; break;
                }
            }
        }
        s.balance = balance;
    }

    // Serialize concurrent fetches per (ci, accountId) to prevent race conditions
    const fetchLocks = {};

    async function fetchEvents(ci, accountId) {
        const lockKey = `${ci}:${accountId}`;
        const prev = fetchLocks[lockKey] || Promise.resolve();
        const next = prev.then(() => doFetchEvents(ci, accountId), () => doFetchEvents(ci, accountId));
        fetchLocks[lockKey] = next;
        return next;
    }

    async function doFetchEvents(ci, accountId) {
        const s = getState(ci, accountId);
        const from = s.currentAggregateVersion + 1;
        const res = await fetch(`/api/accounts/${accountId}/events?fromVersion=${from}`);
        const data = await res.json();
        if (data.batches && data.batches.length > 0) {
            s.batches.push(...data.batches);
            s.currentAggregateVersion = data.batches[data.batches.length - 1].aggregateVersion;
        }
        reproject(ci, accountId);
    }

    function parseCents(input) {
        const val = parseFloat(input);
        if (isNaN(val) || val <= 0) return null;
        return Math.round(val * 100);
    }

    function showToast(cardEl, msg, type) {
        const toast = cardEl.querySelector('.toast');
        toast.textContent = msg;
        toast.className = 'toast ' + type;
        setTimeout(() => { toast.className = 'toast'; }, 5000);
    }

    async function doDeposit(ci, accountId, cardEl) {
        const input = cardEl.querySelector('.amount-input');
        const cents = parseCents(input.value);
        if (!cents) { showToast(cardEl, 'Enter a valid amount.', 'warn'); return; }

        const s = getState(ci, accountId);
        const body = {
            clientId: clients[ci].id,
            amountCents: cents,
            expectedAggregateVersion: s.currentAggregateVersion,
        };

        try {
            const res = await fetch(`/api/accounts/${accountId}/deposit`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            if (res.status === 409) {
                const err = await res.json();
                showToast(cardEl, 'OCC conflict! ' + err.message, 'error');
                return;
            }
            if (!res.ok) { showToast(cardEl, 'Server error.', 'error'); return; }
            input.value = '';
            await fetchEvents(ci, accountId);
            showToast(cardEl, 'Deposit successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    async function doWithdraw(ci, accountId, cardEl) {
        const input = cardEl.querySelector('.amount-input');
        const cents = parseCents(input.value);
        if (!cents) { showToast(cardEl, 'Enter a valid amount.', 'warn'); return; }

        const s = getState(ci, accountId);
        if (s.balance - cents < 0) {
            showToast(cardEl, 'Insufficient funds. Balance: ' + formatCents(s.balance), 'warn');
            return;
        }

        const body = {
            clientId: clients[ci].id,
            amountCents: cents,
            expectedAggregateVersion: s.currentAggregateVersion,
        };

        try {
            const res = await fetch(`/api/accounts/${accountId}/withdraw`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            if (res.status === 409) {
                const err = await res.json();
                showToast(cardEl, 'OCC conflict! ' + err.message, 'error');
                return;
            }
            if (!res.ok) { showToast(cardEl, 'Server error.', 'error'); return; }
            input.value = '';
            await fetchEvents(ci, accountId);
            showToast(cardEl, 'Withdrawal successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    async function doTransfer(ci, accountId, cardEl) {
        const amountInput = cardEl.querySelector('.transfer-amount');
        const selectEl = cardEl.querySelector('.transfer-target');
        const cents = parseCents(amountInput.value);
        if (!cents) { showToast(cardEl, 'Enter a valid transfer amount.', 'warn'); return; }

        const toAccountId = selectEl.value;
        if (!toAccountId) { showToast(cardEl, 'Select a target account.', 'warn'); return; }

        const fromState = getState(ci, accountId);
        if (fromState.balance - cents < 0) {
            showToast(cardEl, 'Insufficient funds. Balance: ' + formatCents(fromState.balance), 'warn');
            return;
        }

        const toState = getState(ci, toAccountId);
        const body = {
            clientId: clients[ci].id,
            fromAccountId: accountId,
            toAccountId: toAccountId,
            amountCents: cents,
            expectedFromVersion: fromState.currentAggregateVersion,
            expectedToVersion: toState.currentAggregateVersion,
        };

        try {
            const res = await fetch('/api/transfers', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            if (res.status === 409) {
                const err = await res.json();
                showToast(cardEl, 'OCC conflict! ' + err.message, 'error');
                return;
            }
            if (!res.ok) { showToast(cardEl, 'Server error.', 'error'); return; }
            amountInput.value = '';
            await fetchEvents(ci, accountId);
            await fetchEvents(ci, toAccountId);
            showToast(cardEl, 'Transfer successful.', 'success');
            render();
        } catch (e) {
            showToast(cardEl, 'Network error.', 'error');
        }
    }

    async function doRefresh(ci, accountId, cardEl) {
        try {
            await fetchEvents(ci, accountId);
            render();
            showToast(cardEl, 'Refreshed.', 'success');
        } catch (e) {
            showToast(cardEl, 'Refresh failed.', 'error');
        }
    }

    // --- Watch (SSE) ---

    function startWatch() {
        if (eventSource) return;
        eventSource = new EventSource('/api/watch/stream');
        eventSource.onmessage = async (msg) => {
            const evt = JSON.parse(msg.data);
            const accountId = evt.aggregateId;
            if (!accounts.find(a => a.id === accountId)) return;

            for (let ci = 0; ci < clients.length; ci++) {
                if (!watchEnabled[ci]) continue;
                await fetchEvents(ci, accountId);
            }
            render();
        };
        eventSource.onerror = () => {
            // EventSource auto-reconnects; nothing to do
        };
    }

    function stopWatchIfUnneeded() {
        if (!watchEnabled.some(Boolean) && eventSource) {
            eventSource.close();
            eventSource = null;
        }
    }

    function toggleWatch(ci, enabled) {
        watchEnabled[ci] = enabled;
        if (enabled) {
            startWatch();
        } else {
            stopWatchIfUnneeded();
        }
        render();
    }

    function buildCard(ci, account) {
        const s = getState(ci, account.id);
        const others = accounts.filter(a => a.id !== account.id);

        const card = document.createElement('div');
        card.className = 'account-card';
        card.dataset.ci = ci;
        card.dataset.accountId = account.id;

        // History items (newest first for display)
        let historyHtml = '';
        const allBatches = [...s.batches].reverse();
        for (const batch of allBatches) {
            for (const e of batch.events) {
                const label = eventLabel(e);
                historyHtml += `<div class="history-item">
                    <span>${label.text}</span>
                    <span class="amount ${label.cls}">${formatCents(label.amount)}</span>
                    <span class="meta">v${batch.aggregateVersion}</span>
                </div>`;
            }
        }

        card.innerHTML = `
            <div class="balance-row">
                <h3>${account.name}</h3>
                <button class="btn-refresh" data-action="refresh">Refresh</button>
            </div>
            <div class="balance-row">
                <span class="balance">${formatCents(s.balance)}</span>
                <span class="stream-pos">stream pos: ${s.currentAggregateVersion}</span>
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

        // Wire up buttons
        card.querySelector('[data-action="refresh"]').onclick = () => doRefresh(ci, account.id, card);
        card.querySelector('[data-action="deposit"]').onclick = () => doDeposit(ci, account.id, card);
        card.querySelector('[data-action="withdraw"]').onclick = () => doWithdraw(ci, account.id, card);
        card.querySelector('[data-action="transfer"]').onclick = () => doTransfer(ci, account.id, card);

        return card;
    }

    function render() {
        const app = document.getElementById('app');
        app.innerHTML = '';

        for (let ci = 0; ci < clients.length; ci++) {
            const col = document.createElement('div');
            col.className = 'machine-column';

            const header = document.createElement('div');
            header.className = 'machine-header';

            const watchActive = watchEnabled[ci];
            header.innerHTML = `
                <h2>${clients[ci].name}</h2>
                <div class="uuid">${clients[ci].id}</div>
                <button class="btn-watch ${watchActive ? 'active' : ''}" data-ci="${ci}">Watch</button>
            `;

            const watchBtn = header.querySelector('.btn-watch');
            watchBtn.onclick = () => toggleWatch(ci, !watchEnabled[ci]);

            col.appendChild(header);

            for (const account of accounts) {
                col.appendChild(buildCard(ci, account));
            }
            app.appendChild(col);
        }
    }

    async function init() {
        const res = await fetch('/api/accounts');
        const data = await res.json();
        accounts = data.accounts;
        clients = data.clients;

        // Initial load: fetch all events for every account x client
        const promises = [];
        for (let ci = 0; ci < clients.length; ci++) {
            for (const account of accounts) {
                promises.push(fetchEvents(ci, account.id));
            }
        }
        await Promise.all(promises);
        render();
    }

    init();
})();
