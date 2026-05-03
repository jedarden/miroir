// Miroir Admin UI - SPA
(function() {
    'use strict';

    // Configuration
    const API_BASE = '/_miroir';
    const SESSION_CHECK_INTERVAL = 60000; // 1 minute

    // State
    let currentRoute = '/';
    let sessionValid = false;

    // Router
    const routes = {
        '/': renderOverview,
        '/topology': renderTopology,
        '/indexes': renderIndexes,
        '/aliases': renderAliases,
        '/documents': renderDocuments,
        '/tasks': renderTasks,
        '/settings': renderSettings
    };

    // Initialize
    function init() {
        checkSession();
        setupEventListeners();
        setupRouter();
        loadVersion();
        setInterval(checkSession, SESSION_CHECK_INTERVAL);
    }

    // Session management
    async function checkSession() {
        try {
            const response = await fetch(`${API_BASE}/admin/session`, {
                credentials: 'include'
            });

            if (response.ok) {
                const data = await response.json();
                sessionValid = data.valid;

                if (!sessionValid) {
                    window.location.href = '/_miroir/admin/login.html';
                    return false;
                }
                return true;
            } else {
                window.location.href = '/_miroir/admin/login.html';
                return false;
            }
        } catch (error) {
            console.error('Session check failed:', error);
            window.location.href = '/_miroir/admin/login.html';
            return false;
        }
    }

    // API helper
    async function apiFetch(endpoint, options = {}) {
        const url = endpoint.startsWith('/') ? `${API_BASE}${endpoint}` : endpoint;
        const response = await fetch(url, {
            ...options,
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            }
        });

        if (!response.ok) {
            throw new Error(`API error: ${response.status} ${response.statusText}`);
        }

        return response.json();
    }

    // Router setup
    function setupRouter() {
        window.addEventListener('hashchange', handleRoute);
        handleRoute();
    }

    async function handleRoute() {
        const hash = window.location.hash.slice(1) || '/';
        const [path, queryString] = hash.split('?');
        const route = routes[path] || routes['/'];

        if (route) {
            currentRoute = path;
            await route();
            updateNav();
        }
    }

    function updateNav() {
        document.querySelectorAll('.nav-link').forEach(link => {
            const href = link.getAttribute('href');
            if (href === `#${currentRoute}`) {
                link.classList.add('active');
            } else {
                link.classList.remove('active');
            }
        });
    }

    // Event listeners
    function setupEventListeners() {
        document.getElementById('logoutBtn').addEventListener('click', handleLogout);
    }

    async function handleLogout() {
        try {
            await apiFetch('/admin/logout', { method: 'POST' });
            window.location.href = '/_miroir/admin/login.html';
        } catch (error) {
            console.error('Logout failed:', error);
        }
    }

    async function loadVersion() {
        try {
            const data = await apiFetch('/metrics');
            document.getElementById('version').textContent = data.version || '';
        } catch (error) {
            console.error('Failed to load version:', error);
        }
    }

    // Render functions
    async function renderOverview() {
        const content = document.getElementById('content');

        try {
            const [topology, ready, metrics] = await Promise.all([
                apiFetch('/topology'),
                apiFetch('/ready'),
                apiFetch('/metrics')
            ]);

            const degradedCount = topology.shards.filter(s => !s.healthy).length;

            content.innerHTML = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Nodes</div>
                        <div class="stat-value">${topology.nodes.length}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Shards</div>
                        <div class="stat-value">${topology.shards}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Degraded Shards</div>
                        <div class="stat-value" style="color: ${degradedCount > 0 ? 'var(--error-color)' : 'var(--success-color)'}">${degradedCount}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Status</div>
                        <div class="stat-value" style="font-size: 1.5rem">
                            <span class="badge ${ready.ready ? 'badge-success' : 'badge-warning'}">${ready.ready ? 'Ready' : 'Not Ready'}</span>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Cluster Health</h2>
                    </div>
                    <div class="card-body">
                        <p>Replica Groups: ${topology.replica_groups}</p>
                        <p>Replication Factor: ${topology.replication_factor}</p>
                        <p>Ready: ${ready.ready ? 'Yes' : 'No'}</p>
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderTopology() {
        const content = document.getElementById('content');

        try {
            const topology = await apiFetch('/topology');

            const rows = topology.nodes.map(node => {
                const statusClass = node.is_healthy() ? 'badge-success' : 'badge-error';
                return `
                    <tr>
                        <td>${node.id}</td>
                        <td>${node.address}</td>
                        <td>${node.group}</td>
                        <td><span class="badge ${statusClass}">${node.status}</span></td>
                    </tr>
                `;
            }).join('');

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Topology</h2>
                    </div>
                    <div class="card-body">
                        <div class="table-container">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Node ID</th>
                                        <th>Address</th>
                                        <th>Group</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${rows}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderIndexes() {
        const content = document.getElementById('content');

        try {
            const stats = await apiFetch('/stats');
            const indexes = stats.indexes || {};

            const rows = Object.entries(indexes).map(([name, info]) => `
                <tr>
                    <td>${name}</td>
                    <td>${info.numberOfDocuments || 0}</td>
                    <td>${info.isIndexing ? 'Yes' : 'No'}</td>
                    <td>
                        <button class="btn btn-sm btn-secondary" onclick="window.location.hash='#/documents?index=${name}'">View</button>
                    </td>
                </tr>
            `).join('');

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Indexes</h2>
                    </div>
                    <div class="card-body">
                        ${rows ? `
                            <div class="table-container">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Name</th>
                                            <th>Documents</th>
                                            <th>Indexing</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${rows}
                                    </tbody>
                                </table>
                            </div>
                        ` : '<div class="empty-state"><div class="empty-state-title">No indexes found</div></div>'}
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderAliases() {
        const content = document.getElementById('content');

        try {
            const aliases = await apiFetch('/aliases');

            const rows = aliases.map(alias => `
                <tr>
                    <td>${alias.name}</td>
                    <td>${alias.indexUid}</td>
                    <td><span class="badge badge-info">${alias.kind || 'single'}</span></td>
                    <td>${alias.createdAt ? new Date(alias.createdAt).toLocaleString() : 'N/A'}</td>
                    <td>${alias.updatedAt ? new Date(alias.updatedAt).toLocaleString() : 'N/A'}</td>
                </tr>
            `).join('');

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Aliases</h2>
                    </div>
                    <div class="card-body">
                        ${rows ? `
                            <div class="table-container">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Name</th>
                                            <th>Target Index</th>
                                            <th>Kind</th>
                                            <th>Created</th>
                                            <th>Updated</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${rows}
                                    </tbody>
                                </table>
                            </div>
                        ` : '<div class="empty-state"><div class="empty-state-title">No aliases found</div></div>'}
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderDocuments() {
        const content = document.getElementById('content');

        // Get index from query string
        const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
        const index = params.get('index');

        if (!index) {
            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Documents</h2>
                    </div>
                    <div class="card-body">
                        <p>Select an index from the <a href="#/indexes">Indexes</a> page to view documents.</p>
                    </div>
                </div>
            `;
            return;
        }

        content.innerHTML = `
            <div class="card">
                <div class="card-header">
                    <h2 class="card-title">Documents: ${index}</h2>
                </div>
                <div class="card-body">
                    <div class="loading">
                        <div class="spinner"></div>
                    </div>
                </div>
            </div>
        `;

        try {
            const data = await apiFetch(`/stats`);
            const indexStats = data.indexes?.[index];

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Documents: ${index}</h2>
                    </div>
                    <div class="card-body">
                        <p>Document count: ${indexStats?.numberOfDocuments || 0}</p>
                        <p style="margin-top: 1rem;">Document browser is a placeholder - full implementation would query documents from the index.</p>
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderTasks() {
        const content = document.getElementById('content');

        content.innerHTML = `
            <div class="card">
                <div class="card-header">
                    <h2 class="card-title">Tasks</h2>
                </div>
                <div class="card-body">
                    <div class="loading">
                        <div class="spinner"></div>
                    </div>
                </div>
            </div>
        `;

        try {
            const tasks = await apiFetch('/tasks?limit=20');

            const rows = tasks.map(task => `
                <tr>
                    <td>${task.uid}</td>
                    <td><span class="badge badge-${task.status === 'succeeded' ? 'success' : task.status === 'failed' ? 'error' : 'info'}">${task.status}</span></td>
                    <td>${task.type}</td>
                    <td>${task.enqueuedAt ? new Date(task.enqueuedAt).toLocaleString() : 'N/A'}</td>
                </tr>
            `).join('');

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Recent Tasks</h2>
                    </div>
                    <div class="card-body">
                        ${rows ? `
                            <div class="table-container">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>UID</th>
                                            <th>Status</th>
                                            <th>Type</th>
                                            <th>Enqueued</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${rows}
                                    </tbody>
                                </table>
                            </div>
                        ` : '<div class="empty-state"><div class="empty-state-title">No tasks found</div></div>'}
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    async function renderSettings() {
        const content = document.getElementById('content');

        content.innerHTML = `
            <div class="card">
                <div class="card-header">
                    <h2 class="card-title">Settings</h2>
                </div>
                <div class="card-body">
                    <div class="loading">
                        <div class="spinner"></div>
                    </div>
                </div>
            </div>
        `;

        try {
            const topology = await apiFetch('/topology');

            content.innerHTML = `
                <div class="card">
                    <div class="card-header">
                        <h2 class="card-title">Cluster Configuration</h2>
                    </div>
                    <div class="card-body">
                        <p><strong>Shards:</strong> ${topology.shards}</p>
                        <p><strong>Replica Groups:</strong> ${topology.replica_groups}</p>
                        <p><strong>Replication Factor:</strong> ${topology.replication_factor}</p>
                        <p style="margin-top: 1rem; color: var(--text-secondary);">Settings management is a placeholder - full implementation would allow editing cluster configuration.</p>
                    </div>
                </div>
            `;
        } catch (error) {
            renderError(error);
        }
    }

    function renderError(error) {
        const content = document.getElementById('content');
        content.innerHTML = `
            <div class="error">
                <strong>Error:</strong> ${error.message}
            </div>
        `;
    }

    // Start the app
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
