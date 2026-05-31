// Miroir Search UI - SPA
(function() {
    'use strict';

    // Configuration
    const DEBOUNCE_MS = 150;
    const RESULTS_PER_PAGE = 20;

    // Parse embed/headless modes from URL (plan §13.21)
    const urlParams = new URLSearchParams(window.location.search);
    const isEmbed = urlParams.get('embed') === 'true';
    const isHeadless = urlParams.get('headless') === 'true';

    // State
    let currentIndex = null;
    let sessionToken = null;
    let currentQuery = '';
    let currentFilters = {};
    let currentPage = 0;
    let currentSort = null;
    let currentPerPage = 20;
    let debounceTimer = null;
    let config = null;
    let ignoreUrlUpdate = false;
    let focusedResultIndex = -1;
    let searchStartTime = 0;
    let sessionId = crypto.randomUUID();

    // Apply embed mode classes (plan §13.21)
    if (isEmbed || isHeadless) {
        document.body.classList.add('embed-mode');
    }
    if (isHeadless) {
        document.body.classList.add('headless-mode');
    }

    // Initialize
    function init() {
        const indexMatch = window.location.pathname.match(/\/ui\/search\/([^/]+)/);
        if (!indexMatch) {
            showError('No index specified');
            return;
        }

        currentIndex = indexMatch[1];

        // Parse URL state for initial search parameters
        const urlState = parseUrlState();
        currentQuery = urlState.query;
        currentFilters = urlState.filters;
        currentSort = urlState.sort;
        currentPage = urlState.page;

        setupEventListeners();
        setupDarkMode();
        loadSession();

        // Handle browser back/forward
        window.addEventListener('popstate', (event) => {
            ignoreUrlUpdate = true;
            if (event.state) {
                currentQuery = event.state.query || '';
                currentFilters = event.state.filters || {};
                currentSort = event.state.sort;
                currentPage = event.state.page || 0;
            } else {
                const state = parseUrlState();
                currentQuery = state.query;
                currentFilters = state.filters;
                currentSort = state.sort;
                currentPage = state.page;
            }

            // Update UI and perform search
            document.getElementById('searchInput').value = currentQuery;
            performSearch(currentQuery, currentPage, false);
            setTimeout(() => { ignoreUrlUpdate = false; }, 100);
        });
    }

    // Dark mode toggle (plan §13.21)
    function setupDarkMode() {
        const toggle = document.getElementById('darkModeToggle');

        // Check for saved preference or system preference
        const savedTheme = localStorage.getItem('search-ui-theme');
        const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

        let initialTheme = 'light';
        if (savedTheme) {
            initialTheme = savedTheme;
        } else if (systemPrefersDark) {
            initialTheme = 'dark';
        }

        setTheme(initialTheme);

        toggle.addEventListener('click', () => {
            const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            setTheme(newTheme);
        });
    }

    function setTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('search-ui-theme', theme);
    }

    // Session management
    async function loadSession() {
        try {
            const response = await fetch(`/_miroir/ui/search/${currentIndex}/session`);
            if (!response.ok) {
                throw new Error('Failed to get session');
            }

            const data = await response.json();
            sessionToken = data.token;

            // Load config after session
            await loadConfig();

            // Update UI
            document.getElementById('logo').textContent = config?.title || 'Search';

            // Set initial search input value from URL state
            document.getElementById('searchInput').value = currentQuery;

            // Perform initial search if we have URL state
            if (currentQuery || Object.keys(currentFilters).length > 0) {
                ignoreUrlUpdate = true;
                await performSearch(currentQuery, currentPage, false);
                setTimeout(() => { ignoreUrlUpdate = false; }, 100);
            }

        } catch (error) {
            showError('Failed to initialize search: ' + error.message);
        }
    }

    async function loadConfig() {
        try {
            const response = await fetch(`/_miroir/ui/search/${currentIndex}/config`);
            if (response.ok) {
                config = await response.json();

                // Populate sort options from config (plan §13.21)
                populateSortOptions();

                // Set per-page options from config
                if (config.per_page_options) {
                    const perPageSelect = document.getElementById('perPageSelect');
                    perPageSelect.innerHTML = config.per_page_options.map(
                        size => `<option value="${size}">${size}</option>`
                    ).join('');

                    if (config.per_page_default) {
                        perPageSelect.value = config.per_page_default;
                        currentPerPage = config.per_page_default;
                    }
                }

                // Apply accent color if configured (for embed mode)
                if (config.accent_color && (isEmbed || isHeadless)) {
                    document.documentElement.style.setProperty('--accent-color', config.accent_color);
                }
            }
        } catch (error) {
            console.warn('Failed to load config:', error);
        }
    }

    function populateSortOptions() {
        if (!config?.sort_options) return;

        const sortSelect = document.getElementById('sortSelect');
        sortSelect.innerHTML = config.sort_options.map(opt => {
            const value = opt.field || '';
            const label = opt.label || 'Relevance';
            const selected = value === currentSort ? 'selected' : '';
            return `<option value="${value}" ${selected}>${escapeHtml(label)}</option>`;
        }).join('');
    }

    // Canonicalize JSON by sorting object keys recursively (plan §13.10)
    function canonicalJson(obj) {
        if (obj === null || typeof obj !== 'object') {
            return JSON.stringify(obj);
        }
        if (Array.isArray(obj)) {
            return '[' + obj.map(canonicalJson).join(',') + ']';
        }
        const sortedKeys = Object.keys(obj).sort();
        return '{' + sortedKeys.map(k => `"${k}":${canonicalJson(obj[k])}`).join(',') + '}';
    }

    // Generate per-query idempotency key (plan §13.10, §13.21)
    // Hash of index + normalized query body for query coalescing
    function generateIdempotencyKey(query, filters, page, sort, perPage) {
        const requestBody = {
            q: query,
            limit: perPage || currentPerPage || RESULTS_PER_PAGE,
            offset: page * (perPage || currentPerPage || RESULTS_PER_PAGE),
            attributesToRetrieve: ['*'],
            attributesToHighlight: config?.display_attributes || ['*'],
            facets: config?.facets?.map(f => f.attribute) || []
        };

        // Add filters
        const filterParts = [];
        for (const [key, values] of Object.entries(filters)) {
            if (Array.isArray(values) && values.length > 0) {
                filterParts.push(`${key} IN ${JSON.stringify(values)}`);
            }
        }
        if (filterParts.length > 0) {
            requestBody.filter = filterParts.join(' AND ');
        }

        // Add sort
        if (sort) {
            requestBody.sort = [sort];
        }

        // Canonicalize and hash
        const canonical = `${currentIndex}:${canonicalJson(requestBody)}`;
        let hash = 0;
        for (let i = 0; i < canonical.length; i++) {
            const char = canonical.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash; // Convert to 32-bit integer
        }
        return `search-${Math.abs(hash).toString(16)}`;
    }

    // API helper
    async function search(query, filters = {}, page = 0, sort = null, perPage = null) {
        const requestBody = {
            q: query,
            limit: perPage || currentPerPage || RESULTS_PER_PAGE,
            offset: page * (perPage || currentPerPage || RESULTS_PER_PAGE),
            attributesToRetrieve: ['*'],
            attributesToHighlight: config?.display_attributes || ['*'],
            facets: config?.facets?.map(f => f.attribute) || []
        };

        // Add filters
        const filterParts = [];
        for (const [key, values] of Object.entries(filters)) {
            if (Array.isArray(values) && values.length > 0) {
                filterParts.push(`${key} IN ${JSON.stringify(values)}`);
            }
        }
        if (filterParts.length > 0) {
            requestBody.filter = filterParts.join(' AND ');
        }

        // Add sort
        if (sort) {
            requestBody.sort = [sort];
        }

        // Generate idempotency key for query coalescing (plan §13.10, §13.21)
        const idempotencyKey = generateIdempotencyKey(query, filters, page, sort, perPage);

        const response = await fetch(`/indexes/${currentIndex}/search`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${sessionToken}`,
                'Idempotency-Key': idempotencyKey
            },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            throw new Error(`Search failed: ${response.status}`);
        }

        return response.json();
    }

    // Event listeners
    function setupEventListeners() {
        const searchInput = document.getElementById('searchInput');
        const searchBtn = document.getElementById('searchBtn');

        // Debounced search on input
        searchInput.addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                performSearch(e.target.value, 0);
            }, DEBOUNCE_MS);
        });

        // Search button click
        searchBtn.addEventListener('click', () => {
            performSearch(searchInput.value, 0);
        });

        // Sort change
        document.getElementById('sortSelect').addEventListener('change', (e) => {
            currentSort = e.target.value || null;
            currentPage = 0;
            performSearch(currentQuery, 0);
        });

        // Per-page change
        document.getElementById('perPageSelect').addEventListener('change', (e) => {
            currentPerPage = parseInt(e.target.value, 10);
            currentPage = 0;
            performSearch(currentQuery, 0);
        });

        // Facet toggle for mobile (plan §13.21 - bottom-sheet drawer)
        const facetToggle = document.getElementById('facetToggle');
        if (facetToggle) {
            facetToggle.addEventListener('click', () => {
                const facets = document.getElementById('facets');
                facets.classList.toggle('facets-open');
                facetToggle.classList.toggle('active');
            });
        }

        // Close facets when clicking outside on mobile
        document.addEventListener('click', (e) => {
            const facets = document.getElementById('facets');
            const facetToggle = document.getElementById('facetToggle');
            if (window.innerWidth <= 640 &&
                facets.classList.contains('facets-open') &&
                !facets.contains(e.target) &&
                !facetToggle.contains(e.target)) {
                facets.classList.remove('facets-open');
                facetToggle.classList.remove('active');
            }
        });

        // Enter key
        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                performSearch(searchInput.value, 0);
            }
        });

        // Focus search on slash key
        document.addEventListener('keydown', (e) => {
            if (e.key === '/' && document.activeElement !== searchInput) {
                e.preventDefault();
                searchInput.focus();
            }

            // Keyboard navigation for results (plan §13.21)
            if (document.activeElement === searchInput || document.activeElement === document.body) {
                const results = document.querySelectorAll('.result-card');

                switch (e.key) {
                    case 'ArrowDown':
                        e.preventDefault();
                        focusedResultIndex = Math.min(focusedResultIndex + 1, results.length - 1);
                        highlightResult(results, focusedResultIndex);
                        break;

                    case 'ArrowUp':
                        e.preventDefault();
                        focusedResultIndex = Math.max(focusedResultIndex - 1, 0);
                        highlightResult(results, focusedResultIndex);
                        break;

                    case 'Enter':
                        if (focusedResultIndex >= 0 && results[focusedResultIndex]) {
                            e.preventDefault();
                            const link = results[focusedResultIndex].querySelector('.result-title a');
                            if (link) {
                                link.click();
                            }
                        } else {
                            // If no result focused, perform search
                            performSearch(searchInput.value, 0);
                        }
                        break;

                    case 'Escape':
                        e.preventDefault();
                        searchInput.value = '';
                        currentQuery = '';
                        focusedResultIndex = -1;
                        clearResultHighlights(results);
                        updateUrl('', {}, currentSort, 0);
                        break;
                }
            }
        });
    }

    // Perform search
    async function performSearch(query, page, updateUrlState = true) {
        currentQuery = query;
        currentPage = page;

        const resultsDiv = document.getElementById('results');

        // Show skeleton loaders instead of spinner (plan §13.21 - layout-shift-free)
        resultsDiv.innerHTML = Array(3).fill(0).map(() => `
            <div class="skeleton-card">
                <div class="skeleton skeleton-title"></div>
                <div class="skeleton skeleton-text"></div>
                <div class="skeleton skeleton-text-short"></div>
                <div class="skeleton skeleton-meta"></div>
            </div>
        `).join('');

        // Update height for embed mode (plan §13.21)
        if (isEmbed) {
            sendHeightUpdate();
        }

        try {
            const data = await search(query, currentFilters, page, currentSort, currentPerPage);
            renderResults(data);
            renderFacets(data);
            renderPagination(data);
            updateResultCount(data);
            updateActiveFilterCount();

            // Update URL state for bookmarkable searches (plan §13.21)
            if (updateUrlState) {
                updateUrl(query, currentFilters, currentSort, page);
            }

            // Update height after rendering (plan §13.21)
            if (isEmbed) {
                sendHeightUpdate();
            }
        } catch (error) {
            showError(error.message);
        }
    }

    // Render results with highlighting via _formatted (plan §13.21)
    function renderResults(data) {
        const resultsDiv = document.getElementById('results');

        if (!data.hits || data.hits.length === 0) {
            // Check for typo tolerance suggestions (plan §13.21)
            const didYouMean = data._meilisearch?.typoTolerance?.suggest;
            const emptyHtml = `
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <div class="empty-state-title">No results found</div>
                    <p>Try adjusting your search or filters</p>
                    ${didYouMean ? `<button class="did-you-mean-link" data-query="${escapeHtml(didYouMean)}">Did you mean: <strong>${escapeHtml(didYouMean)}</strong>?</button>` : ''}
                </div>
            `;
            resultsDiv.innerHTML = emptyHtml;

            // Add click handler for "did you mean" link
            const didYouMeanBtn = resultsDiv.querySelector('.did-you-mean-link');
            if (didYouMeanBtn) {
                didYouMeanBtn.addEventListener('click', () => {
                    const suggestedQuery = didYouMeanBtn.dataset.query;
                    document.getElementById('searchInput').value = suggestedQuery;
                    performSearch(suggestedQuery, 0);
                });
            }
            return;
        }

        // Use custom template if configured (plan §13.21)
        if (config?.result_template === 'custom' && config?.custom_template_html) {
            try {
                const resultsHtml = data.hits.map((hit, index) => {
                    return renderCustomResult(hit, index, config.custom_template_html);
                }).join('');
                resultsDiv.innerHTML = resultsHtml;

                // Add click tracking for custom templates
                resultsDiv.querySelectorAll('.result-card').forEach((card, index) => {
                    const link = card.querySelector('a[data-result-id]');
                    if (link) {
                        link.addEventListener('click', (e) => {
                            const resultId = link.dataset.resultId;
                            const position = parseInt(link.dataset.resultPosition, 10);
                            const url = link.href;
                            trackClickThrough(resultId, position);
                            sendResultClickEvent(resultId, position, url);
                        });
                    }
                });
                return;
            } catch (error) {
                console.warn('Failed to render custom template, falling back to default:', error);
            }
        }

        const resultsHtml = data.hits.map((hit, index) => {
            const formatted = hit._formatted || {};
            const titleAttr = config?.display_attributes?.[0] || 'title';
            const snippetAttr = config?.display_attributes?.[1] || 'description';

            // Use _formatted for highlighted terms (plan §13.21)
            const title = formatted[titleAttr] || hit[titleAttr] || hit.id || 'Untitled';
            const snippet = formatted[snippetAttr] || hit[snippetAttr] || '';

            const resultId = hit[config?.primary_key_field || 'id'] || hit.id || '';
            const url = config?.hit_url_template?.replace(
                `{${config?.primary_key_field || 'id'}}`,
                resultId
            ) || '#';

            return `
                <div class="result-card" data-result-index="${index}" data-result-id="${escapeHtml(resultId)}">
                    <div class="result-title">
                        <a href="${url}" data-result-id="${escapeHtml(resultId)}" data-result-position="${index}" target="_blank" rel="noopener">${title}</a>
                    </div>
                    ${snippet ? `<div class="result-snippet">${snippet}</div>` : ''}
                    <div class="result-meta">
                        <span>ID: ${escapeHtml(String(resultId))}</span>
                        ${hit._rankingScore ? `<span>Score: ${hit._rankingScore.toFixed(2)}</span>` : ''}
                    </div>
                </div>
            `;
        }).join('');

        resultsDiv.innerHTML = resultsHtml;

        // Add result index for keyboard navigation
        resultsDiv.querySelectorAll('.result-card').forEach((card, index) => {
            card.querySelector('[data-result-index]')?.setAttribute('data-result-index', index);
        });

        // Add click tracking for analytics (plan §13.21)
        resultsDiv.querySelectorAll('.result-title a').forEach(link => {
            link.addEventListener('click', (e) => {
                const resultId = link.dataset.resultId;
                const position = parseInt(link.dataset.resultPosition, 10);
                const url = link.href;
                trackClickThrough(resultId, position);

                // Send postMessage in embed mode (plan §13.21)
                sendResultClickEvent(resultId, position, url);
            });
        });
    }

    // Render a custom result template (plan §13.21)
    function renderCustomResult(hit, index, template) {
        const formatted = hit._formatted || {};
        const resultId = hit[config?.primary_key_field || 'id'] || hit.id || '';
        const url = config?.hit_url_template?.replace(
            `{${config?.primary_key_field || 'id'}}`,
            resultId
        ) || '#';

        // Build data object for template rendering
        const data = {
            ...hit,
            _formatted: formatted,
            _url: url,
            _index: index,
            _result_id: resultId,
        };

        // Render template with Handlebars-style interpolation
        let html = template;

        // Process {{#if}}...{{/if}} blocks
        html = html.replace(/\{\{#if\s+(\w+)\}\}([\s\S]*?)\{\{\/if\}\}/g, (match, field, content) => {
            const value = data[field];
            const isTruthy = value !== undefined && value !== null && value !== '' &&
                (typeof value !== 'boolean' || value === true) &&
                (typeof value !== 'number' || value !== 0) &&
                (typeof value !== 'object' || (Array.isArray(value) ? value.length > 0 : true));

            return isTruthy ? content : '';
        });

        // Process simple {{field}} tags
        html = html.replace(/\{\{(\w+)\}\}/g, (match, field) => {
            if (field.startsWith('_')) {
                // Meta fields
                return escapeHtml(String(data[field] || ''));
            }
            // Regular fields - prefer _formatted for highlighting
            const value = formatted[field] !== undefined ? formatted[field] : hit[field];
            return escapeHtml(String(value !== undefined ? value : ''));
        });

        // Add data attributes for click tracking
        html = html.replace(
            /<a\s+/gi,
            `<a data-result-id="${escapeHtml(resultId)}" data-result-position="${index}" `
        );

        return `<div class="result-card" data-result-index="${index}" data-result-id="${escapeHtml(resultId)}">${html}</div>`;
    }

    // Render facets
    function renderFacets(data) {
        const facetsDiv = document.getElementById('facets');

        if (!data.facetDistribution || Object.keys(data.facetDistribution).length === 0) {
            facetsDiv.innerHTML = '';
            return;
        }

        const facetsHtml = Object.entries(data.facetDistribution).map(([facetName, values]) => `
            <div class="facet-group">
                <div class="facet-title">${escapeHtml(facetName)}</div>
                ${Object.entries(values).slice(0, 10).map(([value, count]) => `
                    <label class="facet-option">
                        <input
                            type="checkbox"
                            data-facet="${escapeHtml(facetName)}"
                            data-value="${escapeHtml(value)}"
                            ${currentFilters[facetName]?.includes(value) ? 'checked' : ''}
                        >
                        <span>${escapeHtml(value)}</span>
                        <span class="facet-count">${count}</span>
                    </label>
                `).join('')}
            </div>
        `).join('');

        facetsDiv.innerHTML = facetsHtml;

        // Add event listeners to checkboxes
        facetsDiv.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                const facet = checkbox.dataset.facet;
                const value = checkbox.dataset.value;

                if (!currentFilters[facet]) {
                    currentFilters[facet] = [];
                }

                if (checkbox.checked) {
                    currentFilters[facet].push(value);
                } else {
                    currentFilters[facet] = currentFilters[facet].filter(v => v !== value);
                }

                performSearch(currentQuery, 0);
            });
        });
    }

    // Render pagination
    function renderPagination(data) {
        const paginationDiv = document.getElementById('pagination');
        const totalPages = Math.ceil((data.estimatedTotalHits || 0) / RESULTS_PER_PAGE);

        if (totalPages <= 1) {
            paginationDiv.innerHTML = '';
            return;
        }

        const currentPageNum = currentPage + 1;
        let pages = [];

        // Always show first page
        pages.push(1);

        // Show pages around current page
        for (let i = Math.max(2, currentPageNum - 2); i <= Math.min(totalPages - 1, currentPageNum + 2); i++) {
            pages.push(i);
        }

        // Always show last page
        if (totalPages > 1) {
            pages.push(totalPages);
        }

        // Remove duplicates and sort
        pages = [...new Set(pages)].sort((a, b) => a - b);

        const paginationHtml = pages.map((page, index) => {
            const prevPage = pages[index - 1];
            const showEllipsisBefore = prevPage && prevPage < page - 1;

            let html = '';

            if (showEllipsisBefore) {
                html += '<button disabled>...</button>';
            }

            const isActive = page === currentPageNum;
            html += `<button class="${isActive ? 'active' : ''}" data-page="${page - 1}">${page}</button>`;

            return html;
        }).join('');

        paginationDiv.innerHTML = paginationHtml;

        // Add event listeners
        paginationDiv.querySelectorAll('button:not(:disabled)').forEach(button => {
            button.addEventListener('click', () => {
                const page = parseInt(button.dataset.page);
                performSearch(currentQuery, page);
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });
        });
    }

    // Update result count
    function updateResultCount(data) {
        const count = data.estimatedTotalHits || 0;
        const time = data.processingTimeMs || 0;
        document.getElementById('resultCount').textContent =
            `${count.toLocaleString()} results (${time}ms)`;

        // Track search latency (plan §13.21)
        trackSearchLatency(time);
    }

    // Show error
    function showError(message) {
        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
    }

    // Escape HTML
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // URL State Management (plan §13.21)
    function parseUrlState() {
        const params = new URLSearchParams(window.location.search);
        return {
            query: params.get('q') || '',
            filters: parseFilters(params),
            sort: params.get('sort'),
            page: parseInt(params.get('page') || '0', 10)
        };
    }

    function parseFilters(params) {
        const filters = {};
        for (const [key, value] of params) {
            if (key.startsWith('f[') && key.endsWith(']')) {
                const facetName = key.slice(2, -1);
                if (!filters[facetName]) {
                    filters[facetName] = [];
                }
                filters[facetName].push(value);
            }
        }
        return filters;
    }

    function updateUrl(query, filters, sort, page) {
        if (ignoreUrlUpdate) return;

        const params = new URLSearchParams();
        if (query) params.set('q', query);
        if (sort) params.set('sort', sort);
        if (page > 0) params.set('page', page.toString());

        // Encode filters as f[facetName]=value
        for (const [facetName, values] of Object.entries(filters)) {
            if (Array.isArray(values)) {
                for (const value of values) {
                    params.append(`f[${facetName}]`, value);
                }
            }
        }

        const newUrl = params.toString() ? `?${params.toString()}` : window.location.pathname;
        window.history.replaceState({ query, filters, sort, page }, '', newUrl);
    }

    // Keyboard navigation helpers (plan §13.21)
    function highlightResult(results, index) {
        clearResultHighlights(results);

        if (index >= 0 && index < results.length) {
            const card = results[index];
            card.classList.add('result-focused');
            card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
    }

    function clearResultHighlights(results) {
        results.forEach(card => card.classList.remove('result-focused'));
    }

    // Update active filter count badge (plan §13.21)
    function updateActiveFilterCount() {
        const count = Object.values(currentFilters).reduce(
            (sum, values) => sum + (Array.isArray(values) ? values.length : 0),
            0
        );
        const badge = document.getElementById('activeFilterCount');
        if (badge) {
            badge.textContent = count > 0 ? count.toString() : '';
        }
    }

    // Analytics beacon (plan §13.21)
    async function sendBeacon(type, eventData) {
        if (!config?.analytics_enabled) return;

        const eventId = crypto.randomUUID();
        const beaconData = {
            type,
            event_id: eventId,
            query: currentQuery,
            index: currentIndex,
            timestamp: Date.now(),
            ...eventData
        };

        // Send beacon with idempotency key (plan §13.21)
        try {
            await fetch(`/_miroir/ui/search/${currentIndex}/beacon`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${sessionToken}`
                },
                body: JSON.stringify(beaconData),
                keepalive: true
            });
        } catch (error) {
            console.warn('Failed to send analytics beacon:', error);
        }
    }

    // Track search latency
    function trackSearchLatency(processingTimeMs) {
        sendBeacon('latency', {
            duration_ms: processingTimeMs,
            result_count: document.querySelectorAll('.result-card').length
        });
    }

    // Track click-through
    function trackClickThrough(resultId, position) {
        sendBeacon('click_through', {
            result_id: resultId,
            position
        });
    }

    // Embed mode: send height update to parent frame (plan §13.21)
    function sendHeightUpdate() {
        if (!isEmbed) return;

        const height = document.body.scrollHeight;
        window.parent.postMessage({
            type: 'miroir-search:resize',
            height: height,
            index: currentIndex
        }, '*');

        // Also send result count for convenience
        const resultCount = document.querySelectorAll('.result-card').length;
        window.parent.postMessage({
            type: 'miroir-search:results-count',
            count: resultCount,
            index: currentIndex
        }, '*');
    }

    // Embed mode: send result click event to parent frame (plan §13.21)
    function sendResultClickEvent(resultId, position, url) {
        if (!isEmbed) return;

        window.parent.postMessage({
            type: 'miroir-search:result-clicked',
            result_id: resultId,
            position: position,
            url: url,
            index: currentIndex,
            query: currentQuery
        }, '*');
    }

    // Start the app
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
