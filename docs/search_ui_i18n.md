# Search UI i18n (Internationalization)

## Overview

The Search UI supports internationalization (i18n) for operator-supplied translations. This is part of plan §13.21.

## How It Works

1. **Query Parameter**: Users can specify a language via the `lang` query parameter:
   - Example: `GET /ui/search/myindex?lang=fr`

2. **Browser Fallback**: If no `lang` parameter is provided, the UI uses `navigator.language`

3. **Fallback to English**: If the requested language is not configured, the UI falls back to English (the default built-in locale)

## Configuring Custom Locales

Operators can add custom translations via the `POST /_miroir/ui/search/{index}/config` endpoint:

```bash
curl -X POST "http://miroir:8080/_miroir/ui/search/myindex/config" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <admin_key>" \
  -d '{
    "locales": {
      "fr": {
        "ui.title": "Recherche",
        "ui.logo": "Recherche",
        "ui.search.placeholder": "Rechercher...",
        "ui.search.label": "Recherche",
        "ui.search.ariaLabel": "Requête de recherche",
        "ui.search.hint": "Tapez pour rechercher. Appuyez sur / pour focus. Utilisez les flèches pour naviguer.",
        "ui.search.button": "Rechercher",
        "ui.filters": "Filtres",
        "ui.sort.label": "Trier:",
        "ui.sort.relevance": "Pertinence",
        "ui.perPage.label": "Par page:",
        "ui.results.empty": "Aucun résultat trouvé",
        "ui.results.emptyHint": "Essayez d'ajuster votre recherche ou vos filtres",
        "ui.results.count": "{count} résultats ({time}ms)"
      }
    }
  }'
```

## Built-in Translations

The Search UI includes English translations by default in the bundled JavaScript. The `t()` function handles variable interpolation:

```javascript
// With interpolation
t('ui.results.count', { count: 42, time: 15 })
// Returns: "42 results (15ms)"

// With French locale
// Returns: "42 résultats (15ms)"
```

## Translation Keys

The following translation keys are supported (with default English values):

### UI Labels
- `ui.title` - Page title
- `ui.logo` - Logo text
- `ui.search.placeholder` - Search input placeholder
- `ui.search.label` - Search label
- `ui.search.ariaLabel` - Search ARIA label
- `ui.search.hint` - Search keyboard hint
- `ui.search.button` - Search button ARIA label
- `ui.darkMode.ariaLabel` - Dark mode toggle ARIA label
- `ui.filters` - Filters button text
- `ui.sort.label` - Sort selector label
- `ui.sort.relevance` - Default sort option text
- `ui.perPage.label` - Per-page selector label

### Results
- `ui.results.empty` - Empty state title
- `ui.results.emptyHint` - Empty state hint
- `ui.results.didYouMean` - "Did you mean" suggestion (use `{query}` variable)
- `ui.results.count` - Result count (use `{count}` and `{time}` variables)

### Errors
- `ui.error.noIndex` - No index specified error
- `ui.error.searchFailed` - Search failed error (use `{status}` variable)
- `ui.error.initFailed` - Init failed error (use `{message}` variable)
- `ui.error.sessionFailed` - Session fetch failed

### Accessibility
- `ui.filters.ariaLabel` - Filters ARIA label
- `ui.pagination.ariaLabel` - Pagination ARIA label
- `ui.keyboardShortcuts.ariaLabel` - Keyboard shortcuts ARIA label
- `ui.keyboardShortcuts.text` - Keyboard shortcuts help text
