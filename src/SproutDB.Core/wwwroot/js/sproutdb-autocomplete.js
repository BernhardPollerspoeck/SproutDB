// SproutDB Query Autocomplete Engine
// Pure logic — no DOM dependencies. Used by AdminApp.razor and testable via Jint.

var SproutAutocomplete = (function () {
    var COMMANDS = ['get','upsert','delete','describe','create','purge','add','alter','rename','backup','restore'];
    var CLAUSES = ['where','select','-select','order','limit','page','count','distinct','group','follow','on','as'];
    var OPERATORS = ['=','!=','>','>=','<','<=','between','in','is','contains','starts','ends'];
    var DIRECTIONS = ['asc','desc'];
    var BOOLEANS = ['true','false','null'];
    var TYPES = ['string','bool','sbyte','ubyte','sshort','ushort','sint','uint','slong','ulong','float','double','date','time','datetime'];
    var CREATE_SUB = ['database','table','index','apikey'];
    var PURGE_SUB = ['database','table','column','index','apikey','ttl'];

    function getContext(text, pos) {
        var before = text.substring(0, pos);

        // Track brace/bracket depth
        var braceDepth = 0, bracketDepth = 0;
        for (var ci = 0; ci < before.length; ci++) {
            var ch = before[ci];
            if (ch === '{') braceDepth++;
            else if (ch === '}') braceDepth--;
            else if (ch === '[') bracketDepth++;
            else if (ch === ']') bracketDepth--;
        }

        if (braceDepth > 0 || bracketDepth > 0) {
            // Inside upsert body: suggest columns after { or ,
            var fullToks = before.trim().split(/\s+/).filter(function(t) { return t.length > 0; });
            if (braceDepth > 0 && fullToks.length >= 2 && fullToks[0].toLowerCase() === 'upsert') {
                var afterLastBrace = before.substring(before.lastIndexOf('{') + 1);
                var afterComma = afterLastBrace.lastIndexOf(',') >= 0 ? afterLastBrace.substring(afterLastBrace.lastIndexOf(',') + 1) : afterLastBrace;
                var trimmed = afterComma.trim();
                if (trimmed.indexOf(':') < 0) {
                    var colPrefix = trimmed.split(/\s+/).filter(function(t) { return t.length > 0; });
                    var cp = colPrefix.length > 0 && !before.endsWith(' ') ? colPrefix[colPrefix.length - 1] : '';
                    return { type: 'column', prefix: cp, table: fullToks[1] };
                }
            }
            return { type: 'none', prefix: '', table: null };
        }

        // Upsert: after closed body suggest 'on', after 'on' suggest columns
        var hasBraces = before.indexOf('{') >= 0 || before.indexOf('[') >= 0;
        if (hasBraces) {
            var fullTokens = before.trim().split(/\s+/).filter(function(t) { return t.length > 0; });
            if (fullTokens.length > 0 && fullTokens[0].toLowerCase() === 'upsert') {
                var lastClose = Math.max(before.lastIndexOf('}'), before.lastIndexOf(']'));
                var tail = before.substring(lastClose + 1).trim();
                var tailTokens = tail.split(/\s+/).filter(function(t) { return t.length > 0; });
                var tailPrefix = (!before.endsWith(' ') && tailTokens.length > 0) ? tailTokens.pop() : '';
                var uTable = fullTokens.length >= 2 ? fullTokens[1] : null;

                if (tailTokens.length === 0)
                    return { type: 'upsert-after', prefix: tailPrefix, table: uTable };
                if (tailTokens.length === 1 && tailTokens[0].toLowerCase() === 'on')
                    return { type: 'column', prefix: tailPrefix, table: uTable };
                return { type: 'none', prefix: '', table: null };
            }
        }

        var lineStart = before.lastIndexOf('\n') + 1;
        var line = before.substring(lineStart);
        var tokens = line.trim().split(/\s+/).filter(function(t) { return t.length > 0; });
        var prefix = '';

        // For multiline queries, also tokenize the full text to find cmd and table
        var allTokens = before.trim().split(/\s+/).filter(function(t) { return t.length > 0; });

        if (tokens.length === 0 && allTokens.length === 0) return { type: 'command', prefix: '', table: null };

        // Determine if we're mid-word or starting a new token
        var endsWithSpace = before.length > 0 && before[before.length - 1] === ' ';
        if (!endsWithSpace && tokens.length > 0) {
            prefix = tokens[tokens.length - 1];
            tokens = tokens.slice(0, -1);
        }
        // Also adjust allTokens for prefix
        var allPrefix = '';
        if (!endsWithSpace && allTokens.length > 0) {
            allPrefix = allTokens[allTokens.length - 1];
            allTokens = allTokens.slice(0, -1);
        }

        // Use allTokens for cmd and table detection (works across lines)
        var cmd = allTokens.length > 0 ? allTokens[0].toLowerCase() : (prefix || allPrefix).toLowerCase();

        if (allTokens.length === 0) return { type: 'command', prefix: prefix || allPrefix, table: null };

        // Detect table from all tokens (full query, not just current line)
        var table = null;
        for (var i = 0; i < allTokens.length; i++) {
            var tl = allTokens[i].toLowerCase();
            if ((tl === 'get' || tl === 'upsert' || tl === 'delete' || tl === 'from' || tl === 'describe' || tl === 'purge') && i + 1 < allTokens.length) {
                var next = allTokens[i + 1].toLowerCase();
                if (next !== 'from' && next !== 'database' && next !== 'table' && next !== 'column' && next !== 'index' && next !== 'apikey') {
                    table = allTokens[i + 1];
                }
            }
        }

        // Check for table.col pattern in prefix
        var p = prefix || allPrefix;
        if (p.indexOf('.') > 0) {
            var parts = p.split('.');
            return { type: 'column', prefix: parts[1] || '', table: parts[0] };
        }

        var lastToken = allTokens.length > 0 ? allTokens[allTokens.length - 1].toLowerCase() : '';

        // After create -> create-sub
        if (cmd === 'create' && allTokens.length === 1) return { type: 'create-sub', prefix: p, table: null };

        // After purge -> purge-sub
        if (cmd === 'purge' && allTokens.length === 1) return { type: 'purge-sub', prefix: p, table: null };

        // After command keyword -> table
        if (allTokens.length === 1 && (cmd === 'get' || cmd === 'upsert' || cmd === 'delete' || cmd === 'describe'))
            return { type: 'table', prefix: p, table: null };

        // After from/table keyword -> table
        if (lastToken === 'from' || lastToken === 'table')
            return { type: 'table', prefix: p, table: null };

        // After purge subcommand -> table
        if (cmd === 'purge' && allTokens.length === 2)
            return { type: 'table', prefix: p, table: null };

        // After 'order by' -> column
        if (allTokens.length >= 2 && allTokens[allTokens.length - 2].toLowerCase() === 'order' && lastToken === 'by')
            return { type: 'column', prefix: p, table: table };

        // After where/select/-select/by -> column
        if (lastToken === 'where' || lastToken === 'select' || lastToken === '-select' || lastToken === 'by')
            return { type: 'column', prefix: p, table: table };

        // After comma in select/where list -> column (e.g. "select name," or "select name, ")
        if (lastToken.endsWith(',') || (allTokens.length >= 2 && allTokens[allTokens.length - 1] === ',')) {
            // Walk back to find if we're in a select/where context
            for (var si = allTokens.length - 1; si >= 0; si--) {
                var st = allTokens[si].toLowerCase();
                if (st === 'select' || st === '-select' || st === 'where' || st === 'by')
                    return { type: 'column', prefix: p, table: table };
                if (st === 'get' || st === 'follow' || st === 'order' || st === 'limit' || st === 'page')
                    break;
            }
        }

        // After 'order by <col>' -> direction
        if (allTokens.length >= 3) {
            var ob1 = allTokens[allTokens.length - 3] ? allTokens[allTokens.length - 3].toLowerCase() : '';
            var ob2 = allTokens[allTokens.length - 2] ? allTokens[allTokens.length - 2].toLowerCase() : '';
            if (ob1 === 'order' && ob2 === 'by')
                return { type: 'direction', prefix: p, table: table };
        }

        // After 'where <col>' -> operator
        if (allTokens.length >= 2 && allTokens[allTokens.length - 2].toLowerCase() === 'where')
            return { type: 'operator', prefix: p, table: table };

        // After 'and/or <col>' -> operator
        if (allTokens.length >= 2 && (allTokens[allTokens.length - 2].toLowerCase() === 'and' || allTokens[allTokens.length - 2].toLowerCase() === 'or'))
            return { type: 'operator', prefix: p, table: table };

        // After operator -> boolean
        var ops = ['=','!=','>','>=','<','<=','contains','starts','ends','between','in','is'];
        if (ops.indexOf(lastToken) >= 0)
            return { type: 'boolean', prefix: p, table: table };

        // After 'add column tbl.col' or inside type context
        if (cmd === 'add' && allTokens.length >= 3)
            return { type: 'type', prefix: p, table: table };

        // After 'upsert <table>' -> suggest body openers
        if (cmd === 'upsert' && allTokens.length === 2)
            return { type: 'upsert-body', prefix: p, table: table };

        // After table name -> clause
        if (table) {
            return { type: 'clause', prefix: p, table: table };
        }

        return { type: 'command', prefix: p, table: null };
    }

    function getSuggestions(ctx, schema) {
        var list, cat;
        switch (ctx.type) {
            case 'command': list = COMMANDS; cat = 'cmd'; break;
            case 'table': list = (schema && schema.tables) || []; cat = 'tbl'; break;
            case 'column':
                var t = ctx.table;
                if (t && schema && schema.columns) {
                    var cols = null;
                    var keys = Object.keys(schema.columns);
                    for (var k = 0; k < keys.length; k++) {
                        if (keys[k].toLowerCase() === t.toLowerCase()) {
                            cols = schema.columns[keys[k]];
                            break;
                        }
                    }
                    list = cols || [];
                } else {
                    list = [];
                }
                cat = 'col'; break;
            case 'clause': list = CLAUSES; cat = 'kw'; break;
            case 'operator': list = OPERATORS; cat = 'op'; break;
            case 'direction': list = DIRECTIONS; cat = 'dir'; break;
            case 'boolean': list = BOOLEANS; cat = 'val'; break;
            case 'type': list = TYPES; cat = 'type'; break;
            case 'create-sub': list = CREATE_SUB; cat = 'kw'; break;
            case 'purge-sub': list = PURGE_SUB; cat = 'kw'; break;
            case 'upsert-body': list = ['{ }', '[ ]']; cat = 'kw'; break;
            case 'upsert-after': list = ['on']; cat = 'kw'; break;
            default: list = []; cat = '';
        }
        var p = (ctx.prefix || '').toLowerCase();
        var filtered = list.filter(function(s) { return s.toLowerCase().indexOf(p) === 0; });
        return { items: filtered.slice(0, 12), cat: cat };
    }

    return {
        getContext: getContext,
        getSuggestions: getSuggestions,
        COMMANDS: COMMANDS,
        CLAUSES: CLAUSES,
        OPERATORS: OPERATORS,
        DIRECTIONS: DIRECTIONS,
        BOOLEANS: BOOLEANS,
        TYPES: TYPES,
        CREATE_SUB: CREATE_SUB,
        PURGE_SUB: PURGE_SUB
    };
})();
