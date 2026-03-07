// SproutDB Query Autocomplete Engine
// Pure logic — no DOM dependencies. Used by AdminApp.razor and testable via Jint.

var SproutAutocomplete = (function () {
    var COMMANDS = ['get','upsert','delete','describe','create','purge','add','alter','rename','backup','restore'];
    var CLAUSES = ['where','select','order','limit','page','count','distinct','group','follow','on','as'];
    var OPERATORS = ['=','!=','>','>=','<','<=','between','in','is','contains','starts','ends'];
    var DIRECTIONS = ['asc','desc'];
    var BOOLEANS = ['true','false','null'];
    var TYPES = ['string','bool','sbyte','ubyte','sshort','ushort','sint','uint','slong','ulong','float','double','date','time','datetime'];
    var CREATE_SUB = ['database','table','index','apikey'];
    var PURGE_SUB = ['database','table','column','index','apikey'];

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

        if (tokens.length === 0) return { type: 'command', prefix: '', table: null };

        // Determine if we're mid-word or starting a new token
        var endsWithSpace = line.length > 0 && line[line.length - 1] === ' ';
        if (!endsWithSpace && tokens.length > 0) {
            prefix = tokens[tokens.length - 1];
            tokens = tokens.slice(0, -1);
        }

        var cmd = tokens.length > 0 ? tokens[0].toLowerCase() : prefix.toLowerCase();

        if (tokens.length === 0) return { type: 'command', prefix: prefix, table: null };

        // Detect table from tokens
        var table = null;
        for (var i = 0; i < tokens.length; i++) {
            var tl = tokens[i].toLowerCase();
            if ((tl === 'get' || tl === 'upsert' || tl === 'delete' || tl === 'from' || tl === 'describe' || tl === 'purge') && i + 1 < tokens.length) {
                var next = tokens[i + 1].toLowerCase();
                if (next !== 'from' && next !== 'database' && next !== 'table' && next !== 'column' && next !== 'index' && next !== 'apikey') {
                    table = tokens[i + 1];
                }
            }
        }

        // Check for table.col pattern in prefix
        if (prefix.indexOf('.') > 0) {
            var parts = prefix.split('.');
            return { type: 'column', prefix: parts[1] || '', table: parts[0] };
        }

        var lastToken = tokens[tokens.length - 1].toLowerCase();

        // After create -> create-sub
        if (cmd === 'create' && tokens.length === 1) return { type: 'create-sub', prefix: prefix, table: null };

        // After purge -> purge-sub
        if (cmd === 'purge' && tokens.length === 1) return { type: 'purge-sub', prefix: prefix, table: null };

        // After command keyword -> table
        if (tokens.length === 1 && (cmd === 'get' || cmd === 'upsert' || cmd === 'delete' || cmd === 'describe'))
            return { type: 'table', prefix: prefix, table: null };

        // After from/table keyword -> table
        if (lastToken === 'from' || lastToken === 'table')
            return { type: 'table', prefix: prefix, table: null };

        // After purge subcommand -> table
        if (cmd === 'purge' && tokens.length === 2)
            return { type: 'table', prefix: prefix, table: null };

        // After 'order by' -> column
        if (tokens.length >= 2 && tokens[tokens.length - 2].toLowerCase() === 'order' && lastToken === 'by')
            return { type: 'column', prefix: prefix, table: table };

        // After where/select/by -> column
        if (lastToken === 'where' || lastToken === 'select' || lastToken === 'by')
            return { type: 'column', prefix: prefix, table: table };

        // After 'order by <col>' -> direction
        if (tokens.length >= 3) {
            var ob1 = tokens[tokens.length - 3] ? tokens[tokens.length - 3].toLowerCase() : '';
            var ob2 = tokens[tokens.length - 2] ? tokens[tokens.length - 2].toLowerCase() : '';
            if (ob1 === 'order' && ob2 === 'by')
                return { type: 'direction', prefix: prefix, table: table };
        }

        // After 'where <col>' -> operator
        if (tokens.length >= 2 && tokens[tokens.length - 2].toLowerCase() === 'where')
            return { type: 'operator', prefix: prefix, table: table };

        // After 'and/or <col>' -> operator
        if (tokens.length >= 2 && (tokens[tokens.length - 2].toLowerCase() === 'and' || tokens[tokens.length - 2].toLowerCase() === 'or'))
            return { type: 'operator', prefix: prefix, table: table };

        // After operator -> boolean
        var ops = ['=','!=','>','>=','<','<=','contains','starts','ends','between','in','is'];
        if (ops.indexOf(lastToken) >= 0)
            return { type: 'boolean', prefix: prefix, table: table };

        // After 'add column tbl.col' or inside type context
        if (cmd === 'add' && tokens.length >= 3)
            return { type: 'type', prefix: prefix, table: table };

        // After 'upsert <table>' -> suggest body openers
        if (cmd === 'upsert' && tokens.length === 2)
            return { type: 'upsert-body', prefix: prefix, table: table };

        // After table name -> clause
        if (table) {
            return { type: 'clause', prefix: prefix, table: table };
        }

        return { type: 'command', prefix: prefix, table: null };
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
