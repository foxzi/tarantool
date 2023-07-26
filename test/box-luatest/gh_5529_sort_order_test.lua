local server = require('luatest.server')
local t = require('luatest')

local g1 = t.group('sort_order_test', {
    {engine = 'memtx', is_unique = true},
    {engine = 'memtx', is_unique = false},
    {engine = 'vinyl', is_unique = true},
    {engine = 'vinyl', is_unique = false}
})
local g2 = t.group('sort_order_test:func_index_singlepart', {
    {engine = 'memtx', is_nullable = true, is_unique = true},
    {engine = 'memtx', is_nullable = true, is_unique = false},
    {engine = 'memtx', is_nullable = false, is_unique = true},
    {engine = 'memtx', is_nullable = false, is_unique = false}
})

local groups = {g1, g2}

for _, g in pairs(groups) do
    g.before_all(function(cg)
        cg.server = server:new({alias = 'default'})
        cg.server:start()
    end)

    g.after_all(function(cg)
        cg.server:drop()
    end)

    g.before_each(function(cg)
        t.assert(cg.params.engine ~= nil)
        cg.server:exec(function(engine)
            local s

            s = box.schema.space.create('s_uuuu', {engine = engine})
            s:create_index('pk', {parts = {
                {1, 'unsigned'},
                {2, 'unsigned'},
                {3, 'unsigned'},
                {4, 'unsigned'}
            }})
            for i = 1, 2 do
                for j = 1, 2 do
                    for k = 1, 2 do
                        for l = 1, 2 do
                            s:insert({i, j, k, l})
                        end
                    end
                end
            end

            s = box.schema.space.create('s_u', {engine = engine})
            s:create_index('pk', {parts = {{1, 'unsigned'}}})
            for i = 1, 16 do
                s:insert({i})
            end

            s = box.schema.space.create('s_uuu_id', {engine = engine})
            s:create_index('pk', {parts = {{4, 'unsigned'}}})
            local id = 1
            for i = 1, 2 do
                for j = 1, 2 do
                    for k = 1, 2 do
                        s:insert({i, j, k, id})
                        id = id + 1
                    end
                end
            end
        end, {cg.params.engine})
    end)

    g.after_each(function(cg)
        cg.server:exec(function()
            box.space.s_uuuu:drop()
            box.space.s_u:drop()
            box.space.s_uuu_id:drop()
            if box.func.f ~= nil then
                box.func.f:drop()
            end
        end)
    end)
end

g2.test_func_index = function(cg)
    cg.server:exec(function(engine, is_nullable, is_unique)
        t.assert(engine ~= nil)
        t.assert(is_nullable ~= nil)
        t.assert(is_unique ~= nil)
        local s = box.space.s_uuuu
        local lua_code
        if is_nullable then
            lua_code = 'function(t) return {t[1], t[2],'
                       .. ' t[3] == 2 and t[3] or nil, t[4]} end'
        else
            lua_code = 'function(t) return {t[1], t[2], t[3], t[4]} end'
        end
        box.schema.func.create('f', {
            body = lua_code,
            is_deterministic = true,
            is_sandboxed = true
        })
        local idx = s:create_index('f', {parts = {
            {1, 'unsigned', sort_order = 'desc'},
            {2, 'unsigned', sort_order = 'asc'},
            {3, 'unsigned', sort_order = 'desc', is_nullable = is_nullable},
            {4, 'unsigned', sort_order = 'asc'}
        }, func = 'f', unique = is_unique})
        local expect = {
            {2, 1, 2, 1},
            {2, 1, 2, 2},
            {2, 1, 1, 1},
            {2, 1, 1, 2},
            {2, 2, 2, 1},
            {2, 2, 2, 2},
            {2, 2, 1, 1},
            {2, 2, 1, 2},
            {1, 1, 2, 1},
            {1, 1, 2, 2},
            {1, 1, 1, 1},
            {1, 1, 1, 2},
            {1, 2, 2, 1},
            {1, 2, 2, 2},
            {1, 2, 1, 1},
            {1, 2, 1, 2}
        }
        t.assert_equals(idx:select(), expect)
    end, {cg.params.engine, cg.params.is_nullable, cg.params.is_unique})
end

g2.test_func_index_singlepart = function(cg)
    cg.server:exec(function(engine, is_nullable, is_unique)
        t.assert(engine ~= nil)
        t.assert(is_nullable ~= nil)
        t.assert(is_unique ~= nil)
        local s = box.space.s_u
        local lua_code = 'function(tuple) return {tuple[1]} end'
        box.schema.func.create('f', {
            body = lua_code,
            is_deterministic = true,
            is_sandboxed = true
        })
        local idx = s:create_index('f', {parts = {
            {1, 'unsigned', sort_order = 'desc', is_nullable = is_nullable},
        }, func = 'f', unique = is_unique})
        local expect = {{16}, {15}, {14}, {13}, {12}, {11}, {10}, {9},
                        {8},  {7},  {6},  {5},  {4},  {3},  {2},  {1}}
        t.assert_equals(idx:select(), expect)
        expect = {{8},  {7},  {6},  {5},  {4},  {3},  {2},  {1}}
        t.assert_equals(idx:select(9, {iterator = 'GT'}), expect)
        expect = {{9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}}
        t.assert_equals(idx:select(8, {iterator = 'LT'}), expect)
        expect = {{9}, {8},  {7},  {6},  {5},  {4},  {3},  {2},  {1}}
        t.assert_equals(idx:select(9, {iterator = 'GE'}), expect)
        expect = {{8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}}
        t.assert_equals(idx:select(8, {iterator = 'LE'}), expect)
    end, {cg.params.engine, cg.params.is_nullable, cg.params.is_unique})
end

g1.test_sequential = function(cg)
    cg.server:exec(function(engine, is_unique)
        t.assert(engine ~= nil)
        t.assert(is_unique ~= nil)
        local s = box.space.s_uuu_id
        local idx = s:create_index('sk', {parts = {
            {1, 'unsigned', sort_order = 'desc'},
            {2, 'unsigned', sort_order = 'asc'},
            {3, 'unsigned', sort_order = 'desc'},
        }, unique = is_unique})
        local expect = {
            {2, 1, 2, 6},
            {2, 1, 1, 5},
            {2, 2, 2, 8},
            {2, 2, 1, 7},
            {1, 1, 2, 2},
            {1, 1, 1, 1},
            {1, 2, 2, 4},
            {1, 2, 1, 3}
        }
        t.assert_equals(idx:select(), expect)
    end, {cg.params.engine, cg.params.is_unique})
end
