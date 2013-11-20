-- Copyright (C) 2012 Zhihua Chen (zhhchen)

local now = ngx.now
local sub = string.sub
local tcp = ngx.socket.tcp
local sleep = ngx.sleep
local insert = table.insert
local concat = table.concat
local foreach = table.foreach
local floor = math.floor
local len = string.len
local null = ngx.null
local print = ngx.print
local byte = string.byte
local setmetatable = setmetatable
local tonumber = tonumber
local error = error
local binstr = {'\0','\1','\2','\3','\4','\5','\6','\7','\8','\9','\10','\11','\12','\13','\14','\15','\16','\17','\18','\19','\20','\21','\22','\23','\24','\25','\26','\27','\28','\29','\30','\31','\32','\33','\34','\35','\36','\37','\38','\39','\40','\41','\42','\43','\44','\45','\46','\47','\48','\49','\50','\51','\52','\53','\54','\55','\56','\57','\58','\59','\60','\61','\62','\63','\64','\65','\66','\67','\68','\69','\70','\71','\72','\73','\74','\75','\76','\77','\78','\79','\80','\81','\82','\83','\84','\85','\86','\87','\88','\89','\90','\91','\92','\93','\94','\95','\96','\97','\98','\99','\100','\101','\102','\103','\104','\105','\106','\107','\108','\109','\110','\111','\112','\113','\114','\115','\116','\117','\118','\119','\120','\121','\122','\123','\124','\125','\126','\127','\128','\129','\130','\131','\132','\133','\134','\135','\136','\137','\138','\139','\140','\141','\142','\143','\144','\145','\146','\147','\148','\149','\150','\151','\152','\153','\154','\155','\156','\157','\158','\159','\160','\161','\162','\163','\164','\165','\166','\167','\168','\169','\170','\171','\172','\173','\174','\175','\176','\177','\178','\179','\180','\181','\182','\183','\184','\185','\186','\187','\188','\189','\190','\191','\192','\193','\194','\195','\196','\197','\198','\199','\200','\201','\202','\203','\204','\205','\206','\207','\208','\209','\210','\211','\212','\213','\214','\215','\216','\217','\218','\219','\220','\221','\222','\223','\224','\225','\226','\227','\228','\229','\230','\231','\232','\233','\234','\235','\236','\237','\238','\239','\240','\241','\242','\243','\244','\245','\246','\247','\248','\249','\250','\251','\252','\253','\254','\255'}

module(...)

_VERSION = '0.02'

local commands = {
    submit_job='\0\0\0\7',submit_job_bg='\0\0\0\18',
    submit_job_high='\0\0\0\21',submit_job_high_bg='\0\0\0\32',
    submit_job_low='\0\0\0\33',submit_job_low_bg='\0\0\0\34'
}


local mt = { __index = _M }


function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end


function set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end


function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


function StringBytesToNumber(str)
    local num = 0
    local len = #str
    for i = 1,len do
        num = num + byte(str,i) * 256^(len-i)
    end

    return num
end


local function _read_reply(sock,cmd)
    local line, err = sock:receive(12)
    if not line then
        return nil, err
    end

    local prefix = sub(line, 1, 4)
    local ptype = sub(line, 8, 8)
    local hidlenbin = sub(line,9,12)
    local hidlen = StringBytesToNumber(hidlenbin)
    if prefix == '\0RES' and ptype == '\8' then -- 8   JOB_CREATED
        local handleid, err = sock:receive(hidlen)
        if not handleid then
            return nil, err
        end

        if cmd == '\0\0\0\7' or cmd == '\0\0\0\21' or cmd == '\0\0\0\33' then -- Not Backgroud Job
            local resdata = {}
            local time0 = now()
            repeat
                local req = {'\0REQ', '\0\0\0\15', hidlenbin, handleid} -- 15  GET_STATUS
                local bytes, err = sock:send(concat(req, ""))
                if not bytes then
                    return nil, err
                end

                local line, err = sock:receive(12)
                if not line then
                    return nil, err
                end

                local prefix = sub(line, 1, 4)
                local ptype = sub(line, 8, 8)
                local datalen = StringBytesToNumber(sub(line,9,12))
                local data, err = sock:receive(datalen)
                if not data then
                    return nil, err
                end

                if prefix == '\0RES' and ptype == '\13' then -- 13  WORK_COMPLETE
                    insert(resdata,sub(data, hidlen+2, datalen))
                elseif prefix == '\0RES' and ptype == '\14' then -- 14  WORK_FAIL
                    return nil, "WORK FAIL"
                elseif prefix == '\0RES' and ptype == '\25' then -- 25  WORK_EXCEPTION
                    return nil, "WORK EXCEPTION"
                elseif prefix == '\0RES' and ptype == '\28' then -- 28  WORK_DATA
                    insert(resdata,sub(data, hidlen+2, datalen))
                elseif prefix == '\0RES' and ptype == '\29' then -- 29  WORK_WARNING
                    return nil, "WORK WARNING"
                elseif prefix == '\0RES' and ptype == '\20' then -- 20  STATUS_RES
                    if (now()-time0) > 5 then
                        return nil, "time out"
                    end
                    -- sleep(1)
                elseif prefix == '\0RES' and ptype == '\12' then -- 12  WORK_STATUS
                    if (now()-time0) > 5 then
                        return nil, "time out"
                    end
                    -- sleep(1)
                else
                    return nil, "response error"
                end
            until prefix == '\0RES' and ptype == '\13'

            local resstr = concat(resdata, "")

            return resstr
        else
            return handleid
        end
    else
        return nil, "submit job error"
    end
end


local function _gen_req(cmd, args)
    local data = concat({args[1],args[3],args[2]},'\0')
    local datalen = len(data)
    local datalenbin = {binstr[floor(datalen/16777216)%256+1],binstr[floor(datalen/65536)%256+1],binstr[floor(datalen/256)%256+1],binstr[datalen%256+1]}
    local datalenstr = concat(datalenbin, "")
    local req = {'\0REQ', cmd, datalenstr, data}

    return concat(req, "")
end


local function _do_cmd(self, cmd, ...)
    local args = {...}
    if #args < 2 or #args > 3 then
        return nil, "args error"
    elseif #args == 2 then
        args[3] = ""
    end

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(cmd, args)

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return _read_reply(sock, cmd)
end


foreach(commands, function(i, v, self)
    local cmd, val = i, v
    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, val, ...)
        end
end)


local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}


setmetatable(_M, class_mt)

