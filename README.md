Name
====

lua-resty-gearman - Lua gearman client driver for the ngx_lua based on the cosocket API 

Status
======

This library is considered experimental and still under active development.

Description
===========

This Lua library is a Gearman client driver for the ngx_lua nginx module:

http://wiki.nginx.org/HttpLuaModule

This Lua library takes advantage of ngx_lua's cosocket API, which ensures
100% nonblocking behavior.

Note that at least [ngx_lua 0.5.14](https://github.com/chaoslawful/lua-nginx-module/tags) or [ngx_openresty 1.2.1.14](http://openresty.org/#Download) is required.

Synopsis
========

    lua_package_path "/path/to/lua-resty-gearman/lib/?.lua;;";

    server {
        location /test {
            content_by_lua '
                local gearman = require "resty.gearman"
                local gm = gearman:new()

                gm:set_timeout(1000) -- 1 sec

                local ok, err = gm:connect("127.0.0.1", 4730)
                if not ok then
                    ngx.say("failed to connect: ", err)
                    return
                end

                ok, err = g:submit_job("wc", "11111\n22222\n33333")  
                -- submit_job,submit_job_bg,submit_job_high,submit_job_high_bg,submit_job_low,submit_job_low_bg are supported
                -- submit_job(function_name, workload, [unique])
                
                if not ok then
                    ngx.say("failed to submit job: ", err)
                    return
                else
                    ngx.say(ok)                
                end

                -- put it into the connection pool of size 100,
                -- with 0 idle timeout
                local ok, err = gm:set_keepalive(0, 100)
                if not ok then
                    ngx.say("failed to set keepalive: ", err)
                    return
                end

                -- or just close the connection right away:
                -- local ok, err = gm:close()
                -- if not ok then
                --     ngx.say("failed to close: ", err)
                --     return
                -- end
            ';
        }
    }


Methods
===========


Limitations
===========

* Now only client tasks(submit_job,submit_job_bg,submit_job_high,submit_job_high_bg,submit_job_low,submit_job_low_bg)
are supported.
* This library cannot be used in code contexts like set_by_lua*, log_by_lua*, and
header_filter_by_lua* where the ngx_lua cosocket API is not available.
* The `resty.gearman` object instance cannot be stored in a Lua variable at the Lua module level,
because it will then be shared by all the concurrent requests handled by the same nginx
 worker process (see
http://wiki.nginx.org/HttpLuaModule#Data_Sharing_within_an_Nginx_Worker ) and
result in bad race conditions when concurrent requests are trying to use the same `resty.gearman` instance.
You should always initiate `resty.gearman` objects in function local
variables or in the `ngx.ctx` table. These places all have their own data copies for
each request.

Installation
============

If you are using the ngx_openresty bundle (http://openresty.org ), then
you don't need to do anything because it already includes and enables
lua-resty-gearman by default. And you can just use it in your Lua code,
as in

    local gearman = require "resty.gearman"
    ...

If you're using your own nginx + ngx_lua build, then you need to configure
the lua_package_path directive to add the path of your lua-resty-gearman source
tree to ngx_lua's LUA_PATH search path, as in

    # nginx.conf
    http {
        lua_package_path "/path/to/lua-resty-gearman/lib/?.lua;;";
        ...
    }

TODO
====

Community
=========

English Mailing List
--------------------

The [openresty-en](https://groups.google.com/group/openresty-en) mailing list is for English speakers.

Chinese Mailing List
--------------------

The [openresty](https://groups.google.com/group/openresty) mailing list is for Chinese speakers.

Bugs and Patches
================

Please report bugs or submit patches by

1. creating a ticket on the [GitHub Issue Tracker](http://github.com/zhhchen/lua-resty-gearman/issues)

Author
======

Zhihua Chen (陈智华) <zhhchen@gmail.com>

Copyright and License
=====================

This module is licensed under the BSD license.

Copyright (C) 2012, by Zhihua Chen (陈智华) <zhhchen@gmail.com>.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

See Also
========
* the ngx_lua module: http://wiki.nginx.org/HttpLuaModule
* the gearman protocol specification: http://gearman.org/doku.php?id=protocol

