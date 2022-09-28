# Copyright 2022 Tomas Brabec
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

namespace eval hudb {
    source [file join [file dirname [file normalize [info script]]] huddle huddle.tcl]

    set db {HUDDLE {D {}}};
    set separator {/}

    # infuse new routines for Huddle processing
    # TODO as long as the routines are independent of other `hudb` code, they
    #      shall move to the `huddle` code
    namespace eval huddle {

        # Performs depth-first traverse of the huddle structure, calling the
        # given visitor `callback` for each huddle node being visited.
        #
        # Container nodes are passed to the visitor callback before traversing
        # their subnodes. This way, the visitor may decide to cease diving
        # deeper for that container node.
        #
        # Arguments:
        #   callback - name of routine to be called on node visit
        #   node_var - name of the variable holding the node representation
        #   path - list of key tokens to the `node_var`
        #   args - remaining arguments to be passed to the callback
        #
        # Callback prototype should look like::
        #
        #   proc my_callback {node_var path args} { ... }
        #
        # The callback shall return a flat dictionary with the following keys:
        # - stop: Value of 1 will make the the visit traverse stop altogether
        # - ascend: stop traversing to subnode
        # - repack: Indicates that the node has been changed during the visit
        #   and shall be repacked into the huddle structure.
        proc Visit_node_depth_first {callback node_var path args} {
            namespace upvar [namespace current] types types;
            upvar 1 $node_var node;

            set tag [lindex $node 0];

            if {![info exists types(type:$tag)] || ![info exists types(isContainer:$tag)]} {
                error "Unrecognized huddle node type: '$tag'";
            } elseif { $types(isContainer:$tag) } {
                # call the visitor/callback on the container node first
                # (before calling it for its sub-nodes)
                set resp [eval ${callback} node \$path ${args}];

                # see if to traverse deeper (and hence if to visit sub-nodes)
                if {[dict get $resp "ascend"] != 1 || [dict get $resp "stop"] != 1} {
                    set repack 0; # flag to indicate the container value changed
                    set value [lindex $node 1];
                    foreach item [$types(callback:$tag) items $value] {
                        lassign $item key subnode;
                        set subpath ${path};
                        lappend subpath $key;

                        # call the visitor/callback (for a sub-node)
                        set subresp [eval Visit_node_depth_first \${callback} subnode \$subpath ${args}];

                        # see if the sub-node changed (and, if so, re-pack it into the container
                        # node)
                        if {[dict get ${subresp} "repack"] == 1} {
                            $types(callback:$tag) delete_subnode_but_not_key value $key
                            $types(callback:$tag) set value $key $subnode
                            set repack 1;
                        }

                        # see if to stop processing immediately
                        if {[dict get ${subresp} "stop"] == 1} {
                            dict set resp "stop" 1;
                            break;
                        }
                    }

                    if {$repack == 1} {
                        lset node 1 $value;
                        dict set resp "repack" 1;
                    }
                }

                return $resp;
            } else {
                # delegate response to the visitor/callback
                return [eval ${callback} node \$path ${args}];
            }
        }
    }

    # infuse new Huddle types into the `huddle` namespace
    namespace eval huddle {
        namespace eval uri {
            variable settings;

            set settings {
                publicMethods {uri}
                tag uri
                isContainer no
            };

            proc uri {src} {
                return [::huddle::wrap [list uri $src]];
            }

            proc equal {p1 p2} {
                #TODO shall compare canonical forms (to cope with relative
                #     path aspects
                return [expr {$p1 eq $p2}];
            }

            proc jsondump {data {offset "  "} {newline "\n"} {begin ""}} {
                # JSON permits only oneline string
                set data [string map {
                        \n \\n
                        \t \\t
                        \r \\r
                        \b \\b
                        \f \\f
                        \\ \\\\
                        \" \\\"
                        / \\/
                    } $data
                ];
                return "\"$data\""
            }

            # A visitor/callback for hudb::Visit_node_depth_first that rebases
            # relative file URI paths.
            #
            # Arguments:
            #   node_var - name of a huddle node variable in the caller's stack frame
            #   path - list of tokens making up a DB key
            #   args[0] - target directory for URI path rebase
            #   args[1] - source directory for URI path rebase
            proc cb_rebase {node_var path args} {
                namespace upvar [namespace current] settings settings;
                upvar 1 $node_var node;
                lassign $node tag value;
                set sep ${::hudb::separator};
                if {$tag eq [dict get ${settings} "tag"] && [file pathtype $value] eq "relative"} {
                    lset node 1 [::hudb::filepath::rebase [lindex $args 0] $value [lindex $args 1]];
                    return {stop 0 ascend 0 repack 1};
                }
                return {stop 0 ascend 0 repack 0};
            }

        }; # end of huddle::uri namespace
    }

    # file path related API
    namespace eval filepath {

        # Returns the given `path` as relative to the `tgt_dir` folder.
        #
        # If the `path` argument is a relative path, it will be
        # interpretted as relative to the `src_dir` path (which
        # defaults to `.` and hence the current working directory).
        # The `tgt_dir` as a relative path would be always interpretted
        # as relative to the current working directory.
        #
        # None of the paths is tested for existence or the type of the
        # file object. `tgt_dir` and `src_dir` are always assumed to be
        # folders
        #
        # Note that if `path` is an absolute path that shall remain
        # absolute, there is no need to call `rebase` at all (as it
        # would change it into a relative path).
        #
        proc rebase {tgt_dir path {src_dir .}} {
            set tgt_dir [file normalize $tgt_dir];
            if {[file pathtype $path] eq "absolute"} {
                set path [file normalize $path];
            } else {
                set path [file normalize [file join $src_dir $path]];
            }
            set separator [file separator];

            set base_parts [file split $tgt_dir]
            set path_parts [file split $path]
            set i 0;
            foreach bpart $base_parts ppart $path_parts {
                if {$bpart ne $ppart} break
                incr i 1;
            }
            set back_steps [expr [llength $base_parts] - $i];
            set l [lrange $path_parts $i end];
            if {$back_steps > 0} {
                set parts [file split [string repeat "..[file separator]" $back_steps]];
                eval lappend parts $l;
                set x [eval file join $parts];
            } elseif {$i < [llength $path_parts]} {
                set x [eval file join $l];
            } else {
                set x ".";
            }
            return $x
        }

    }


    # register new huddle types
    huddle addType "[namespace current]::huddle::uri";


    proc _reset {db_name} {
        upvar ${db_name} db;
        set db {HUDDLE {D {}}};
    }


    proc _is_wellformed {db_name} {
        upvar ${db_name} db;
        if {![huddle isHuddle ${db}]} {
            return 0;
        }
        lassign [huddle unwrap ${db}] head src;
        if {[info exists huddle::types(tagOfType:dict)] &&
            ${head} ne $huddle::types(tagOfType:dict)} {
            return 0;
        }
        return 1;
    }


    proc _is_empty {db_name} {
        upvar ${db_name} db;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }
        lassign [huddle unwrap ${db}] head src;
        if {[llength ${src}] > 0} { return 0; } else { return 1; }
    }


    proc _rebase_uris {db_name tgt_dir src_dir} {
        upvar ${db_name} db;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }
        set node [huddle::unwrap ${db}];
        set resp [huddle::Visit_node_depth_first [namespace current]::huddle::uri::cb_rebase node {} $tgt_dir $src_dir];
        if {[dict get $resp "repack"] == 1} {
            set db [huddle wrap $node];
        }
    }


    proc reset {args} {
        set db_name {};
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -db {
                    if {$i >= [llength $args] -1} { error "Wrong number of arguments"; }
                    incr i 1;
                    set db_name [lindex $args $i];
                    if {[string match "-*" ${name}]} {
                        error "Missing type for the -db option!";
                        set db_name {};
                    }
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args] - 1} {
            error "Too many arguments";
        }

        # get DB object
        if {${db_name} eq {}} {
            namespace upvar [namespace current] db db;
        } else {
            upvar ${db_name} db;
        }

        _reset "db";
    }


    proc is_wellformed {args} {
        set db_name {};
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -db {
                    if {$i >= [llength $args] -1} { error "Wrong number of arguments"; }
                    incr i 1;
                    set db_name [lindex $args $i];
                    if {[string match "-*" ${name}]} {
                        error "Missing type for the -db option!";
                        set db_name {};
                    }
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args] - 1} {
            error "Too many arguments";
        }

        # get DB object
        if {${db_name} eq {}} {
            namespace upvar [namespace current] db db;
        } else {
            upvar ${db_name} db;
        }

        return [_is_wellformed "db"];
    }


    proc is_empty {args} {
#@        puts "[namespace current]"
#@        puts "[namespace which -variable db]"

        set db_name {};
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -db {
                    if {$i >= [llength $args] -1} { error "Wrong number of arguments"; }
                    incr i 1;
                    set db_name [lindex $args $i];
                    if {[string match "-*" ${name}]} {
                        error "Missing type for the -db option!";
                        set db_name {};
                    }
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args] - 1} {
            error "Too many arguments";
        }

        # get DB object
        if {${db_name} eq {}} {
            namespace upvar [namespace current] db db;
        } else {
            upvar ${db_name} db;
        }

        return [_is_empty "db"];
    }


    proc set_key {args} {
        if {[llength ${args}] == 0} { return; }
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        if {[llength $args] < 2} { error "Wrong number of arguments"; }
        set tag s;
        array set _opts { type {} json 0 }
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -type {
                    if {[llength $args] < 4} { error "Wrong number of arguments"; }
                    incr i 1;
                    set type [lindex $args $i];
                    if {${type} eq "--"} {
                        error "Missing type for the -type option!";
                    } elseif {![info exists huddle::types(tagOfType:${type})]} {
                        error "Unknown type '${type}'!";
                    } else {
                        set tag $huddle::types(tagOfType:${type});
                    }
                    if {![info exists huddle::types(isContainer:${tag})]} {
                        error "Cannot support '${type}'!";
                    } elseif {$huddle::types(isContainer:${tag}) ne "no"} {
                        #TODO Not sure if this will end up `-json` or `-type json`.
                        error "Container types not supported. Use -json option."
                    }
                    set _opts(type) ${type};
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option '[lindex $args $i]'!";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; # while

        if {$i+2 != [llength $args]} {
            error "Too many arguments! Args: ${args}";
        }
        set path [split [lindex $args end-1] ${sep}]
#@        puts "path=$path";

        # see if the path already exists
        set i 0;
        set subnode [huddle unwrap ${db}];
        set key "";
        while {$i < [llength $path] - 1} {
            lassign $subnode stag data;
#@            puts "node=$subnode";
            if {!$huddle::types(isContainer:$stag)} {
                error "Not a container node '$key'!";
            } elseif {[$huddle::types(callback:$stag) exists $data [lindex $path $i]] } {
                set subnode [$huddle::types(callback:$stag) get_subnode $data [lindex $path $i]]
                append key "${sep}[lindex $path $i]";
#@                puts "[join [lrange $path 0 $i] $sep]=$subnode";
                incr i 1;
            } else {
                break;
            }
        }

        if {$i+1 < [llength $path]} {
            # build the portion of the path that does not exist
            set new_dict {};
            foreach k [lreverse [lrange $path $i+1 end]] {
                if {${new_dict} eq {}} {
                    set new_dict [huddle create];
                } else {
                    set new_dict [huddle create ${k} ${new_dict}];
                }
            }
#@            puts "i=$i (of [llength $path]), subpath=[lrange $path $i end]"
            set root_node [huddle unwrap ${new_dict}];
#@            puts "root_node=${root_node}"
            set subnode [huddle::argument_to_node [lindex $args end] ${tag}];
#@            puts "subnode=${subnode} (tag=$tag)"
            set subpath [lrange $path $i+1 end]
            huddle::Apply_to_subnode set root_node [llength $subpath] $subpath $subnode
#@            puts "root_node=${root_node}"
            set subnode ${root_node};
        } else {
            set subnode [huddle::argument_to_node [lindex $args end] ${tag}]
        }
        set root_node [huddle unwrap $db]

        # We delete the internal reference of $obj to $root_node
        # Now refcount of $root_node is 1
        unset db
        set path [lrange $path 0 $i];
        huddle::Apply_to_subnode set root_node [llength $path] $path $subnode
#@            puts ">>root_node=${root_node}"
        set db [huddle wrap $root_node]
    }


    proc get_key {args} {
        if {[llength $args] < 1} { error "Wrong number of arguments"; }
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        array set _opts { raw 0 type 0 quiet 0}
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -quiet {
                    set _opts(quiet) 1;
                }
                -raw {
                    if {${_opts(type)}} {
                        error "Can't use both -type and -raw!";
                    }
                    set _opts(raw) 1;
                }
                -type {
                    if {${_opts(raw)}} {
                        error "Can't use both -type and -raw!";
                    }
                    set _opts(type) 1;
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args]-1} {
            #TODO: maybe can return the full DB?
            error "Too few arguments! Args: ${args}";
        } elseif {$i+1 > [llength $args]} {
            error "Too many arguments! Args: ${args}";
        }
        set path [split [lindex $args end] ${sep}]
#@        puts "path:[llength $path]=$path";
        if {${_opts(raw)}} {
            return [eval huddle get \$db [subst $path]];
        } elseif {${_opts(type)}} {
            set tag [lindex [huddle unwrap [eval huddle get \$db [subst $path]]] 0];
            return $huddle::types(type:$tag);
        } else {
            return [eval huddle get_stripped \$db [subst $path]];
        }
    }


    proc delete_key {args} {
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        array set _opts { quiet 0 }
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -quiet {
                    set _opts(quiet) 1;
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        set args [lrange $args $i end];
        foreach key $args {
            set path [split ${key} ${sep}]
            #TODO:<bug in huddle::Unset> eval huddle unset db [subst $path];
            set db [eval huddle remove \$db [subst $path]];
        }
    }


    proc exists_key {args} {
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        array set _opts {quiet 0 list 0 }
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -list {
                    set _opts(list) 1;
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i >= [llength $args]} {
            if {[info exists _opts(quiet)] && ${_opts(quiet)}} {
                return 0;
            } else {
                error "Too few arguments: '${args}'";
            }
        }

        set args [lrange $args $i end];
        set l {};
        foreach key $args {
            set path [split ${key} ${sep}]
            if {![eval huddle exists \$db [subst $path]]} {
                if {${_opts(list)}} {
                    lappend l ${key};
                } else {
                    return 0;
                }
            }
        }

        if {${_opts(list)}} {
            return ${l};
        } else {
            return 1;
        }
    }


    proc is_empty_key {args} {
        if {[llength $args] < 1} { error "Wrong number of arguments"; }
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        array set _opts { raw 0 type 0 quiet 0}
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -quiet {
                    set _opts(quiet) 1;
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args]-1} {
            #TODO: make consistent with `get_key`
            error "Too few arguments! Args: ${args}";
        } elseif {$i+1 > [llength $args]} {
            #TODO: can iterate over a list of keys
            error "Too many arguments! Args: ${args}";
        }


        set args [lrange $args $i end];
        foreach key $args {
            if {[exists_key ${key}]} {
                set path [split ${key} ${sep}];
                set h [eval huddle get \$db [subst $path]];
                lassign [huddle unwrap $h] tag data;
                if {![info exists huddle::types(isContainer:${tag})]} {
                    error "Wrong huddle tag '${tag}'!";
                } elseif {$huddle::types(isContainer:${tag}) ne "yes"} {
                    if {${data} ne {}} { return 0; }
                } else {
                    set items [$huddle::types(callback:$tag) items $data];
                    if {[llength ${items}] != 0} { return 0; }
                }
            } else {
                error "Key '${key}' does not exist!";
            }
        }
        return 1;
    }


    proc to_file {args} {
        array set _opts { db {} format huddle quiet 0}
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -quiet {
                    set _opts(quiet) 1;
                }
                -db {
                    if {$i >= [llength $args] -1} { error "Wrong number of arguments"; }
                    incr i 1;
                    set name [lindex $args $i];
                    if {[string match "-*" ${name}]} {
                        error "Missing type for the -db option!";
                    }
                    set _opts(db) ${name};
                }
                -huddle {
                    set _opts(format) "huddle";
                }
                -json {
                    set _opts(format) "json";
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        if {$i < [llength $args] - 1} {
            error "Too many arguments";
        }

        # get DB object
        if {${_opts(db)} eq {}} {
            namespace upvar [namespace current] db db;
        } else {
            upvar ${_opts(db)} db;
        }

        if {${_opts(format)} eq "huddle"} {
            [namespace current]::_to_huddle_file "db" ${_opts(quiet)} [lindex $args $i];
        } elseif {${_opts(format)} eq "json"} {
            [namespace current]::_to_json_file "db" ${_opts(quiet)} [lindex $args $i];
        } else {
            error "Unsupported file format '${_opts(format)}'!";
        }
    }


    proc _to_huddle_file {db_name quiet path} {
        upvar ${db_name} db;

        # load data from files
        set fl stdout;
        if {${path} ne {}} {
            if {[catch {set fl [open ${path} w]} err]} {
                if {${quiet} == 0} { error ${err}; }
                return;
            }
        }

        puts -nonewline ${fl} ${db};

        if {${path} ne {}} {
            if {[catch {close $fl} err]} {
                if {${quiet} == 0} { error ${err}; }
            }
        }
    }


    proc _to_json_file {db_name quiet path} {
        upvar ${db_name} db;

        # load data from files
        set fl stdout;
        if {${path} ne {}} {
            if {[catch {set fl [open ${path} w]} err]} {
                if {${quiet} == 0} { error ${err}; }
                return;
            }
        }

        puts -nonewline ${fl} [huddle::jsondump ${db} "" ""];

        if {${path} ne {}} {
            if {[catch {close $fl} err]} {
                if {${quiet} == 0} { error ${err}; }
            }
        }
    }


    # Loads data from DB files.
    #
    # TODO: Presently loads data from the 1st file only, others are ignored.
    #
    # Prototype::
    #
    #     from_files [-db <varName>] [-quiet] [-huddle|-json] <filePaths>
    #
    # When `-quiet`, no errors will be reported for non-existing files.
    #
    # When no file paths given, nothing happens (except reporting errors for
    # other options).
    proc from_files {args} {
        array set _opts { db {} format huddle quiet 0}
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -quiet {
                    set _opts(quiet) 1;
                }
                -db {
                    if {$i >= [llength $args] -1} { error "Wrong number of arguments"; }
                    incr i 1;
                    set name [lindex $args $i];
                    if {[string match "-*" ${name}]} {
                        error "Missing type for the -db option!";
                    }
                    set _opts(db) ${name};
                }
                -huddle {
                    set _opts(format) "huddle";
                }
                -json {
                    set _opts(format) "json";
                }
                -- { incr i 1; break; }
                -* {
                    error "Unknown option [lindex $args $i]";
                }
                default {
                    break;
                }
            }
            incr i 1;
        }; #while

        # get DB object
        if {${_opts(db)} eq {}} {
            namespace upvar [namespace current] db db;
        } else {
            upvar ${_opts(db)} db;
        }

        if {${_opts(format)} eq "huddle"} {
            [namespace current]::_from_huddle_files "db" ${_opts(quiet)} [lrange $args $i end];
        } else {
            error "Unsupported file format '${_opts(format)}'!";
        }
    }

    proc _from_huddle_files {db_name quiet paths} {
        upvar ${db_name} db;

        # load data from files
        foreach path ${paths} {
            if {![file exists ${path}] || [file isdirectory ${path}]} {
                if {${quiet} == 0} { error "Not a file: '${path}'"; }
                continue;
            }

            if {[catch {set fl [open ${path}]} err]} {
                if {${quiet} == 0} { error ${err}; }
                continue;
            }

            if {[catch {set newdb [read ${fl}]} err]} {
                catch {close $fl};
                if {${quiet} == 0} { error ${err}; }
                continue;
            }

            # TODO
            set db ${newdb};

            if {[catch {close $fl} err]} {
                if {${quiet} == 0} { error ${err}; }
                continue;
            }

            #TODO skip all but the 1st file
            break;
        }
    }

}

## ##puts "isHuddle=[huddle isHuddle $::hudb::db]"
## puts "empty=[hudb::is_empty]"
## puts "db=$::hudb::db"
## #@ parray ::huddle::types
## hudb::set_key -type number x/y 1.0
## hudb::set_key a xyz
## set l {x y}
## ##puts [eval huddle get_stripped \$::hudb::db [subst $l]]
## puts [::hudb::get_key x/y]
## puts [::hudb::get_key -raw x/y]
## puts [::hudb::get_key -type x/y]
## hudb::set_key -type string x/y 1.5
## puts "db=$::hudb::db"
## puts "exists=[::hudb::exists_key x/y a x]"
## puts "exists=[::hudb::exists_key -list x/y a x]"
## puts "exists=[::hudb::exists_key x/y b x]"
## puts "exists=[::hudb::exists_key -list x/y b x y]"
## puts "is_empty=[::hudb::is_empty_key x]"
## puts "is_empty=[::hudb::is_empty_key a]"
## hudb::set_key a ""
## puts "is_empty=[::hudb::is_empty_key a]"
## ::hudb::delete_key x/y a
## puts "db=$::hudb::db"
## puts "is_empty=[::hudb::is_empty_key x]"
## hudb::set_key -type boolean x tadaaa
## puts "db=$::hudb::db"

