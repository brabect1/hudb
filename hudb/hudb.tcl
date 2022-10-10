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

        #TODO Presently the implementation is not really a URI but a file path.
        #     Proper (file) URI processing will be added later.
        namespace eval uri {
            variable settings;

            set settings {
                publicMethods {uri}
                tag uri
                isContainer no
            };

            proc uri {src} {
                return [[namespace parent]::wrap [list uri $src]];
            }

            proc create {data} {
                return [uri ${data}];
            }

            proc equal {p1 p2} {
                set p1 [file normalize $p1];
                set p2 [file normalize $p2];
                return [expr {$p1 eq $p2}];
            }

            proc jsondump {huddle_obj {offset "  "} {newline "\n"} {begin ""}} {
                set data [[namespace parent] get_stripped ${huddle_obj}];
                set data [string map {
                        \\ \\\\
                        \" \\\"
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
                    lset node 1 [[namespace parent [namespace parent]]::filepath::rebase [lindex $args 0] $value [lindex $args 1]];
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


    # Returns a fully qualified name of internal DB object.
    #
    # This routine helps to identify the internal DB object if users need to
    # manipulate it directly or need to pass it to `huddb::_*` routines meant
    # for internal use.
    #
    # To get a fully qualified name of the DB parent namespace use:
    #
    #     namespace qualifiers [hudb::_get_db_ref]
    #
    # To get a simple name of the DB object variable, use:
    #
    #     namespace tail [hudb::_get_db_ref]
    #
    proc _get_db_ref {} {
        return [namespace which -variable db];
    }


    # Resets a DB object identified by its variable name, `db_name`.
    #
    # Resetting will make the referenced DB empty. The routine may hence also
    # be used to get the empty DB representation.
    #
    # DB obj is referenced through `upvar` (with the default level) and hence
    # has to exist in the caller's frame stack (either directly or through
    # another `upvar` reference) or be a global variable, or it has to refer
    # to a fully qualified namespace variable (such as given by `hudb::_get_db_conf`.
    #
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


    # Changes relative paths in a DB object referenced by `db_name` so that
    # the paths assumed to be relative to `src_dir` become relative to `tgt_dir`.
    #
    # Neither of the dir arguments is checked for existence or file type. It is
    # up to the caller to ensure they are meaningful.
    #
    # The routine traverses the entire DB tree structure and for every URI type
    # performs the check for being a relative file paths and then rebases it
    # from `src_dir` to `tgt_dir`. This travers does occur even if the source
    # and target directories resolve into the same path; this is intentional
    # for testing purposes. For performance reasons, users shall do an file
    # path equivalence check prior to calling `_rebase_uris`.
    #
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


    # Sets a new key.
    #
    # Prototype::
    #
    #   set_key [-type <type>] [-raw] <key> <value>
    #
    # Arguments (after -* options):
    #   args[end-1] - DB key
    #   args[end] - value to be set
    #
    # The latter of `-type` and `-raw` overrides the other. That is, `-raw` and
    # `-type` options are mutually exclusive.
    #
    # With `-raw` the value is supposed to be a Huddle node. With `-type <type>`,
    # the value will be converted to the `<type>` Huddle node.
    #
    # TODO Container types such as `dict` and `list` are not supported at the moment.
    #
    # It is assumed that all the key parts represent an index into a huddle container.
    # If that turns false for any but the last key part, an error is raised.
    #
    proc set_key {args} {
        if {[llength ${args}] == 0} { return; }
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        if {[llength $args] < 2} { error "Wrong number of arguments"; }
        array set _opts { type {string} json 0 raw 0}
        set i 0;
        while {$i < [llength $args]} {
            switch -glob -- [lindex $args $i] {
                -type {
                    if {[llength $args] < $i+4} { error "Wrong number of arguments"; }
                    incr i 1;
                    set type [lindex $args $i];
                    if {${type} eq "--"} {
                        error "Missing type for the -type option!";
                    }
                    set _opts(type) ${type};
                    set _opts(raw) 0; # override raw value
                }
                -raw {
                    if {[llength $args] < $i+3} { error "Wrong number of arguments"; }
                    set _opts(raw) 1;
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

        _set_key db [lindex $args end-1] [lindex $args end] ${_opts(type)} ${_opts(raw)};
    }


    proc _set_key {db_name key value type {raw 0}} {
        upvar ${db_name} db;
        if {![_is_wellformed "db"]} {
            error "Malformed DB!";
        }

        namespace upvar [namespace current] separator sep;
        set path [split ${key} ${sep}]

        # remove leading empty key part (and do nothing if
        # no non-empty key part remains)
        set i 0;
        foreach p ${path} {
            if {$p ne {}} {
                break;
            }
            incr i 1;
        }
        if {$i == [llength $path]} { return; }

        # get the normalized key parts
        set path [lrange $path $i end];

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
            if {${raw} == 0} {
                if {[catch { set h [huddle::compile ${type} ${value}] } result options]} {
                    set subnode null;
                    return -options ${options} ${result};
                }
                set subnode [huddle::unwrap $h];
            } else {
                set subnode ${value}
            }
#@            puts "subnode=${subnode} (tag=$tag)"
            set subpath [lrange $path $i+1 end]
            huddle::Apply_to_subnode set root_node [llength $subpath] $subpath $subnode
#@            puts "root_node=${root_node}"
            set subnode ${root_node};
        } else {
            if {${raw} == 0} {
                if {[catch { set h [huddle::compile ${type} ${value}] } result options]} {
                    set subnode null;
                    return -options ${options} ${result};
                }
                set subnode [huddle::unwrap $h];
            } else {
                set subnode ${value};
            }
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

        # get output file path
        set path [lindex $args $i]; # yields an empty path if $i beyond $args range

        # determine the tagret dir for relative path rebasing
        # (empty value means use the $path parent folder)
        set tgt_dir {};
        if {${path} eq {}} {
            # empty path means "stdout" and hence avoid any rebasing
            set tgt_dir {.};
        }

        # write out (delegate a proper routine)
        if {${_opts(format)} eq "huddle"} {
            [namespace current]::_to_huddle_file "db" ${_opts(quiet)} $path {.} ${tgt_dir};
        } elseif {${_opts(format)} eq "json"} {
            [namespace current]::_to_json_file   "db" ${_opts(quiet)} $path {.} ${tgt_dir};
        } else {
            error "Unsupported file format '${_opts(format)}'!";
        }
    }


    # Note: To avoid rebasing relative path URIs in the output DB file use
    # the same values for `src_base_dir` and `tgt_base_dir`.
    #
    proc _to_huddle_file {db_name quiet path {src_base_dir .} {tgt_base_dir {}}} {
        upvar ${db_name} db;

        # set the output channel
        set fl stdout;
        if {${path} ne {}} {
            if {[catch {set fl [open ${path} w]} err]} {
                if {${quiet} == 0} { error ${err}; }
                return;
            }
        }

        # normalize the base dir paths
        set src_base_dir [file normalize ${src_base_dir}];
        if {${tgt_base_dir} ne {}} {
            set tgt_base_dir [file normalize ${tgt_base_dir}];
        } else {
            set tgt_base_dir [file normalize [file dirname $path]];
        }

        # write out DB (incl. rebasing relative file path URIs if needed)
        if {${src_base_dir} eq ${tgt_base_dir}} {
            # no rebasing needed
            puts -nonewline ${fl} ${db};
        } else {
            # rebase on a DB copy (so as not alter the original DB)
            set newdb ${db};
            _rebase_uris "newdb" ${tgt_base_dir} ${src_base_dir};
            puts -nonewline ${fl} ${newdb};
        }

        if {${path} ne {}} {
            if {[catch {close $fl} err]} {
                if {${quiet} == 0} { error ${err}; }
            }
        }
    }


    proc _to_json_file {db_name quiet path {src_base_dir .} {tgt_base_dir {}}} {
        upvar ${db_name} db;

        # set the output channel
        set fl stdout;
        if {${path} ne {}} {
            if {[catch {set fl [open ${path} w]} err]} {
                if {${quiet} == 0} { error ${err}; }
                return;
            }
        }

        # normalize the base dir paths
        set src_base_dir [file normalize ${src_base_dir}];
        if {${tgt_base_dir} ne {}} {
            set tgt_base_dir [file normalize ${tgt_base_dir}];
        } else {
            set tgt_base_dir [file normalize [file dirname $path]];
        }

        # write out DB (incl. rebasing relative file path URIs if needed)
        if {${src_base_dir} eq ${tgt_base_dir}} {
            # no rebasing needed
            puts -nonewline ${fl} [huddle::jsondump ${db} "" ""];
        } else {
            # rebase on a DB copy (so as not alter the original DB)
            set newdb ${db};
            _rebase_uris "newdb" ${tgt_base_dir} ${src_base_dir};
            puts -nonewline ${fl} [huddle::jsondump ${newdb} "" ""];
        }

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

    proc _from_huddle_files {db_name quiet paths {tgt_base_dir .} {src_base_dir {}}} {
        if {[llength $paths] == 0} { return; }

        upvar ${db_name} db;
        namespace upvar [namespace current] separator sep;

        # normalize the target dir base for rebasing relative file paths
        # in DBs read from $paths
        set tgt_dir [file normalize ${tgt_base_dir}];

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

            if {[catch {close $fl} err]} {
                if {${quiet} == 0} { error ${err}; }
            }

            # rebase relative file path URIs to the current working directory
            if {$src_base_dir ne {}} {
                set src_dir [file normalize ${src_base_dir}];
            } else {
                set src_dir [file normalize [file dirname $path]];
            }
            if {${src_dir} ne ${tgt_dir}} {
                _rebase_uris newdb ${tgt_dir} ${src_dir};
            }

            # see if the loaded DB is a dictionary
            # (if not, it will be disregarded as it 
            # TODO: If DB were allowed to have arbitrary structure, then a non-dictionary
            #       the loaded DB would just overwrite the whole DB. Maybe the list type
            #       would need to be treated specifically.
            set newdb_rn [huddle::unwrap ${newdb}];
            lassign ${newdb_rn} newdb_rn_tag newdb_rn_val;
            if {![info exists huddle::types(type:${newdb_rn_tag})]} {
                # unknown type -> ignore
                continue;
            } elseif {$huddle::types(type:${newdb_rn_tag}) ne "dict"} {
                # not a dictionary -> ignore
                continue;
            }

            # merge individual items at 1st level; items that are not dictionaries
            # are ignored
            # (that is, sub-items at the 2nd level will get added to DB
            # irrsepective if that sub-item already existed there)
            foreach item [$huddle::types(callback:${newdb_rn_tag}) items ${newdb_rn_val}] {
                lassign $item key subnode;

                # skip all but dictionary type items
                lassign ${subnode} sn_tag sn_val;
                if {![info exists huddle::types(type:${sn_tag})]} {
                    # unknown type -> ignore
                    continue;
                } elseif {$huddle::types(type:${sn_tag}) ne "dict"} {
                    # not a dictionary -> ignore
                    continue;
                }

                # put subitems into the DB
                foreach subitem [$huddle::types(callback:${sn_tag}) items ${sn_val}] {
                    lassign $subitem subkey subsubnode;
                    _set_key db "${key}${sep}${subkey}" ${subsubnode} {} 1;
                }
            }

##            # TODO
##            set db ${newdb};

##            #TODO skip all but the 1st file
##            break;
        }
    }

}

