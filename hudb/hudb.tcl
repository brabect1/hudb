namespace eval hudb {
    source [file join [file dirname [file normalize [info script]]] huddle huddle.tcl]

    set db {HUDDLE {D {}}};
    set separator {/}

    proc is_wellformed {} {
        namespace upvar [namespace current] db db;
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


    proc is_empty {} {
#@        puts "[namespace current]"
#@        puts "[namespace which -variable db]"
        namespace upvar [namespace current] db db;
        if {![is_wellformed]} {
            error "Malformed DB!";
        }
        lassign [huddle unwrap ${db}] head src;
        if {[llength ${src}] > 0} { return 0; } else { return 1; }
    }


    proc set_key {args} {
        if {[llength ${args}] == 0} { return; }
        namespace upvar [namespace current] db db;
        namespace upvar [namespace current] separator sep;
        if {![is_wellformed]} {
            error "Malformed DB!";
        }

        if {[llength $args] < 2} { error "Wrong number of arguments"; }
        set tag s;
        set i 0;
        switch -glob -- [lindex $args $i] {
            -type {
                if {[llength $args] < 4} { error "Wrong number of arguments"; }
                incr i 1;
                set type [lindex $args $i];
                if {![info exists huddle::types(tagOfType:${type})]} {
                    error "Unknown type '${type}'!";
                } else {
                    set tag $huddle::types(tagOfType:${type});
                }
                incr i 1;
                if {![info exists huddle::types(isContainer:${tag})]} {
                    error "Cannot support '${type}'!";
                } elseif {$huddle::types(isContainer:${tag}) ne "no"} {
                    error "Container types not supported. Use -json option."
                }
            }
            -- { incr i 1; }
            -* {
                error "Unknown option [lindex $args $i]";
            }
        }

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
        if {![is_wellformed]} {
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
        if {![is_wellformed]} {
            error "Malformed DB!";
        }

        array set _opts { quiet 0 }
        set i 0;
        switch -glob -- [lindex $args $i] {
            -quiet {
                set _opts(quiet) 1;
                incr i 1;
            }
            -- { incr i 1; }
            -* {
                error "Unknown option [lindex $args $i]";
            }
        }

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
        if {![is_wellformed]} {
            error "Malformed DB!";
        }

        array set _opts { list 0 }
        set i 0;
        switch -glob -- [lindex $args $i] {
            -list {
                set _opts(list) 1;
                incr i 1;
            }
            -- { incr i 1; }
            -* {
                error "Unknown option [lindex $args $i]";
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
        if {![is_wellformed]} {
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
}

##puts "isHuddle=[huddle isHuddle $::hudb::db]"
puts "empty=[hudb::is_empty]"
puts "db=$::hudb::db"
#@ parray ::huddle::types
hudb::set_key -type number x/y 1.0
hudb::set_key a xyz
set l {x y}
##puts [eval huddle get_stripped \$::hudb::db [subst $l]]
puts [::hudb::get_key x/y]
puts [::hudb::get_key -raw x/y]
puts [::hudb::get_key -type x/y]
hudb::set_key -type string x/y 1.5
puts "db=$::hudb::db"
puts "exists=[::hudb::exists_key x/y a x]"
puts "exists=[::hudb::exists_key -list x/y a x]"
puts "exists=[::hudb::exists_key x/y b x]"
puts "exists=[::hudb::exists_key -list x/y b x y]"
puts "is_empty=[::hudb::is_empty_key x]"
puts "is_empty=[::hudb::is_empty_key a]"
hudb::set_key a ""
puts "is_empty=[::hudb::is_empty_key a]"
::hudb::delete_key x/y a
puts "db=$::hudb::db"
puts "is_empty=[::hudb::is_empty_key x]"
hudb::set_key -type boolean x tadaaa
puts "db=$::hudb::db"

