hudb (Huddle DB)
================

``hudb`` is meant to be a DB- or dictionary-like structure built on Tcllib's
``huddle`` data structure. All it adds to ``huddle``, indeed, is more conevnient
API for adding and setting values stored in a huddle object/structure.

``hudb`` aims to be used at places where you would normally use Tcl's ``dict``
but need to know types of the disctionary values to serialize in and out from
the internal structure (which is not possible with native ``dict`` without
a separate schema). ``huddle`` naturally fits the need, except that its API is
not too convenient (and very poorly documented, too).

From performance standpoint, ``huddle`` is a fairly expensive data structure.
Every ``get``/``exists`` operation requires to unwind the corresponding
sub-tree; every set operation also adds re-packing the new sub-tree into
the complete data structure.
