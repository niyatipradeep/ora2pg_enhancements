#!/usr/bin/perl
use strict;
use warnings;
use File::Copy qw(copy);

# Usage:
#   perl transform_ora2pg_output_inplace_heuristic.pl yourfile.sql
#
# The script makes a backup yourfile.sql.bak before overwriting.

my $file = shift or die "Usage: $0 <ora2pg_output.sql>\n";

# create backup
my $bak = "$file.bak";
copy($file, $bak) or die "Failed to create backup $bak: $!";

open my $fh, '<', $file or die "Cannot open $file: $!";
my @lines = <$fh>;
close $fh;

my $in_copy = 0;

for (my $i = 0; $i < @lines; $i++) {
    my $line = $lines[$i];

    # Start of COPY block (capture headers if needed later)
    if ($line =~ /^COPY\s+/i) {
        $in_copy = 1;
        next;
    }

    # End of COPY block
    if ($line =~ /^\\\./) {
        $in_copy = 0;
        next;
    }

    # Only transform the actual data lines inside COPY blocks
    next unless $in_copy;
    next if $line =~ /^\s*$/;

    chomp $line;

    # split by TAB (ora2pg uses tabs for COPY data)
    my @cols = split(/\t/, $line, -1);

    for my $c (0..$#cols) {
        my $col = $cols[$c];

        # Detect complex-ish encodings that originate from Oracle-to-Postgres transformation
        # Examples we expect:
        #   ({"(a,b,c)"})   -- nested wrapper around a parenthesized, comma-separated list
        #   ("{1,2,3}")     -- wrapper around an array string
        #   ("a","b","c")   -- already partially transformed; handle gracefully
        #
        # We handle three canonical shapes by extracting an "inner" CSV-like string and deciding:
        #  - composite/object -> produce ( ... ) with quoting rules
        #  - array/string-list -> produce { ... } with no quoting of elements (per user's examples)

        # Normalize some common wrappers to reach the inner payload:
        my $original = $col;

        # remove outer whitespace
        $col =~ s/^\s+|\s+$//g;

        my $inner;
        my $kind = ''; # 'object' or 'array' or ''

        if ($col =~ /^\(\{\"?\((.*)\)\"?\}\)$/s) {
            # ({"(a,b,c)"})
            $inner = $1;
            # ambiguous — decide below by inspecting parts
        }
        elsif ($col =~ /^\(\"?\{(.*)\}\"?\)$/s) {
            # ("{1,2,3}") or ("{a,b,c}")
            $inner = $1;
            $kind = 'array';
        }
        elsif ($col =~ /^\((.*)\)$/s && $col =~ /\A\([^)]/,  ) {
            # ( ... ) plain parentheses (could already be composite/object)
            $inner = $1;
            # decide below
        }
        elsif ($col =~ /^\{(.*)\}$/s) {
            # already an array literal {a,b}
            $kind = 'array';
            $inner = $1;
        }
        elsif ($col =~ /^\"(.*)\"$/s && $col =~ /,/) {
            # line like: "a","b","c" (split across columns may give this)
            # We'll not try to stitch columns together; handle as CSV list in this single column.
            $inner = $1;
        } else {
            # Not a complex-looking field; skip
            next;
        }

        # If no inner payload found, skip
        next unless defined $inner;

        # split into parts — simple CSV split on commas (ora2pg does not quote commas within fields here)
        # preserve empty fields as '' (representing NULLs for composites)
        my @parts = split(/,/, $inner, -1);

        # Trim whitespace around parts
        for (@parts) { s/^\s+|\s+$//g; }

        # Decide kind if not already known:
        if (!$kind) {
            # heuristics:
            # - if any part is empty => composite/object (NULL slots in composite)
            # - if any part looks purely numeric (integer or float) => composite/object (addresses often contain postal numbers)
            # - else => array/string-list
            my $has_empty = grep { $_ eq '' } @parts;
            my $has_numeric = grep { $_ =~ /^-?\d+(?:\.\d+)?$/ } @parts;
            if ($has_empty || $has_numeric) {
                $kind = 'object';
            } else {
                $kind = 'array';
            }
        }

        if ($kind eq 'array') {
            # Build array literal {a,b,c}
            # Per your examples, elements are emitted without double-quotes.
            # Escape closing brace and backslash inside elements to be safe.
            for my $p (@parts) {
                $p =~ s/\\/\\\\/g;
                $p =~ s/\}/\\\}/g;
                # do not wrap in quotes (per examples)
            }
            $cols[$c] = '{' . join(',', @parts) . '}';
        }
        else { # object/composite
            # For composites: numeric parts unquoted, empty parts left empty, strings quoted.
            for my $p (@parts) {
                if ($p eq '') {
                    # keep empty (represents NULL/unset field in composite)
                    $p = '';
                }
                elsif ($p =~ /^-?\d+(?:\.\d+)?$/) {
                    # numeric -> keep as is
                    # but ensure integer/float format consistent
                    $p = $p;
                }
                else {
                    # string -> double-quote, but escape any embedded double quotes
                    $p =~ s/"/\\"/g;
                    $p = qq{"$p"};
                }
            }
            $cols[$c] = '(' . join(',', @parts) . ')';
        }
    }

    # Rebuild the line preserving tabs
    $lines[$i] = join("\t", @cols) . "\n";
}

# Write back to same file (in-place)
open my $out, '>', $file or die "Cannot write to $file: $!";
print $out @lines;
close $out;

print "Fixed.";
