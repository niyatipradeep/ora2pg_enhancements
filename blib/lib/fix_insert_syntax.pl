#!/usr/bin/perl

use strict;
use warnings;
use File::Copy qw(copy);

# Usage:
#   perl transform_ora2pg_insert_inplace_heuristic.pl yourfile.sql
#
# The script makes a backup yourfile.sql.bak before overwriting.

my $file = shift or die "Usage: $0 <ora2pg_insert_output.sql>\n";

# create backup
my $bak = "$file.bak";
copy($file, $bak) or die "Failed to create backup $bak: $!";

open my $fh, '<', $file or die "Cannot open $file: $!";
my @lines = <$fh>;
close $fh;

# Transform a single "complex" field string like:
#   ({"(a,b,c)"})
#   ("{1,2,3}")
#   ("{10,20,30,40,50}")
# into:
#   {a,b,c}                     (array)
#   ("street","city",123,"ctry") (composite)
#
# Returns undef if it doesn't recognize the pattern.
sub transform_complex_field {
    my ($col) = @_;

    my $original = $col;

    # Trim outer whitespace
    $col =~ s/^\s+|\s+$//g;

    my $inner;
    my $kind = ''; # 'object' or 'array' or ''

    if ($col =~ /^\(\{\"?\((.*)\)\"?\}\)$/s) {
        # ({"(a,b,c)"})
        $inner = $1;
        # kind decided later
    }
    elsif ($col =~ /^\(\"?\{(.*)\}\"?\)$/s) {
        # ("{1,2,3}") or ("{a,b,c}")
        $inner = $1;
        $kind  = 'array';
    }
    elsif ($col =~ /^\((.*)\)$/s) {
        # Plain parentheses (a,b,c)
        $inner = $1;
        # kind decided later
    }
    elsif ($col =~ /^\{(.*)\}$/s) {
        # Already an array literal {a,b}
        $inner = $1;
        $kind  = 'array';
    }
    elsif ($col =~ /^\"(.*)\"$/s && $col =~ /,/) {
        # "a,b,c" – treat as CSV inside a single string
        $inner = $1;
    } else {
        # Not a complex-looking field; leave unchanged
        return undef;
    }

    return undef unless defined $inner;

    # Split into CSV parts (ora2pg does not put commas inside values here)
    my @parts = split(/,/, $inner, -1);

    # Trim whitespace for each part
    for (@parts) {
        s/^\s+|\s+$//g;
    }

    # Decide type if still unknown
    if (!$kind) {
        my $has_empty   = grep { $_ eq '' } @parts;
        my $has_numeric = grep { $_ =~ /^-?\d+(?:\.\d+)?$/ } @parts;

        if ($has_empty || $has_numeric) {
            $kind = 'object';  # composite/object
        } else {
            $kind = 'array';   # string list
        }
    }

    if ($kind eq 'array') {
        # Build array literal {a,b,c}
        # Per your COPY script, array elements are not additionally
        # double-quoted, even for strings.
        for my $p (@parts) {
            $p =~ s/\\/\\\\/g;  # escape backslash
            $p =~ s/\}/\\\}/g;  # escape closing brace
        }
        my $new = '{' . join(',', @parts) . '}';
        return $new;
    }
    else {
        # Composite/object: numeric parts unquoted, empty => NULL slot,
        # strings in double quotes.
        for my $p (@parts) {
            if ($p eq '') {
                # empty: leave as empty between commas in composite
                $p = '';
            }
            elsif ($p =~ /^-?\d+(?:\.\d+)?$/) {
                # numeric: keep as-is
                $p = $p;
            }
            else {
                # string: wrap in double quotes, escape inner double quotes
                $p =~ s/"/\\"/g;
                $p = qq{"$p"};
            }
        }
        my $new = '(' . join(',', @parts) . ')';
        return $new;
    }
}

# Process each line:
# We do NOT rely on valid SQL '...' parsing (because of cases like O'Reilly).
# Instead we:
#   - find single-quoted chunks whose CONTENT clearly looks like our
#     complex wrappers: ({"( ... )"}), ("{ ... }"), { ... }
#   - transform just those chunks.
for my $line (@lines) {

    $line =~ s{
        (E?)'                            # $1: optional E prefix
        (                                # $2: body that looks like a complex wrapper
           \(\{\"?\( .*? \)\"?\}\)       # ({"( ... )"})
          |\"?\{ .*? \}\"?               # "{ ... }" or { ... } inside the string
          |\(\"?\{ .*? \}\"?\)           # ("{ ... }")
        )
        '                                # closing single quote
    }{
        my $prefix  = $1;
        my $body    = $2;

        # Normalize any doubled single quotes inside this body back to literal '
        # (just in case ora2pg had escaped them already in some cases)
        my $decoded = $body;
        $decoded =~ s/''/'/g;

        my $transformed = transform_complex_field($decoded);

        my $final = defined($transformed) ? $transformed : $decoded;

        # Re-escape single quotes for SQL
        $final =~ s/'/''/g;

        $prefix . "'" . $final . "'";
    }gsex;

}

# Write back to same file (in-place)
open my $out, '>', $file or die "Cannot write to $file: $!";
print $out @lines;
close $out;

print "Fixed.\n";
